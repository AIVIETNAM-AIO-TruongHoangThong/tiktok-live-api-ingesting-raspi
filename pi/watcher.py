"""
TikTok Live 24/7 Multi-Stream Autonomous Watcher & Telemetry Daemon.

Production Data Engineering Daemon for Raspberry Pi:
1. Pure Stream Processing - Raw data (gifts, comments, likes, pins) is streamed directly
   to Kafka; NEVER printed to stdout or logged as text rows.
2. Production Observability - Uses structured logging with rotating file handlers and a
   periodic 5-minute telemetry heartbeat rollup (uptime, event rates, active slots).
3. Value-Qualification Gate - Probes live streams for 90s; drops dead/idle streams early.
4. PK Battle Spider - Automatically discovers peer creators from live battles.
"""

import asyncio
import logging
from logging.handlers import RotatingFileHandler
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    CommentEvent,
    ConnectEvent,
    DisconnectEvent,
    GiftEvent,
    LikeEvent,
    LinkEvent,
    LinkMicBattleEvent,
    LiveEndEvent,
    OecLiveShoppingEvent,
    RoomUserSeqEvent,
)

try:
    from producer import close_producer, get_producer, publish
    from registry import CandidateRegistry
except ImportError:
    from pi.producer import close_producer, get_producer, publish
    from pi.registry import CandidateRegistry

log = logging.getLogger("watcher")


# ---------------------------------------------------------------------------
# Production Logging Setup (Rotating File + Systemd Journald / Stdout)
# ---------------------------------------------------------------------------

def setup_logging(cfg: dict) -> None:
    """Configures structured logging with log rotation to protect Pi SD cards."""
    log_cfg = cfg.get("logging", {})
    level_name = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1. Console handler (captured by systemd journalctl with automatic OS rotation)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    # 2. Rotating file handler (max 10MB, keep 2 backups = max 30MB disk usage)
    log_file_path = log_cfg.get("log_file")
    if log_file_path:
        try:
            path = Path(log_file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            rf_handler = RotatingFileHandler(
                path,
                maxBytes=10 * 1024 * 1024,
                backupCount=2,
                encoding="utf-8",
            )
            rf_handler.setFormatter(formatter)
            root_logger.addHandler(rf_handler)
        except (PermissionError, OSError) as e:
            # Non-root service on Pi - fallback cleanly to systemd stdout
            log.debug(f"File logging to {log_file_path} skipped ({e}); using stdout.")


def load_config(path: str = "config.yaml") -> dict:
    """Load configuration options from YAML."""
    config_path = Path(__file__).resolve().parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Telemetry Metrics Tracker (Aggregated Counters, Zero Log Flooding)
# ---------------------------------------------------------------------------

class PipelineMetrics:
    """Tracks global event throughput for periodic heartbeat rollups."""

    def __init__(self):
        self.start_time = datetime.now(timezone.utc)
        self.total_events = 0
        self.counts: dict[str, int] = {
            "gift": 0,
            "comment": 0,
            "like": 0,
            "room_users": 0,
            "product_pin": 0,
        }

    def record_event(self, event_type: str) -> None:
        self.total_events += 1
        self.counts[event_type] = self.counts.get(event_type, 0) + 1

    def format_heartbeat(self, active_streamers: list[str]) -> str:
        elapsed = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        rate = (self.total_events / elapsed) if elapsed > 0 else 0.0
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, _ = divmod(remainder, 60)
        uptime = f"{hours}h {minutes:02d}m"

        active_list = ", ".join(f"@{s}" for s in active_streamers) if active_streamers else "none"
        breakdown = ", ".join(f"{k}: {v:,}" for k, v in self.counts.items() if v > 0) or "no events"

        return (
            f"[HEARTBEAT] Uptime: {uptime} | Active: {len(active_streamers)} [{active_list}] | "
            f"Total Sent: {self.total_events:,} ({rate:.1f} eps) | Breakdown: ({breakdown})"
        )


async def heartbeat_worker(
    metrics: PipelineMetrics,
    active_tasks: dict[str, asyncio.Task],
    interval_sec: int = 300,
) -> None:
    """Emits one clean, aggregated telemetry heartbeat every 5 minutes."""
    while True:
        await asyncio.sleep(interval_sec)
        active_names = [u for u, task in active_tasks.items() if not task.done()]
        log.info(metrics.format_heartbeat(active_names))


# ---------------------------------------------------------------------------
# Helper Extractors
# ---------------------------------------------------------------------------

def extract_user(user: Any) -> dict:
    """Safely extracts identifiers from TikTokLive User protobuf."""
    if not user:
        return {"user_id": None, "unique_id": "anonymous", "nickname": "anonymous"}
    return {
        "user_id": str(getattr(user, "id", None)),
        "unique_id": (
            getattr(user, "display_id", None)
            or getattr(user, "unique_id", None)
            or getattr(user, "nickname", "anonymous")
        ),
        "nickname": getattr(user, "nickname", "anonymous"),
    }


def extract_battle_opponents(event: Any, current_streamer: str) -> list[str]:
    """Extracts peer creator handles from battle coordination events."""
    opponents: list[str] = []
    anchors = (
        getattr(event, "anchors_info", None)
        or getattr(event, "anchor_info", None)
        or []
    )
    for a in anchors:
        user = getattr(a, "user", None) or getattr(a, "anchor", None) or a
        handle = getattr(user, "display_id", None) or getattr(user, "unique_id", None)
        if handle and handle.lower() != current_streamer.lower():
            opponents.append(handle.lower())

    rival = getattr(event, "rival_extra", None)
    if rival:
        handle = getattr(rival, "display_id", None) or getattr(rival, "unique_id", None)
        if handle and handle.lower() != current_streamer.lower():
            opponents.append(handle.lower())

    return list(set(opponents))


# ---------------------------------------------------------------------------
# Single-stream Ingestion Task (Silent Data Streaming)
# ---------------------------------------------------------------------------

async def ingest_stream(
    username: str,
    category: str,
    archetype: str,
    kafka_config: dict,
    qualification_config: dict,
    registry: CandidateRegistry,
    metrics: PipelineMetrics,
) -> None:
    """
    Connects to one live stream, silently routes raw events to Kafka,
    tracks local probation metrics, and drops early if low value.
    """
    topic = f"tiktok.events.{category}"
    producer = await get_producer(kafka_config["bootstrap_servers"])
    client = TikTokLiveClient(unique_id=username)

    # Suppress schema drift warnings
    if hasattr(client, "parse_error_ignorelist"):
        try:
            client.parse_error_ignorelist.append("WebcastLinkLayerMessage")
        except AttributeError:
            client.parse_error_ignorelist.add("WebcastLinkLayerMessage")

    if hasattr(client, "logger"):
        import logging as _l
        client.logger.setLevel(_l.CRITICAL)

    probation_metrics = {
        "viewers": 0,
        "diamonds": 0,
        "gifts": 0,
        "comments": 0,
        "likes": 0,
        "has_shop_products": False,
        "is_qualified": False,
    }

    def make_event(event_type: str, data: dict) -> dict:
        return {
            "streamer": username,
            "category": category,
            "archetype": archetype,
            "event_type": event_type,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }

    # --- Event Handlers (Pure Data Publishing, NO Print/Log per event) ---

    @client.on(ConnectEvent)
    async def on_connect(_: ConnectEvent) -> None:
        log.info(f"[CONNECTED] @{username} ({category}/{archetype}) live room open.")

    @client.on(RoomUserSeqEvent)
    async def on_room_users(event: RoomUserSeqEvent) -> None:
        viewer_count = getattr(event, "total", 0)
        probation_metrics["viewers"] = max(probation_metrics["viewers"], viewer_count)
        await publish(
            producer,
            topic,
            make_event("room_users", {
                "viewer_count": viewer_count,
                "popularity": getattr(event, "popularity", 0),
            }),
        )
        metrics.record_event("room_users")

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent) -> None:
        gift = getattr(event, "gift", None)
        diamonds = getattr(gift, "diamond_count", 0) or 0
        streak = getattr(event, "repeat_count", 1) or 1
        probation_metrics["gifts"] += streak
        probation_metrics["diamonds"] += diamonds * streak

        await publish(
            producer,
            topic,
            make_event("gift", {
                **extract_user(event.user),
                "gift_id": getattr(event, "gift_id", None),
                "gift_name": getattr(gift, "name", "Unknown"),
                "diamond_count": diamonds,
                "repeat_count": streak,
                "repeat_end": bool(getattr(event, "repeat_end", False)),
                "streaking": bool(getattr(event, "streaking", False)),
                "usd_value": getattr(event, "value", None),
            }),
        )
        metrics.record_event("gift")

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent) -> None:
        probation_metrics["comments"] += 1
        await publish(
            producer,
            topic,
            make_event("comment", {
                **extract_user(event.user),
                "comment": getattr(event, "comment", None) or getattr(event, "content", ""),
            }),
        )
        metrics.record_event("comment")

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent) -> None:
        probation_metrics["likes"] += getattr(event, "count", 1)
        await publish(
            producer,
            topic,
            make_event("like", {
                **extract_user(event.user),
                "batch_count": getattr(event, "count", 1),
                "total_room_likes": getattr(event, "total", 0),
            }),
        )
        metrics.record_event("like")

    @client.on(OecLiveShoppingEvent)
    async def on_shop(event: OecLiveShoppingEvent) -> None:
        pop = getattr(event, "pop_product", None)
        if pop:
            probation_metrics["has_shop_products"] = True
            await publish(
                producer,
                topic,
                make_event("product_pin", {
                    "product_id": getattr(pop, "product_id", None),
                    "title": getattr(pop, "title", None),
                    "price": getattr(pop, "price", None),
                    "open_url": getattr(pop, "open_url", None),
                    "live_product_count": getattr(event, "live_product_number", 0),
                }),
            )
            metrics.record_event("product_pin")

    # Spidering: LinkMic & Battle events discover opponent creators
    async def handle_spider_discovery(event: Any) -> None:
        opponents = extract_battle_opponents(event, username)
        for opp in opponents:
            opp_archetype = "pk_battler" if category == "npc" else archetype
            if registry.add_candidate(
                opp,
                category=category,
                archetype=opp_archetype,
                source=f"pk_battle_@{username}",
            ):
                log.info(f"[SPIDER] Discovered creator @{opp} ({category}/{opp_archetype}) via battle with @{username}.")

    @client.on(LinkMicBattleEvent)
    async def on_battle(event: LinkMicBattleEvent) -> None:
        await handle_spider_discovery(event)

    @client.on(LinkEvent)
    async def on_link(event: LinkEvent) -> None:
        await handle_spider_discovery(event)

    @client.on(LiveEndEvent)
    async def on_end(_: LiveEndEvent) -> None:
        log.info(f"[ENDED] @{username} stream completed by host.")
        await client.disconnect()

    @client.on(DisconnectEvent)
    async def on_disconnect(_: DisconnectEvent) -> None:
        log.info(f"[DISCONNECTED] @{username} connection closed.")

    # ------------------------------------------------------------------
    # Value-Qualification Gate (Probation Monitor)
    # ------------------------------------------------------------------
    async def probation_monitor() -> None:
        if not qualification_config.get("enabled", True):
            return

        probation_sec = qualification_config.get("probation_seconds", 90)
        await asyncio.sleep(probation_sec)

        if not client.connected:
            return

        min_viewers = qualification_config.get("min_viewers", 5)
        cooldown_hours = qualification_config.get("cooldown_hours_on_fail", 3)

        passed = False
        if category == "npc":
            has_activity = (
                (probation_metrics["gifts"] > 0)
                or (probation_metrics["diamonds"] > 0)
                or (probation_metrics["comments"] >= 3)
            )
            passed = (probation_metrics["viewers"] >= min_viewers) and has_activity
        else:
            passed = probation_metrics["has_shop_products"] or (
                probation_metrics["viewers"] >= min_viewers and probation_metrics["comments"] >= 1
            )

        probation_metrics["is_qualified"] = passed
        registry.record_probation_result(
            username=username,
            passed=passed,
            metrics=probation_metrics,
            cooldown_hours_on_fail=cooldown_hours,
        )

        if not passed:
            log.warning(
                f"[PROBATION DROPPED] @{username} low activity ({probation_metrics['viewers']} peak viewers, "
                f"{probation_metrics['gifts']} gifts). Slot released."
            )
            await client.disconnect()
        else:
            log.info(
                f"[QUALIFIED] @{username} passed 90s probation "
                f"(Peak viewers: {probation_metrics['viewers']}, Gifts: {probation_metrics['gifts']}, Diamonds: {probation_metrics['diamonds']}). Ingesting 24/7."
            )

    probation_task = asyncio.create_task(probation_monitor())

    try:
        await client.start()
    except Exception as e:
        log.warning(f"[{username}] Connection error: {e}")
    finally:
        probation_task.cancel()
        registry.record_stream_end(username)


# ---------------------------------------------------------------------------
# Main Watcher Daemon Loop
# ---------------------------------------------------------------------------

async def watcher_loop() -> None:
    config = load_config()
    setup_logging(config)

    kafka_config = config["kafka"]
    qual_config = config.get("qualification", {})
    max_concurrent = config.get("max_concurrent_streams", 10)
    poll_interval = config.get("poll_interval_seconds", 45)
    jitter = config.get("poll_jitter_seconds", 3)
    heartbeat_interval = config.get("logging", {}).get("heartbeat_interval_seconds", 300)

    registry = CandidateRegistry()
    pipeline_metrics = PipelineMetrics()

    # Seed candidates from config if newly provided
    for category, cat_cfg in config.get("categories", {}).items():
        for username in cat_cfg.get("streamers", []):
            registry.add_candidate(username, category, source="config_seed")

    log.info("=" * 65)
    log.info("[START] TikTok Live Autonomous 24/7 Daemon Initialized")
    log.info(f"Target Broker: {kafka_config['bootstrap_servers']}")
    log.info(f"Max Concurrent Slots: {max_concurrent}")
    log.info(f"Candidates Registered: {len(registry.candidates)}")
    log.info(f"Heartbeat Interval: {heartbeat_interval}s")
    log.info("=" * 65)

    active: dict[str, asyncio.Task] = {}

    # Start the 5-minute telemetry heartbeat
    heartbeat_task = asyncio.create_task(
        heartbeat_worker(pipeline_metrics, active, interval_sec=heartbeat_interval)
    )

    try:
        while True:
            # 1. Clean up completed tasks
            finished = [u for u, task in active.items() if task.done()]
            for u in finished:
                exc = active[u].exception()
                if exc:
                    log.warning(f"[{u}] Ingestion task completed with exception: {exc}")
                del active[u]

            # 2. Check available slot capacity
            available_slots = max_concurrent - len(active)
            if available_slots <= 0:
                log.debug(f"All {max_concurrent} slots active. Waiting {poll_interval}s.")
                await asyncio.sleep(poll_interval)
                continue

            # 3. Pull actionable candidates (qualified first, then untested)
            targets = registry.get_actionable_streamers(
                active_streamers=set(active.keys()),
                limit=available_slots,
            )

            for username, category, archetype in targets:
                if len(active) >= max_concurrent:
                    break

                await asyncio.sleep(random.uniform(0.5, jitter))
                try:
                    probe = TikTokLiveClient(unique_id=username)
                    is_live = await probe.is_live()

                    if is_live:
                        log.info(f"[LIVE DETECTED] @{username} ({category}/{archetype}) is live! Spawning stream task.")
                        task = asyncio.create_task(
                            ingest_stream(
                                username=username,
                                category=category,
                                archetype=archetype,
                                kafka_config=kafka_config,
                                qualification_config=qual_config,
                                registry=registry,
                                metrics=pipeline_metrics,
                            ),
                            name=f"ingest-{username}",
                        )
                        active[username] = task
                    else:
                        registry.record_checked_offline(username, next_check_delay_sec=180)

                except Exception as e:
                    err_msg = str(e).lower()
                    if (
                        "not capable of going live" in err_msg
                        or "does not exist" in err_msg
                        or "user_not_found" in err_msg
                    ):
                        log.info(f"[BLACKLIST] @{username} cannot go live or not found. Blacklisting.")
                        registry.mark_invalid(username, reason="incapable_of_live")
                    else:
                        log.debug(f"[{username}] is_live() check returned: {e}")
                        registry.record_checked_offline(username, next_check_delay_sec=300)

            # High-frequency status is logged at DEBUG to keep logs clean
            log.debug(f"[POLL CYCLE] Active: {len(active)} / Candidates: {len(registry.candidates)}")
            await asyncio.sleep(poll_interval)

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("[SHUTDOWN] Cancelling stream tasks and heartbeat...")
        heartbeat_task.cancel()
        for task in active.values():
            task.cancel()
        await asyncio.gather(*active.values(), return_exceptions=True)
        await close_producer()
        log.info("[SHUTDOWN] Ingestion cleanly terminated.")


if __name__ == "__main__":
    asyncio.run(watcher_loop())
