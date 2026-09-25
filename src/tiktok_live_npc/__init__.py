"""TikTok Live NPC Event Stream Ingestion & Discovery Probe.

Captures real-time gift combos, comments, viewer counts, and like spikes
for TikTok Live streams (optimized for high-frequency NPC streams).
"""

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    CommentEvent,
    ConnectEvent,
    DisconnectEvent,
    GiftEvent,
    LikeEvent,
    LiveEndEvent,
    RoomUserSeqEvent,
)


def extract_user_info(user: Any) -> dict:
    """Safely extracts identifiers from either User or ExtendedUser protobuf objects."""
    if not user:
        return {"user_id": None, "unique_id": "anonymous", "nickname": "anonymous"}

    # In TikTok protobuf, display_id is the unique @handle; nickname is display name
    unique_id = (
        getattr(user, "display_id", None)
        or getattr(user, "unique_id", None)
        or getattr(user, "nickname", None)
        or str(getattr(user, "id", "anonymous"))
    )
    nickname = getattr(user, "nickname", None) or unique_id
    user_id = str(getattr(user, "id", getattr(user, "user_id", None)))

    return {
        "user_id": user_id,
        "unique_id": unique_id,
        "nickname": nickname,
    }


def extract_gift_info(event: GiftEvent) -> dict:
    """Safely extracts gift metadata from GiftEvent."""
    gift = getattr(event, "gift", None)
    name = getattr(gift, "name", None) or f"Gift_{getattr(event, 'gift_id', 'unknown')}"
    diamond_count = getattr(gift, "diamond_count", 0) or 0
    gift_id = getattr(event, "gift_id", None) or getattr(gift, "id", None)
    return {
        "gift_id": gift_id,
        "name": name,
        "diamond_count": diamond_count,
    }


class NPCStreamStats:
    """In-memory aggregation metrics for the live session."""

    def __init__(self, streamer: str):
        self.streamer = streamer
        self.start_time = datetime.now(timezone.utc)
        self.total_diamonds = 0
        self.total_usd = 0.0
        self.gift_counts = defaultdict(int)
        self.user_contributions = defaultdict(int)  # user -> total diamonds
        self.last_viewer_count = 0
        self.total_likes = 0
        self.total_comments = 0

    def record_gift(
        self, user: str, gift_name: str, diamonds: int, count: int, usd: Optional[float]
    ):
        total_coins = diamonds * count
        self.total_diamonds += total_coins
        if usd is not None:
            self.total_usd += usd
        else:
            self.total_usd += total_coins * 0.005

        self.gift_counts[gift_name] += count
        self.user_contributions[user] += total_coins

    def print_summary(self):
        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        rate_per_min = (self.total_diamonds / (duration / 60.0)) if duration > 0 else 0
        usd_rate_per_min = (self.total_usd / (duration / 60.0)) if duration > 0 else 0

        print("\n" + "=" * 60)
        print(f"[SESSION SUMMARY] @{self.streamer} ({duration:.0f}s elapsed)")
        print(
            f"Total Revenue: {self.total_diamonds:,} diamonds (~${self.total_usd:,.2f} USD)"
        )
        print(
            f"Burn Rate: {rate_per_min:,.1f} diamonds/min (~${usd_rate_per_min:,.2f}/min)"
        )
        print(
            f"Last Viewers: {self.last_viewer_count:,} | Likes: {self.total_likes:,} | Comments: {self.total_comments:,}"
        )

        if self.gift_counts:
            print("\nTop Gifts Triggered:")
            sorted_gifts = sorted(
                self.gift_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]
            for name, count in sorted_gifts:
                print(f"   - {name}: {count:,} times")

        if self.user_contributions:
            print("\nTop 3 Gifters (Whales):")
            top_whales = sorted(
                self.user_contributions.items(), key=lambda x: x[1], reverse=True
            )[:3]
            for user, coins in top_whales:
                print(f"   - @{user}: {coins:,} diamonds (~${coins * 0.005:,.2f})")
        print("=" * 60 + "\n")


def build_client(streamer: str, output_file: Optional[Path]) -> TikTokLiveClient:
    stats = NPCStreamStats(streamer)
    client = TikTokLiveClient(unique_id=streamer)

    # Ignore noisy internal TikTok packets (e.g. co-host/linkmic layer) that fail protobuf parsing
    if hasattr(client, "parse_error_ignorelist"):
        try:
            client.parse_error_ignorelist.append("WebcastLinkLayerMessage")
        except AttributeError:
            client.parse_error_ignorelist.add("WebcastLinkLayerMessage")

    # Silence internal parser error tracebacks so the console stays clean
    if hasattr(client, "logger"):
        import logging

        client.logger.setLevel(logging.CRITICAL)

    def write_bronze_event(event_type: str, data: dict):
        if not output_file:
            return
        payload = {
            "streamer": streamer,
            "event_type": event_type,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        with output_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        print(
            f"[CONNECTED] Successfully connected to live stream: https://www.tiktok.com/@{streamer}/live"
        )
        print(
            "Listening for incoming gifts, comments, and stats... (Press Ctrl+C to exit)\n"
        )

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        print(f"\n[DISCONNECTED] Disconnected from @{streamer}")
        stats.print_summary()

    @client.on(LiveEndEvent)
    async def on_live_end(event: LiveEndEvent):
        print(f"\n[ENDED] Stream ended by broadcaster: @{streamer}")
        stats.print_summary()

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        user_info = extract_user_info(event.user)
        gift_info = extract_gift_info(event)
        user_name = user_info["unique_id"]
        gift_name = gift_info["name"]
        diamond_count = gift_info["diamond_count"]
        streak = getattr(event, "repeat_count", 1)
        is_streaking = getattr(event, "streaking", False)
        value_usd = getattr(event, "value", None)

        # Write bronze event to raw lake
        write_bronze_event(
            "gift",
            {
                "user_id": user_info["user_id"],
                "unique_id": user_name,
                "nickname": user_info["nickname"],
                "gift_id": gift_info["gift_id"],
                "gift_name": gift_name,
                "diamond_count": diamond_count,
                "repeat_count": streak,
                "repeat_end": bool(getattr(event, "repeat_end", False)),
                "streaking": is_streaking,
                "usd_value": value_usd,
            },
        )

        if not is_streaking:
            stats.record_gift(user_name, gift_name, diamond_count, streak, value_usd)
            total_coins = diamond_count * streak
            approx_usd = total_coins * 0.005 if value_usd is None else value_usd
            print(
                f"[GIFT FINAL] @{user_name:<16} -> {streak}x {gift_name:<15} "
                f"({total_coins:,} coins | ~${approx_usd:,.2f})"
            )
        else:
            sys.stdout.write(
                f"\rCombo building: @{user_name} sending {gift_name} (x{streak})..."
            )
            sys.stdout.flush()

    @client.on(RoomUserSeqEvent)
    async def on_room_users(event: RoomUserSeqEvent):
        viewer_count = getattr(event, "total", 0)
        stats.last_viewer_count = viewer_count
        write_bronze_event(
            "room_users",
            {
                "viewer_count": viewer_count,
                "popularity": getattr(event, "popularity", 0),
            },
        )
        print(f"\n[ROOM STATS] Current Viewers: {viewer_count:,}")

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):
        user_info = extract_user_info(event.user)
        total_likes = getattr(event, "total", stats.total_likes)
        stats.total_likes = total_likes
        write_bronze_event(
            "like",
            {
                "user_id": user_info["user_id"],
                "unique_id": user_info["unique_id"],
                "batch_count": getattr(event, "count", 1),
                "total_room_likes": total_likes,
            },
        )

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        stats.total_comments += 1
        user_info = extract_user_info(event.user)
        comment_text = getattr(event, "comment", None) or getattr(event, "content", "")
        write_bronze_event(
            "comment",
            {
                "user_id": user_info["user_id"],
                "unique_id": user_info["unique_id"],
                "nickname": user_info["nickname"],
                "comment": comment_text,
            },
        )
        print(f"[{user_info['unique_id']}]: {comment_text}")

    return client


def parse_args():
    parser = argparse.ArgumentParser(
        description="TikTok Live NPC Event Stream Ingestion & Discovery Probe"
    )
    parser.add_argument(
        "username",
        nargs="?",
        help="TikTok streamer username (without the @)",
    )
    parser.add_argument(
        "--output-jsonl",
        "-o",
        type=Path,
        default=None,
        help="Path to dump raw bronze JSONL events (e.g., data/raw_events.jsonl)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    streamer = args.username

    if not streamer:
        try:
            streamer = input(
                "Enter TikTok streamer username to probe (without @): "
            ).strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            sys.exit(0)

    if not streamer:
        print("Error: streamer username cannot be empty.")
        sys.exit(1)

    # Clean up leading @ if provided
    streamer = streamer.lstrip("@")

    output_path = args.output_jsonl
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Raw event lake enabled: saving events to {output_path.resolve()}")

    client = build_client(streamer, output_path)

    try:
        client.run()
    except KeyboardInterrupt:
        print("\nStopping ingestion.")
    except Exception as e:
        print(f"\n[ERROR] Error during stream connection: {e}")
        print(
            "Tip: Ensure the user is currently live, or verify that your IP is not rate-limited."
        )


if __name__ == "__main__":
    main()
