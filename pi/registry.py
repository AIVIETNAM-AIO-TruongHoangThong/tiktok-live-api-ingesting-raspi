"""
Dynamic Streamer Candidate Registry & Discovery Manager.

Maintains a self-updating database of TikTok Live streamers (candidates.json),
tracking their discovery source (seed, PK battle spider, search),
qualification status (untested, qualified, on cooldown, blacklisted),
category (npc, ecommerce), and behavioral archetype (pure_robotic, physical_challenge,
pk_battler, beauty, fashion, tech, food).
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("registry")

DEFAULT_CANDIDATES_FILE = Path(__file__).resolve().parent / "candidates.json"

# Verified active starter seeds with taxonomy
DEFAULT_SEEDS = [
    # --- NPC Streams ---
    {"username": "manhuynn", "category": "npc", "archetype": "physical_challenge"},
    {"username": "milktea_girl2", "category": "npc", "archetype": "pk_battler"},
    {"username": "natuecoco", "category": "npc", "archetype": "pure_robotic"},
    # --- E-Commerce Streams (Retail Verticals) ---
    {"username": "halinhofficial", "category": "ecommerce", "archetype": "beauty"},
    {"username": "maybelline_vn", "category": "ecommerce", "archetype": "beauty"},
    {"username": "lorealparis_vn", "category": "ecommerce", "archetype": "beauty"},
    {"username": "dirtycoins.vn", "category": "ecommerce", "archetype": "fashion"},
    {"username": "routine_vietnam", "category": "ecommerce", "archetype": "fashion"},
    {"username": "levents.vn", "category": "ecommerce", "archetype": "fashion"},
    {"username": "fptshop.official", "category": "ecommerce", "archetype": "tech"},
    {"username": "haidilaovietnam", "category": "ecommerce", "archetype": "food"},
]


class CandidateRegistry:
    """Manages dynamic candidate pool, qualification statuses, archetypes, and spidering."""

    def __init__(self, file_path: Path = DEFAULT_CANDIDATES_FILE):
        self.file_path = file_path
        self.candidates: dict[str, dict[str, Any]] = {}
        self.load()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _iso_now(self) -> str:
        return self._now().isoformat()

    def load(self) -> None:
        """Load candidates from disk, or initialize with verified seeds if missing."""
        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.candidates = data.get("streamers", {})
                    log.info(f"Loaded {len(self.candidates)} streamers from {self.file_path.name}")
                    return
            except Exception as e:
                log.warning(f"Failed to read {self.file_path}, re-initializing: {e}")

        # Seed initial pool
        self.candidates = {}
        for item in DEFAULT_SEEDS:
            self.add_candidate(
                username=item["username"],
                category=item["category"],
                archetype=item["archetype"],
                source="seed",
            )
        self.save()
        log.info(f"Initialized registry with {len(self.candidates)} verified seeds.")

    def save(self) -> None:
        """Persist candidates to disk."""
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": self._iso_now(),
                "total_streamers": len(self.candidates),
                "streamers": self.candidates,
            }
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log.error(f"Failed to save registry to {self.file_path}: {e}")

    def add_candidate(
        self,
        username: str,
        category: str,
        archetype: str = "general",
        source: str = "spider",
    ) -> bool:
        """
        Add a newly discovered streamer to the candidate pool.
        Returns True if newly added, False if already known.
        """
        username = username.strip().lstrip("@").lower()
        if not username:
            return False

        if username in self.candidates:
            # Update archetype if specified and currently general
            if archetype != "general" and self.candidates[username].get("archetype") == "general":
                self.candidates[username]["archetype"] = archetype
                self.save()
            return False

        self.candidates[username] = {
            "username": username,
            "category": category,
            "archetype": archetype,
            "source": source,
            "discovered_at": self._iso_now(),
            "status": "candidate",       # candidate -> qualified | cooldown | blacklisted
            "cooldown_until": None,
            "stats": {
                "total_sessions": 0,
                "peak_viewers": 0,
                "total_diamonds_seen": 0,
                "last_live": None,
                "last_checked": None,
            },
        }
        log.info(f"[DISCOVERY] Added @{username} ({category}/{archetype}) from source: {source}")
        self.save()
        return True

    def get_actionable_streamers(
        self,
        active_streamers: set[str],
        limit: int = 10,
    ) -> list[tuple[str, str, str]]:
        """
        Returns list of (username, category, archetype) ready to be checked for live status.
        Prioritizes:
          1. 'qualified' streamers (proven high-value) whose cooldown expired
          2. 'candidate' streamers (new discoveries needing probation)
        """
        now = self._now()
        ready = []
        active_clean = {u.lower() for u in active_streamers}

        for username, data in self.candidates.items():
            if username in active_clean:
                continue

            status = data.get("status", "candidate")
            if status == "blacklisted":
                continue

            cooldown = data.get("cooldown_until")
            if cooldown:
                try:
                    cd_dt = datetime.fromisoformat(cooldown)
                    if now < cd_dt:
                        continue  # Still on cooldown
                except Exception:
                    pass

            priority = 1 if status == "qualified" else (2 if status == "candidate" else 3)
            archetype = data.get("archetype", "general")
            ready.append((priority, username, data["category"], archetype))

        ready.sort(key=lambda x: x[0])
        return [(u, cat, arch) for _, u, cat, arch in ready[:limit]]

    def record_checked_offline(self, username: str, next_check_delay_sec: int = 180) -> None:
        """Mark that a streamer was checked and found offline."""
        username = username.lower()
        if username in self.candidates:
            cooldown_time = self._now() + timedelta(seconds=next_check_delay_sec)
            self.candidates[username]["cooldown_until"] = cooldown_time.isoformat()
            self.candidates[username]["stats"]["last_checked"] = self._iso_now()

    def record_probation_result(
        self,
        username: str,
        passed: bool,
        metrics: dict[str, Any],
        cooldown_hours_on_fail: int = 3,
    ) -> None:
        """
        Record the result of the 90s qualification probation.
        """
        username = username.lower()
        if username not in self.candidates:
            return

        user_data = self.candidates[username]
        stats = user_data["stats"]

        stats["total_sessions"] = stats.get("total_sessions", 0) + 1
        stats["peak_viewers"] = max(stats.get("peak_viewers", 0), metrics.get("viewers", 0))
        stats["total_diamonds_seen"] = stats.get("total_diamonds_seen", 0) + metrics.get("diamonds", 0)
        stats["last_live"] = self._iso_now()

        if passed:
            user_data["status"] = "qualified"
            user_data["cooldown_until"] = None
            log.info(
                f"[QUALIFIED] @{username} passed probation! "
                f"({metrics.get('viewers')} viewers, {metrics.get('diamonds')} diamonds, {metrics.get('gifts')} gifts)"
            )
        else:
            user_data["status"] = "cooldown"
            cooldown_time = self._now() + timedelta(hours=cooldown_hours_on_fail)
            user_data["cooldown_until"] = cooldown_time.isoformat()
            log.warning(
                f"[PROBATION FAILED] @{username} low value "
                f"({metrics.get('viewers')} viewers, {metrics.get('diamonds')} diamonds). "
                f"Cooldown for {cooldown_hours_on_fail}h."
            )

        self.save()

    def record_stream_end(self, username: str) -> None:
        """When a live broadcast ends, set a 15-minute cooldown before next check."""
        username = username.lower()
        if username in self.candidates:
            cooldown_time = self._now() + timedelta(minutes=15)
            self.candidates[username]["cooldown_until"] = cooldown_time.isoformat()
            self.save()

    def mark_invalid(self, username: str, reason: str = "invalid_user") -> None:
        """Permanently blacklist a username so it is never polled again."""
        username = username.lower()
        if username in self.candidates:
            self.candidates[username]["status"] = "blacklisted"
            self.candidates[username]["blacklist_reason"] = reason
            log.info(f"[BLACKLISTED] @{username}: {reason}")
            self.save()
