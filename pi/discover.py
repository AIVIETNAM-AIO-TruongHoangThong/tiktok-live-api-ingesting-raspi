"""
Discovery CLI & Candidate Pool Management Tool.

Allows inspecting known streamers, manually injecting new candidates,
checking current live statuses, and reviewing streamer performance metrics.

Usage:
    uv run python pi/discover.py --list
    uv run python pi/discover.py --probe
    uv run python pi/discover.py --add username --category npc
    uv run python pi/discover.py --add shop_name --category ecommerce
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from TikTokLive import TikTokLiveClient
try:
    from registry import CandidateRegistry
except ImportError:
    from pi.registry import CandidateRegistry


async def probe_candidates(registry: CandidateRegistry):
    print("=" * 65)
    print("PROBING CANDIDATES LIVE STATUS")
    print("=" * 65)

    candidates = list(registry.candidates.values())
    if not candidates:
        print("No candidates registered.")
        return

    live_count = 0
    for idx, c in enumerate(candidates, 1):
        username = c["username"]
        cat = c["category"]
        status = c.get("status", "candidate")

        # Skip blacklisted accounts during probe
        if status == "blacklisted":
            continue

        await asyncio.sleep(1.5)  # Avoid burst rate-limiting

        try:
            client = TikTokLiveClient(unique_id=username)
            is_live = await client.is_live()
            badge = "[LIVE NOW]" if is_live else "[Offline]"
            if is_live:
                live_count += 1
            print(
                f"[{idx:02d}/{len(candidates):02d}] {badge:<12} | @{username:<20} | {cat:<10} | [{status}]"
            )
        except Exception as e:
            err_msg = str(e).lower()
            if (
                "not capable of going live" in err_msg
                or "does not exist" in err_msg
                or "user_not_found" in err_msg
            ):
                registry.mark_invalid(username, reason="incapable_of_live")
                print(
                    f"[{idx:02d}/{len(candidates):02d}] [Blacklist] | @{username:<20} | Inactive/Invalid"
                )
            else:
                print(
                    f"[{idx:02d}/{len(candidates):02d}] [Error]     | @{username:<20} | {e}"
                )

    print("\n" + "=" * 65)
    print(f"Summary: {live_count} / {len(candidates)} creators currently LIVE.")
    print("=" * 65)


def list_candidates(registry: CandidateRegistry):
    print("=" * 88)
    print(f"REGISTERED CANDIDATE POOL ({len(registry.candidates)} total creators)")
    print("=" * 88)
    print(
        f"{'Username':<22} {'Category':<11} {'Archetype':<20} {'Status':<12} {'Peak Viewers'}"
    )
    print("-" * 88)

    sorted_candidates = sorted(
        registry.candidates.values(),
        key=lambda x: (0 if x.get("status") == "qualified" else 1, x["category"]),
    )

    for c in sorted_candidates:
        stats = c.get("stats", {})
        peak = stats.get("peak_viewers", 0)
        status = c.get("status", "candidate")
        arch = c.get("archetype", "general")
        print(
            f"@{c['username']:<21} {c['category']:<11} {arch:<20} {status:<12} {peak:>8}"
        )
    print("=" * 88)


def main():
    parser = argparse.ArgumentParser(description="TikTok Live Candidate Discovery Tool")
    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all known candidate streamers, archetypes, and stats",
    )
    parser.add_argument(
        "--probe",
        "-p",
        action="store_true",
        help="Probe all registered candidates to check if live now",
    )
    parser.add_argument(
        "--add",
        "-a",
        type=str,
        help="Add a new streamer username to the candidate registry",
    )
    parser.add_argument(
        "--category",
        "-c",
        choices=["npc", "ecommerce"],
        default="npc",
        help="Category for added streamer",
    )
    parser.add_argument(
        "--archetype",
        "-t",
        default="general",
        help="Behavioral archetype (e.g. pure_robotic, physical_challenge, pk_battler, beauty, fashion, tech, food)",
    )
    args = parser.parse_args()

    registry = CandidateRegistry()

    if args.add:
        added = registry.add_candidate(
            args.add,
            category=args.category,
            archetype=args.archetype,
            source="cli_manual",
        )
        if added:
            print(f"[OK] Added @{args.add} ({args.category}/{args.archetype}) to candidate pool.")
        else:
            print(f"[INFO] @{args.add} is already in the candidate pool.")
        return

    if args.probe:
        asyncio.run(probe_candidates(registry))
        return

    # Default to --list
    list_candidates(registry)


if __name__ == "__main__":
    main()
