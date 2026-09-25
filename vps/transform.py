"""
VPS DuckDB Bronze -> Silver Transform (S3 version).

Reads raw JSON event files from MinIO S3 (Bronze layer),
transforms them into structured Parquet files (Silver layer),
and writes the Parquet files back to MinIO S3.

This runs as a nightly cron job at 3am UTC.
It is NOT the primary analytics path - use ClickHouse for live dashboards.
Use this for: historical backfills, reprocessing, Python notebook ad-hoc analysis.

Usage:
    source vps/.env && python vps/transform.py
    python vps/transform.py --date 2026-09-24   # reprocess a specific date
    python vps/transform.py --dry-run            # show event counts without writing
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Config from environment (loaded from vps/.env by the cron job)
# ---------------------------------------------------------------------------

MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER") or os.environ.get(
    "MINIO_ACCESS_KEY"
)
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD") or os.environ.get(
    "MINIO_SECRET_KEY"
)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
BRONZE_BUCKET = "tiktok-lake"
SILVER_PREFIX = "silver"


def check_env() -> None:
    """Fail fast if required environment variables are missing."""
    missing = [
        k
        for k, v in {
            "MINIO_ROOT_USER (or MINIO_ACCESS_KEY)": MINIO_ACCESS_KEY,
            "MINIO_ROOT_PASSWORD (or MINIO_SECRET_KEY)": MINIO_SECRET_KEY,
        }.items()
        if not v
    ]
    if missing:
        print(f"[!] Missing environment variables: {', '.join(missing)}")
        print("    Run: source vps/.env && python vps/transform.py")
        sys.exit(1)


def build_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection configured for MinIO S3."""
    con = duckdb.connect()

    # Strip http:// prefix for the endpoint setting
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    use_ssl = MINIO_ENDPOINT.startswith("https://")

    con.execute(f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{MINIO_ACCESS_KEY}',
            SECRET '{MINIO_SECRET_KEY}',
            ENDPOINT '{endpoint}',
            USE_SSL {str(use_ssl).lower()},
            URL_STYLE path
        );
    """)

    return con


def run_transform(target_date: str, dry_run: bool = False) -> None:
    """
    Transform Bronze JSON for a given date into Silver Parquet.

    Args:
        target_date: Date string in YYYY-MM-DD format.
        dry_run:     If True, print event counts but do not write Parquet.
    """
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    print("=" * 70)
    print(f"DuckDB Bronze -> Silver Transform | Date: {target_date}")
    print(f"Source: s3://{BRONZE_BUCKET}/tiktok.events.*/{year}/{month}/{day}/")
    print(f"Mode:   {'DRY RUN (no writes)' if dry_run else 'WRITE Parquet to S3'}")
    print("=" * 70)

    con = build_connection()

    # ------------------------------------------------------------------
    # Bronze view: all raw JSON for this date across all hours and topics
    # ------------------------------------------------------------------
    bronze_glob = f"s3://{BRONZE_BUCKET}/tiktok.events.*/year={year}/month={month}/day={day}/*/*.json"

    con.execute(f"""
        CREATE OR REPLACE VIEW bronze_events AS
        SELECT *
        FROM read_json_auto('{bronze_glob}', ignore_errors=true);
    """)

    # Quick count to validate data exists
    try:
        total = con.execute("SELECT count(*) FROM bronze_events").fetchone()[0]
        print(f"\n[+] Bronze events found: {total:,}")
        if total == 0:
            print("[!] No events for this date. Exiting.")
            return
    except Exception as e:
        print(f"[!] Could not read Bronze layer: {e}")
        print(f"    Check that {bronze_glob} has files.")
        return

    # Event type breakdown
    print("\n[+] Event breakdown:")
    breakdown = con.execute("""
        SELECT event_type, count(*) AS cnt
        FROM bronze_events
        GROUP BY event_type
        ORDER BY cnt DESC
    """).fetchall()
    for event_type, cnt in breakdown:
        print(f"    {event_type:<20} {cnt:>8,}")

    if dry_run:
        print("\n[DRY RUN] Skipping Parquet write.")
        return

    # ------------------------------------------------------------------
    # Silver transformations: structured + typed tables
    # ------------------------------------------------------------------

    # Silver: Gifts
    con.execute("""
        CREATE OR REPLACE TABLE silver_gifts AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ)                        AS event_timestamp,
            streamer,
            category,
            COALESCE(archetype, 'general')                          AS archetype,
            data->>'user_id'                                        AS user_id,
            COALESCE(data->>'unique_id', data->>'user')             AS user_handle,
            data->>'nickname'                                       AS nickname,
            TRY_CAST(data->>'gift_id' AS BIGINT)                    AS gift_id,
            data->>'gift_name'                                      AS gift_name,
            TRY_CAST(data->>'diamond_count' AS BIGINT)              AS diamond_count,
            TRY_CAST(data->>'repeat_count' AS BIGINT)               AS repeat_count,
            TRY_CAST(COALESCE(data->>'streaking', 'false') AS BOOLEAN) AS is_streaking,
            TRY_CAST(COALESCE(data->>'repeat_end', 'false') AS BOOLEAN) AS is_repeat_end,
            TRY_CAST(data->>'usd_value' AS DOUBLE)                  AS usd_value
        FROM bronze_events
        WHERE event_type = 'gift';
    """)

    # Silver: Comments
    con.execute("""
        CREATE OR REPLACE TABLE silver_comments AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ)             AS event_timestamp,
            streamer,
            category,
            COALESCE(archetype, 'general')               AS archetype,
            data->>'user_id'                             AS user_id,
            COALESCE(data->>'unique_id', data->>'user')  AS user_handle,
            data->>'nickname'                            AS nickname,
            data->>'comment'                             AS comment_text
        FROM bronze_events
        WHERE event_type = 'comment';
    """)

    # Silver: Likes
    con.execute("""
        CREATE OR REPLACE TABLE silver_likes AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ)                    AS event_timestamp,
            streamer,
            category,
            COALESCE(archetype, 'general')                      AS archetype,
            data->>'user_id'                                    AS user_id,
            COALESCE(data->>'unique_id', data->>'user')         AS user_handle,
            TRY_CAST(data->>'batch_count' AS BIGINT)            AS batch_count,
            TRY_CAST(data->>'total_room_likes' AS BIGINT)       AS total_room_likes
        FROM bronze_events
        WHERE event_type = 'like';
    """)

    # Silver: Room Viewers
    con.execute("""
        CREATE OR REPLACE TABLE silver_room_viewers AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ)                 AS event_timestamp,
            streamer,
            category,
            COALESCE(archetype, 'general')                   AS archetype,
            TRY_CAST(data->>'viewer_count' AS BIGINT)        AS viewer_count,
            TRY_CAST(COALESCE(data->>'popularity', '0') AS BIGINT) AS popularity
        FROM bronze_events
        WHERE event_type = 'room_users';
    """)

    # Silver: Product Pins (e-commerce only)
    con.execute("""
        CREATE OR REPLACE TABLE silver_product_pins AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ)             AS event_timestamp,
            streamer,
            category,
            COALESCE(archetype, 'general')               AS archetype,
            data->>'product_id'                          AS product_id,
            data->>'title'                               AS title,
            data->>'price'                               AS price,
            data->>'open_url'                            AS open_url,
            TRY_CAST(data->>'live_product_count' AS BIGINT) AS live_product_count
        FROM bronze_events
        WHERE event_type = 'product_pin';
    """)

    # ------------------------------------------------------------------
    # Write Silver Parquet back to MinIO S3
    # ------------------------------------------------------------------
    print("\n[+] Writing Silver Parquet to MinIO S3...")

    silver_tables = [
        "silver_gifts",
        "silver_comments",
        "silver_likes",
        "silver_room_viewers",
        "silver_product_pins",
    ]

    for table in silver_tables:
        row_count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        if row_count == 0:
            print(f"    SKIP {table} (0 rows)")
            continue

        s3_path = (
            f"s3://{BRONZE_BUCKET}/{SILVER_PREFIX}/{table.replace('silver_', '')}/"
            f"year={year}/month={month}/day={day}/"
        )
        con.execute(f"""
            COPY {table} TO '{s3_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PARTITION_BY (category, streamer),
                OVERWRITE_OR_IGNORE true
            );
        """)
        print(f"    OK  {table:<30} {row_count:>8,} rows -> {s3_path}")

    print("\n[OK] Silver Parquet layer updated successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DuckDB Bronze -> Silver S3 Transform (nightly VPS job)"
    )
    parser.add_argument(
        "--date",
        default=(datetime.now(timezone.utc) - timedelta(days=0)).strftime("%Y-%m-%d"),
        help="Date to process in YYYY-MM-DD format (default: today UTC)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show event counts without writing Parquet files",
    )
    args = parser.parse_args()

    check_env()
    run_transform(target_date=args.date, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
