"""DuckDB ETL Pipeline: Bronze (Raw JSONL) -> Silver (DuckDB / Parquet) -> Gold (Analytics).

Transforms raw TikTok Live event streams into structured relational tables
and generates engagement and revenue analytics.
"""

import argparse
import sys
from pathlib import Path
import duckdb

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_JSONL = PROJECT_ROOT / "data" / "raw_events.jsonl"
DEFAULT_DB = PROJECT_ROOT / "data" / "tiktok_warehouse.duckdb"
DEFAULT_PARQUET_DIR = PROJECT_ROOT / "data" / "silver"


def run_pipeline(
    jsonl_path: Path,
    db_path: Path,
    export_parquet: bool = True,
    parquet_dir: Path = DEFAULT_PARQUET_DIR,
):
    if not jsonl_path.exists():
        print(f"[!] Error: Raw event file '{jsonl_path}' does not exist.")
        return

    print("=" * 70)
    print("STARTING DUCKDB DATA PIPELINE (Bronze -> Silver -> Gold)")
    print("=" * 70)
    print(f"Bronze Input : {jsonl_path.resolve()}")
    print(f"Silver DB    : {db_path.resolve()}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    # ---------------------------------------------------------
    # 1. Bronze staging view: inspect raw JSON
    # ---------------------------------------------------------
    con.execute(f"""
        CREATE OR REPLACE VIEW bronze_events AS
        SELECT * FROM read_json_auto('{jsonl_path.as_posix()}', ignore_errors=true);
    """)

    event_types = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT event_type FROM bronze_events"
        ).fetchall()
    ]
    print(f"\n[+] Detected Event Types in Lake: {event_types}")

    # ---------------------------------------------------------
    # 2. Silver Transformations: Structured & Typed Tables
    # ---------------------------------------------------------

    # --- Silver: Likes ---
    con.execute("""
        CREATE OR REPLACE TABLE silver_likes AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ) AS event_timestamp,
            streamer,
            data->>'user_id' AS user_id,
            COALESCE(data->>'unique_id', data->>'user') AS user_handle,
            TRY_CAST(data->>'batch_count' AS BIGINT) AS batch_count,
            TRY_CAST(data->>'total_room_likes' AS BIGINT) AS total_room_likes
        FROM bronze_events
        WHERE event_type = 'like';
    """)

    # --- Silver: Room Viewers ---
    con.execute("""
        CREATE OR REPLACE TABLE silver_room_viewers AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ) AS event_timestamp,
            streamer,
            TRY_CAST(data->>'viewer_count' AS BIGINT) AS viewer_count,
            TRY_CAST(COALESCE(data->>'popularity', '0') AS BIGINT) AS popularity
        FROM bronze_events
        WHERE event_type = 'room_users';
    """)

    # --- Silver: Gifts ---
    con.execute("""
        CREATE OR REPLACE TABLE silver_gifts AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ) AS event_timestamp,
            streamer,
            data->>'user_id' AS user_id,
            COALESCE(data->>'unique_id', data->>'user') AS user_handle,
            data->>'nickname' AS nickname,
            TRY_CAST(data->>'gift_id' AS BIGINT) AS gift_id,
            data->>'gift_name' AS gift_name,
            TRY_CAST(data->>'diamond_count' AS BIGINT) AS diamond_count,
            TRY_CAST(data->>'repeat_count' AS BIGINT) AS repeat_count,
            TRY_CAST(COALESCE(data->>'repeat_end', 'false') AS BOOLEAN) AS is_repeat_end,
            TRY_CAST(COALESCE(data->>'streaking', 'false') AS BOOLEAN) AS is_streaking,
            TRY_CAST(data->>'usd_value' AS DOUBLE) AS usd_value
        FROM bronze_events
        WHERE event_type = 'gift';
    """)

    # --- Silver: Comments ---
    con.execute("""
        CREATE OR REPLACE TABLE silver_comments AS
        SELECT
            CAST(received_at AS TIMESTAMPTZ) AS event_timestamp,
            streamer,
            data->>'user_id' AS user_id,
            COALESCE(data->>'unique_id', data->>'user') AS user_handle,
            data->>'nickname' AS nickname,
            data->>'comment' AS comment_text
        FROM bronze_events
        WHERE event_type = 'comment';
    """)

    # ---------------------------------------------------------
    # 3. Export to Parquet Data Lake
    # ---------------------------------------------------------
    if export_parquet:
        parquet_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n[+] Exporting Silver Tables to Parquet: {parquet_dir.resolve()}")
        for table in [
            "silver_likes",
            "silver_room_viewers",
            "silver_gifts",
            "silver_comments",
        ]:
            out_file = parquet_dir / f"{table}.parquet"
            con.execute(f"COPY {table} TO '{out_file.as_posix()}' (FORMAT PARQUET);")
            print(f"   [OK] {out_file.name}")

    # ---------------------------------------------------------
    # 4. Gold Layer: Executive Analytics & Reports
    # ---------------------------------------------------------
    print("\n" + "=" * 70)
    print("GOLD LAYER ANALYTICS & INSIGHTS")
    print("=" * 70)

    # 1. Event Overview
    print("\n[1] Overall Event Volume:")
    print(
        con.sql("""
        SELECT 
            event_type,
            count(*) AS total_records,
            min(received_at) AS first_event,
            max(received_at) AS last_event
        FROM bronze_events
        GROUP BY event_type
        ORDER BY total_records DESC
    """)
    )

    # 2. Viewer Metrics
    viewers_count = con.execute("SELECT count(*) FROM silver_room_viewers").fetchone()[
        0
    ]
    if viewers_count > 0:
        print("\n[2] Viewer Count Metrics:")
        print(
            con.sql("""
            SELECT 
                streamer,
                min(viewer_count) AS min_viewers,
                round(avg(viewer_count), 1) AS avg_viewers,
                max(viewer_count) AS peak_viewers,
                count(*) AS snapshot_intervals
            FROM silver_room_viewers
            GROUP BY streamer
        """)
        )

    # 3. Top Likers / Tap Velocity
    likes_count = con.execute("SELECT count(*) FROM silver_likes").fetchone()[0]
    if likes_count > 0:
        print("\n[3] Top Screen Tappers (Likes Leaderboard):")
        print(
            con.sql("""
            SELECT 
                user_handle,
                sum(batch_count) AS total_taps_contributed,
                count(*) AS tap_batches_sent,
                round(avg(batch_count), 1) AS avg_batch_size
            FROM silver_likes
            GROUP BY user_handle
            ORDER BY total_taps_contributed DESC
            LIMIT 5
        """)
        )

    # 4. Gift Revenue (if any)
    gifts_count = con.execute("SELECT count(*) FROM silver_gifts").fetchone()[0]
    if gifts_count > 0:
        print("\n[4] Gift Revenue & Catchphrase Analytics:")
        print(
            con.sql("""
            SELECT 
                gift_name,
                count(*) AS times_sent,
                sum(diamond_count * repeat_count) AS total_diamonds,
                round(sum(diamond_count * repeat_count) * 0.005, 2) AS approx_usd
            FROM silver_gifts
            WHERE is_streaking = false
            GROUP BY gift_name
            ORDER BY total_diamonds DESC
        """)
        )

    # 5. Comment Insights (if any)
    comments_count = con.execute("SELECT count(*) FROM silver_comments").fetchone()[0]
    if comments_count > 0:
        print("\n[5] Comment Activity:")
        print(
            con.sql("""
            SELECT 
                user_handle,
                count(*) AS total_comments
            FROM silver_comments
            GROUP BY user_handle
            ORDER BY total_comments DESC
            LIMIT 5
        """)
        )

    print(
        "\n[OK] Pipeline complete! DuckDB database & Parquet files ready for BI/Dashboarding."
    )


def main():
    parser = argparse.ArgumentParser(description="DuckDB TikTok Live ETL Pipeline")
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        default=DEFAULT_JSONL,
        help="Path to input Bronze raw JSONL file",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to target DuckDB file",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Skip exporting silver tables to Parquet files",
    )
    args = parser.parse_args()

    run_pipeline(
        jsonl_path=args.input,
        db_path=args.db,
        export_parquet=not args.no_parquet,
    )


if __name__ == "__main__":
    main()
