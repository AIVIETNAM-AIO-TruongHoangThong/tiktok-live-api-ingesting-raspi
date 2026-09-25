# TikTok Live Real-Time Lakehouse Pipeline

A production-grade, 24/7 event streaming and analytics lakehouse for TikTok Live (NPC & E-commerce streams).

```
[Raspberry Pi 4B - Edge Ingestion]
  ├── candidates.json (self-updating registry)
  ├── PK Battle Spider (auto-discovers peer creators)
  ├── 90s Value-Qualification Gate (drops inactive/dead streams)
  └── watcher.py (aiokafka producer)
             │
             │ TCP 19092 (gzip-compressed stream)
             ▼
[Dokploy VPS - Lakehouse Stack]
  ├── Redpanda (Kafka-compatible broker on dokploy-network)
  ├── ClickHouse (Gold Layer: real-time streaming analytics via Kafka Engine)
  ├── Kafka Connect (Bronze Layer: auto-drains raw JSON to MinIO S3)
  └── MinIO S3 (tiktok-lake bucket: cold archive + DuckDB nightly Parquet backfill)
```

---

## 1. Raspberry Pi Edge Ingestion (`pi/`)

The edge daemon is **fully autonomous**:
1. **Dynamic Candidate Pool (`candidates.json`):** Starts with seeds, auto-learns new creators.
2. **PK Battle Spider:** Automatically extracts battle opponents and co-hosts when creators battle, adding them to the pool.
3. **Value-Qualification Gate:** When a creator goes live, they are probed for a 90-second trial window. If they have $< 5$ viewers or 0 activity, they are disconnected early to save Pi slots. Qualified high-volume creators are streamed 24/7.
4. **Auto-Blacklisting:** Automatically removes handles that do not exist or cannot go live.

### Setup on Raspberry Pi
```bash
# 1. Clone repo
git clone https://github.com/youruser/tiktok-live-npc.git
cd tiktok-live-npc

# 2. Install uv & Pi dependencies (no DuckDB needed on Pi)
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv sync --extra pi

# 3. Configure VPS IP
nano pi/config.yaml   # replace YOUR_VPS_IP with your VPS host

# 4. Check candidates & test live status
uv run python pi/discover.py --list
uv run python pi/discover.py --probe

# 5. Run test daemon
uv run python pi/watcher.py

# 6. Deploy as 24/7 systemd service
sudo cp pi/tiktok-watcher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tiktok-watcher
sudo journalctl -u tiktok-watcher -f
```

### Discovery CLI (`pi/discover.py`)
```bash
# List all registered candidates and peak viewer records
uv run python pi/discover.py --list

# Probe which candidates are currently live right now
uv run python pi/discover.py --probe

# Manually add a streamer to a category
uv run python pi/discover.py --add streamer_name --category npc
uv run python pi/discover.py --add shop_name --category ecommerce
```

---

## 2. Dokploy VPS Lakehouse Setup (`vps/`)

### Step 1: Deploy ClickHouse
1. In Dokploy `shared-services` project, deploy **ClickHouse** from the template (one click).
2. Open ClickHouse web UI, paste and run `vps/clickhouse/init.sql` once.
   * This creates `tiktok_gifts`, `tiktok_comments`, `tiktok_likes`, `tiktok_room_viewers`, `tiktok_product_pins`.
   * Automatically streams messages from `redpanda:9092` via Kafka Engine into MergeTree tables.

### Step 2: Deploy Redpanda + Kafka Connect
1. Copy `vps/.env.example` to `vps/.env` and fill in your MinIO credentials.
2. Edit `vps/docker-compose.yml` to put your VPS public IP in `--advertise-kafka-addr`.
3. In Dokploy, deploy `vps/docker-compose.yml` as a Compose application in `shared-services`.
4. Register the S3 Bronze sink connector:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @vps/connector-s3-sink.json
```

### Step 3: Nightly DuckDB S3 Backfill (Optional)
Installs a 3am cron job that compacts Bronze JSON from MinIO into partitioned Parquet files:
```bash
crontab vps/crontab.txt
```

---

## 3. Local Single-Stream Testing Mode

To test a single streamer locally on your computer writing to a local JSONL file:

```bash
# Capture raw events
uv run main.py <username> --output-jsonl data/raw_events.jsonl

# Run DuckDB Bronze -> Silver -> Gold analytics
uv run python transform.py
```
