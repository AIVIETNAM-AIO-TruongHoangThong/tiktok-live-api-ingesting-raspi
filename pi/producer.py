"""Shared aiokafka producer - one persistent TCP connection for all concurrent streams.

A single producer instance is reused across all TikTokLive client tasks running
on the Pi. This avoids the overhead of creating a new connection per streamer
and enables micro-batching (linger_ms) across all event streams simultaneously.
"""

import json
from aiokafka import AIOKafkaProducer

_producer: AIOKafkaProducer | None = None


async def get_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    """Return the shared producer, initializing it on first call."""
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
            compression_type="gzip",  # ~70% bandwidth reduction over home internet
            linger_ms=50,  # micro-batch events for 50ms before flushing
            max_batch_size=65536,  # 64KB max batch size
            request_timeout_ms=30000,  # 30s timeout for slow VPS connections
            retry_backoff_ms=500,
        )
        await _producer.start()
    return _producer


async def publish(producer: AIOKafkaProducer, topic: str, payload: dict) -> None:
    """Fire-and-forget publish. Does not block the TikTok event handler."""
    await producer.send(topic, value=payload)


async def close_producer() -> None:
    """Gracefully flush and close the producer on shutdown."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
