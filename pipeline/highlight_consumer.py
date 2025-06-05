#!/usr/bin/env python3
"""Kafka consumer that forwards confirmed highlights to the clipping service."""

import asyncio
import json
import os

import aiohttp
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONFIRMED_TOPIC = os.getenv("CONFIRMED_TOPIC", "confirmed_highlights")
CLIP_ENDPOINT = os.getenv("CLIP_ENDPOINT", "http://localhost:8001/clip")


async def handle_message(session: aiohttp.ClientSession, data: dict):
    try:
        async with session.post(CLIP_ENDPOINT, json=data, timeout=10) as resp:
            await resp.text()
    except Exception:
        pass


async def main():
    consumer = AIOKafkaConsumer(
        CONFIRMED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="clip-consumer",
    )
    await consumer.start()
    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                try:
                    payload = json.loads(msg.value)
                except json.JSONDecodeError:
                    continue
                await handle_message(session, payload)
        finally:
            await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
