#!/usr/bin/env python3
"""Simple Discord posting bot for new clips."""

import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer
import discord
from discord.ext import commands

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
NEW_CLIPS_TOPIC = os.getenv("NEW_CLIPS_TOPIC", "new_clips")

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    print(f"Discord bot logged in as {bot.user}")
    bot.loop.create_task(consume())


async def consume():
    consumer = AIOKafkaConsumer(NEW_CLIPS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP, group_id="poster")
    await consumer.start()
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            channel = bot.get_channel(CHANNEL_ID)
            if channel:
                text = f"New clip by {data['streamer']} at {data['timestamp_utc']}"
                await channel.send(text)
                if url := data.get('gif_url'):
                    await channel.send(url)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
