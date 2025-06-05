#!/usr/bin/env python3
"""Detects chat spikes on a Twitch channel and posts events to an aggregator."""

import asyncio
from datetime import datetime, timezone
import os

import requests
from twitchio.ext import commands

CHANNEL = os.getenv("CHANNEL", "test")
SPIKE_FACTOR = float(os.getenv("SPIKE_FACTOR", 3.0))
WINDOW_SECS = int(os.getenv("WINDOW_SECS", 30))
PUSH_URL = os.getenv("PUSH_URL", "http://localhost:8000/highlight_candidate")
BOT_NICK = os.getenv("BOT_NICK", "ClipBot")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CHAT_TOKEN = os.getenv("CHAT_TOKEN", "")
STREAM_ID = os.getenv("STREAM_ID", "test_live")
STREAMER = os.getenv("STREAMER", "Test")


class Detector(commands.Bot):
    def __init__(self):
        super().__init__(irc_token=CHAT_TOKEN, client_id=CLIENT_ID,
                         nick=BOT_NICK, prefix="!", initial_channels=[CHANNEL])
        self.ts = []

    async def event_ready(self):
        print(f"Connected to chat for #{CHANNEL}")

    async def event_message(self, message):
        now = datetime.now(timezone.utc).timestamp()
        self.ts.append(now)
        cutoff = now - WINDOW_SECS
        while self.ts and self.ts[0] < cutoff:
            self.ts.pop(0)

        last5 = len([t for t in self.ts if t >= now - 5]) / 5
        prior = len([t for t in self.ts if t < now - 5]) / max(1, WINDOW_SECS - 5)
        if prior > 0 and last5 / prior >= SPIKE_FACTOR:
            payload = {
                "stream_id": STREAM_ID,
                "streamer": STREAMER,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "reason": "chat_spike",
                "chat_rate": last5
            }
            try:
                requests.post(PUSH_URL, json=payload, timeout=1)
            except requests.RequestException:
                pass
            self.ts.clear()
        await self.handle_commands(message)


def main():
    bot = Detector()
    bot.run()


if __name__ == "__main__":
    main()
