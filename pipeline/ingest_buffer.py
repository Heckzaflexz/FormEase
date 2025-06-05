#!/usr/bin/env python3
"""Simple ingest worker pulling a livestream into a rolling buffer."""

import argparse
import asyncio
import os
import subprocess
from datetime import datetime, timezone

import redis.asyncio as redis

SEGMENT_TIME = 6  # seconds
MAX_SEGMENTS = 10  # keep ~60 sec
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


async def ingest(stream_url: str, stream_id: str):
    buffer_dir = f"/tmp/buffers/{stream_id}"
    r = await redis.from_url(REDIS_URL)
    os.makedirs(buffer_dir, exist_ok=True)

    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-loglevel", "error", "-i", stream_url,
        "-c", "copy", "-f", "segment", "-segment_time", str(SEGMENT_TIME),
        "-reset_timestamps", "1", f"{buffer_dir}/segment_%03d.ts",
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
    )

    async def monitor():
        last_seen = -1
        redis_key = f"buffer:{stream_id}"
        while True:
            for fname in sorted(os.listdir(buffer_dir)):
                if not fname.startswith("segment_"):
                    continue
                index = int(fname.split("_")[1].split(".")[0])
                if index > last_seen:
                    last_seen = index
                    ts = datetime.now(timezone.utc).timestamp()
                    await r.zadd(redis_key, {fname: ts})
                    # cleanup
                    if await r.zcard(redis_key) > MAX_SEGMENTS:
                        oldest = (await r.zrange(redis_key, 0, 0))[0].decode()
                        await r.zrem(redis_key, oldest)
                        try:
                            os.remove(os.path.join(buffer_dir, oldest))
                        except FileNotFoundError:
                            pass
            await asyncio.sleep(1)

    mon_task = asyncio.create_task(monitor())
    await proc.wait()
    mon_task.cancel()
    await r.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream_url", required=True)
    parser.add_argument("--stream_id", required=True)
    args = parser.parse_args()

    try:
        asyncio.run(ingest(args.stream_url, args.stream_id))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
