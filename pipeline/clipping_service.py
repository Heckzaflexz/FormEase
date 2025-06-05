#!/usr/bin/env python3
"""Minimal clipping service using FFmpeg."""

import os
import subprocess
import uuid
import json
from datetime import datetime
from typing import Optional

import redis.asyncio as redis
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import boto3
import openai
from contextlib import asynccontextmanager

BUFFER_DIR = "/tmp/buffers"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DB_DSN = os.getenv("DB_DSN", "postgresql://user:pass@localhost:5432/highlights")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
NEW_CLIPS_TOPIC = os.getenv("NEW_CLIPS_TOPIC", "new_clips")
S3_BUCKET = os.getenv("S3_BUCKET")
OPENAI_KEY = os.getenv("GEMINI_API_KEY")
openai.api_key = OPENAI_KEY

@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    pg_pool = await asyncpg.create_pool(dsn=DB_DSN)
    app.state.producer = producer
    app.state.pg_pool = pg_pool
    try:
        yield
    finally:
        await producer.stop()
        await pg_pool.close()

app = FastAPI(lifespan=lifespan)


class ConfirmedHighlight(BaseModel):
    stream_id: str
    streamer: str
    timestamp_utc: str
    reason: str


async def slice_clip(ch: ConfirmedHighlight) -> tuple[str, str]:
    r = await redis.from_url(REDIS_URL)
    t0 = datetime.fromisoformat(ch.timestamp_utc.replace("Z", "+00:00"))
    start = t0.timestamp() - 5
    end = t0.timestamp() + 10
    segs = await r.zrangebyscore(f"buffer:{ch.stream_id}", start, end)
    await r.close()
    if not segs:
        raise RuntimeError("No segments")

    segs = [s.decode() for s in segs]
    buffer_dir = os.path.join(BUFFER_DIR, ch.stream_id)
    list_file = f"/tmp/{uuid.uuid4()}_list.txt"
    with open(list_file, "w") as f:
        for s in segs:
            f.write(f"file '{buffer_dir}/{s}'\n")
    concat_ts = f"/tmp/{uuid.uuid4()}_full.ts"
    subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file,
                    "-c", "copy", concat_ts], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    mp4_path = f"/tmp/{uuid.uuid4()}.mp4"
    subprocess.run(["ffmpeg", "-y", "-i", concat_ts, "-t", "15", "-c:v", "libx264",
                    "-c:a", "aac", mp4_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    gif_path = f"/tmp/{uuid.uuid4()}.gif"
    subprocess.run(["ffmpeg", "-y", "-i", mp4_path, "-t", "5",
                    "-vf", "fps=15,scale=480:-1:flags=lanczos",
                    "-loop", "0", gif_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return mp4_path, gif_path




@app.post("/clip")
async def create_clip(ch: ConfirmedHighlight):
    try:
        mp4, gif = await slice_clip(ch)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e))

    caption: Optional[str] = None
    if OPENAI_KEY:
        prompt = (
            f"Write a short caption for a highlight clip from {ch.streamer} at {ch.timestamp_utc}."
            " Keep it under 200 characters."
        )
        try:
            resp = openai.ChatCompletion.create(
                model="gpt-4o-code-interpreter",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )
            caption = resp.choices[0].message.content.strip()
        except Exception:
            caption = None

    mp4_url = mp4
    gif_url = gif
    if S3_BUCKET:
        s3 = boto3.client("s3")
        key_mp4 = f"{ch.streamer}/{uuid.uuid4()}.mp4"
        key_gif = f"{ch.streamer}/{uuid.uuid4()}.gif"
        s3.upload_file(mp4, S3_BUCKET, key_mp4, ExtraArgs={"ACL": "public-read"})
        s3.upload_file(gif, S3_BUCKET, key_gif, ExtraArgs={"ACL": "public-read"})
        mp4_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{key_mp4}"
        gif_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{key_gif}"

    clip_id = str(uuid.uuid4())
    async with app.state.pg_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO clips (clip_id, streamer, timestamp_utc, reason, mp4_url, gif_url, caption_text) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7)",
            clip_id, ch.streamer, ch.timestamp_utc, ch.reason, mp4_url, gif_url, caption
        )
    payload = {
        "clip_id": clip_id,
        "streamer": ch.streamer,
        "timestamp_utc": ch.timestamp_utc,
        "mp4_url": mp4_url,
        "gif_url": gif_url,
        "caption_text": caption,
    }
    await app.state.producer.send_and_wait(NEW_CLIPS_TOPIC, json.dumps(payload).encode())
    return payload


@app.get("/clips/recent")
async def recent_clips(limit: int = 10):
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be 1-100")
    async with app.state.pg_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT clip_id, streamer, timestamp_utc, mp4_url, gif_url, caption_text "
            "FROM clips ORDER BY timestamp_utc DESC LIMIT $1",
            limit,
        )
    return [dict(r) for r in rows]
