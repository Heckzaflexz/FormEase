#!/usr/bin/env python3
"""FastAPI service that receives highlight candidates and forwards them."""

from fastapi import FastAPI
from pydantic import BaseModel
import os
import json
from typing import Optional
from aiokafka import AIOKafkaProducer
import openai
from typing import Dict
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    app.state.producer = producer
    try:
        yield
    finally:
        await producer.stop()

app = FastAPI(lifespan=lifespan)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CONFIRMED_TOPIC = os.getenv("CONFIRMED_TOPIC", "confirmed_highlights")
OPENAI_KEY = os.getenv("GEMINI_API_KEY")
openai.api_key = OPENAI_KEY


class HighlightCandidate(BaseModel):
    stream_id: str
    streamer: str
    timestamp_utc: str
    reason: str
    chat_rate: Optional[float] = None
    rms: Optional[float] = None


async def verify_with_gemini(c: HighlightCandidate) -> Dict[str, str] | None:
    """Call Gemini (OpenAI) to confirm if a highlight candidate is worth keeping."""
    if not OPENAI_KEY:
        return {"is_highlight": True}
    prompt = (
        f"Streamer: {c.streamer}\n"
        f"Time: {c.timestamp_utc}\n"
        f"Reason: {c.reason}\n"
        f"Chat Rate: {c.chat_rate or 'n/a'}\n"
        f"RMS: {c.rms or 'n/a'}\n"
        "Should this be clipped? Respond in JSON with keys 'is_highlight',"
        " 'category', and 'confidence'."
    )
    try:
        resp = await openai.AsyncOpenAI().chat.completions.create(
            model="gpt-4o-code-interpreter",
            messages=[
                {"role": "system", "content": "You decide if a stream moment is a highlight."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
        )
        data = json.loads(resp.choices[0].message.content)
        return data
    except Exception:
        return {"is_highlight": True}




@app.post("/highlight_candidate")
async def highlight_candidate(c: HighlightCandidate):
    decision = await verify_with_gemini(c)
    if not decision or decision.get("is_highlight"):
        enriched = c.dict() | {
            "category": decision.get("category") if decision else None,
            "confidence": decision.get("confidence") if decision else None,
        }
        payload = json.dumps(enriched).encode()
        await app.state.producer.send_and_wait(CONFIRMED_TOPIC, payload)
        return {"status": "confirmed"}
    return {"status": "rejected"}

