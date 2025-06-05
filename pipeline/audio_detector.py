#!/usr/bin/env python3
"""Detects audio RMS spikes from a livestream and notifies aggregator."""

import os
import subprocess
from datetime import datetime, timezone

import numpy as np
import requests

STREAM_URL = os.getenv("STREAM_URL", "")
STREAM_ID = os.getenv("STREAM_ID", "test_live")
STREAMER = os.getenv("STREAMER", "Test")
PUSH_URL = os.getenv("PUSH_URL", "http://localhost:8000/highlight_candidate")
SPIKE_FACTOR = float(os.getenv("SPIKE_FACTOR", 3.0))
WINDOW = int(os.getenv("WINDOW", 20))


def main():
    if not STREAM_URL:
        raise SystemExit("STREAM_URL not provided")

    proc = subprocess.Popen([
        "ffmpeg", "-loglevel", "error", "-i", STREAM_URL,
        "-vn", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "1",
        "-f", "s16le", "-"
    ], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    buf_size = 44100 * 2  # one second
    hist = []
    while True:
        raw = proc.stdout.read(buf_size)
        if len(raw) < buf_size:
            break
        samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768
        rms = np.sqrt(np.mean(samples ** 2))
        if len(hist) >= WINDOW:
            hist.pop(0)
        hist.append(rms)
        if len(hist) > 5:
            prev = np.mean(hist[:-1])
            if prev > 0 and rms / prev >= SPIKE_FACTOR:
                payload = {
                    "stream_id": STREAM_ID,
                    "streamer": STREAMER,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "reason": "audio_spike",
                    "rms": float(rms)
                }
                try:
                    requests.post(PUSH_URL, json=payload, timeout=1)
                except requests.RequestException:
                    pass
                hist.clear()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
