#!/usr/bin/env python3
"""Detects sudden visual changes in a livestream using OpenCV."""

import os
import time
from datetime import datetime, timezone

import requests
import cv2
import numpy as np

STREAM_URL = os.getenv("STREAM_URL", "")
STREAM_ID = os.getenv("STREAM_ID", "test_live")
STREAMER = os.getenv("STREAMER", "Test")
PUSH_URL = os.getenv("PUSH_URL", "http://localhost:8000/highlight_candidate")
INTERVAL = float(os.getenv("INTERVAL", 1.0))
SPIKE_FACTOR = float(os.getenv("SPIKE_FACTOR", 3.0))
WINDOW = int(os.getenv("WINDOW", 20))


def main():
    if not STREAM_URL:
        raise SystemExit("STREAM_URL not provided")

    cap = cv2.VideoCapture(STREAM_URL)
    if not cap.isOpened():
        raise SystemExit("Could not open stream")

    prev_gray = None
    diff_hist = []

    while True:
        ret, frame = cap.read()
        if not ret:
            time.sleep(INTERVAL)
            continue
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if prev_gray is None:
            prev_gray = gray
            time.sleep(INTERVAL)
            continue
        diff = cv2.absdiff(gray, prev_gray)
        mean_diff = float(np.mean(diff))
        prev_gray = gray

        if len(diff_hist) >= WINDOW:
            diff_hist.pop(0)
        diff_hist.append(mean_diff)

        if len(diff_hist) > 5:
            prev_mean = np.mean(diff_hist[:-1])
            if prev_mean > 0 and mean_diff / prev_mean >= SPIKE_FACTOR:
                payload = {
                    "stream_id": STREAM_ID,
                    "streamer": STREAMER,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "reason": "video_spike",
                    "frame_diff": mean_diff,
                }
                try:
                    requests.post(PUSH_URL, json=payload, timeout=1)
                except requests.RequestException:
                    pass
                diff_hist.clear()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
