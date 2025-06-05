#!/usr/bin/env python3
"""Simple orchestration script that spawns pipeline workers for a stream."""

import argparse
import subprocess
import os
import signal
import sys


def start_process(cmd, env=None):
    """Start a subprocess and return it."""
    return subprocess.Popen(cmd, env=env)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream_url", required=True, help="HLS or RTMP URL")
    parser.add_argument("--channel", required=True, help="Streamer/channel name")
    parser.add_argument("--platform", default="twitch", help="Platform name")
    parser.add_argument("--stream_id", help="Unique stream ID")
    args = parser.parse_args()

    stream_id = args.stream_id or f"{args.channel}_live"
    env_base = os.environ.copy()
    env_base.update({
        "STREAM_ID": stream_id,
        "STREAMER": args.channel,
    })

    procs = []
    try:
        # Ingest worker
        procs.append(start_process([
            sys.executable, "-m", "pipeline.ingest_buffer",
            "--stream_url", args.stream_url,
            "--stream_id", stream_id,
        ], env=env_base))

        # Chat detector (only for Twitch/YouTube with chat)
        env_chat = env_base.copy()
        env_chat.update({"CHANNEL": args.channel})
        procs.append(start_process([
            sys.executable, "-m", "pipeline.chat_detector",
        ], env=env_chat))

        # Audio detector
        env_audio = env_base.copy()
        env_audio.update({"STREAM_URL": args.stream_url})
        procs.append(start_process([
            sys.executable, "-m", "pipeline.audio_detector",
        ], env=env_audio))

        # Highlight consumer -> clipping service
        procs.append(start_process([
            sys.executable, "-m", "pipeline.highlight_consumer",
        ], env=env_base))

        # Wait for processes
        while True:
            for p in procs:
                if p.poll() is not None:
                    raise RuntimeError(f"Process {p.args} exited with code {p.returncode}")
            signal.pause()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


if __name__ == "__main__":
    main()
