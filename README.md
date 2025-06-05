# FormEase Streaming Highlights

This repository now provides a working example of an automated livestream clipping pipeline.  It ingests any HLS/RTMP stream, detects highlight events, produces short clips and exposes a small web front-end.

## Components

- **ingest_buffer.py** – pulls an HLS/RTMP stream into a rolling buffer of TS segments
- **chat_detector.py** – monitors Twitch chat for spikes in message rate
- **audio_detector.py** – watches audio RMS levels for sudden changes
- **video_detector.py** – detects abrupt visual changes using OpenCV
- **detection_aggregator.py** – FastAPI service that verifies highlight candidates and forwards them to Kafka
- **clipping_service.py** – combines buffered segments into short MP4/GIF clips
- **posting_bot.py** – simple Discord bot that posts newly created clips
- **highlight_consumer.py** – consumes confirmed highlights from Kafka and calls the clipping service
- **orchestrator.py** – helper script that launches the ingest worker and detectors for a single livestream
- **webapp.py** – minimal frontend for launching streams and browsing clips

## Quick start with Docker Compose

Docker compose is provided to run Redis, Kafka, PostgreSQL and the FormEase services.  Build the image and start the stack:

```bash
docker compose up --build
```

Once running, visit [http://localhost:8000](http://localhost:8000) to start a stream and view recent clips.  Default credentials for PostgreSQL are `user` / `pass`; adjust environment variables in `docker-compose.yml` as needed.

These scripts are still simplified and should be reviewed before production use, but they are functional end-to-end with the provided compose setup.
