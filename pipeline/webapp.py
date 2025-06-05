#!/usr/bin/env python3
"""Simple web front-end for launching and viewing clips."""

import os
import subprocess
from typing import Dict

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape
import httpx

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
CLIPPING_SERVICE_URL = os.getenv("CLIPPING_SERVICE_URL", "http://localhost:8001")

app = FastAPI()

env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml'])
)

# Keep track of running orchestrator processes by channel name
procs: Dict[str, subprocess.Popen] = {}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    template = env.get_template("index.html")
    return template.render(running=list(procs.keys()))


@app.post("/start")
async def start(stream_url: str = Form(...), channel: str = Form(...)):
    cmd = [
        "python", "-m", "pipeline.orchestrator",
        "--stream_url", stream_url,
        "--channel", channel,
    ]
    proc = subprocess.Popen(cmd)
    procs[channel] = proc
    return RedirectResponse("/", status_code=303)


@app.post("/stop")
async def stop(channel: str = Form(...)):
    proc = procs.pop(channel, None)
    if proc:
        proc.terminate()
    return RedirectResponse("/", status_code=303)


@app.get("/clips", response_class=HTMLResponse)
async def clips(request: Request):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{CLIPPING_SERVICE_URL}/clips/recent?limit=10")
            clips = resp.json()
        except Exception:
            clips = []
    template = env.get_template("clips.html")
    return template.render(clips=clips)

