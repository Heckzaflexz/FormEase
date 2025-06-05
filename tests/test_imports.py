import importlib

modules = [
    'pipeline.ingest_buffer',
    'pipeline.chat_detector',
    'pipeline.audio_detector',
    'pipeline.video_detector',
    'pipeline.detection_aggregator',
    'pipeline.clipping_service',
    'pipeline.posting_bot',
    'pipeline.highlight_consumer',
    'pipeline.orchestrator',
    'pipeline.webapp',
]

for m in modules:
    importlib.import_module(m)
