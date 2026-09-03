"""قياس زمني عبر venv الخادم نفسه — لعزل سبب الفرق 4.4s مقابل 0.6s."""
import asyncio
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import websockets

KEY = os.environ.get("TELNYX_STT_API_KEY") or subprocess.run(
    ["grep", "-oP", "(?<=^TELNYX_STT_API_KEY=).+", os.path.expanduser("~/.hermes/.env")],
    capture_output=True, text=True).stdout.strip()


async def main():
    t0 = time.time()
    tmp = Path(tempfile.mkdtemp())
    in_path = tmp / "in.webm"
    in_path.write_bytes(open("/tmp/phone_test.webm", "rb").read())
    mp3 = in_path.with_suffix(".mp3")
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", str(in_path), "-vn", "-ar", "16000", "-ac", "1",
        "-c:a", "libmp3lame", "-q:a", "5", str(mp3),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    t1 = time.time()
    ws_url = "wss://api.telnyx.com/v2/speech-to-text/transcription?transcription_engine=Cohere&input_format=mp3&language=ar"
    async with websockets.connect(ws_url, additional_headers={"Authorization": f"Bearer {KEY}"}, open_timeout=15) as ws:
        await ws.send(mp3.read_bytes())
        await ws.send('{"type":"CloseStream"}')
        got = False
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=8 if not got else 0.4)
            except Exception:
                break
            if isinstance(raw, bytes):
                continue
            d = json.loads(raw)
            if d.get("is_final") and d.get("transcript"):
                got = True
            elif d.get("type") == "CloseStream":
                break
        t2 = time.time()
    print(f"ffmpeg: {t1-t0:.2f}s | WS كامل: {t2-t1:.2f}s | إجمالي: {t2-t0:.2f}s")


asyncio.run(main())
