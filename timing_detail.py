"""قياس زمني داخل _telnyx_stt_inline نفسها بوضع طوابع زمنية على كل خطوة."""
import asyncio
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
import websockets

KEY = os.environ.get("TELNYX_STT_API_KEY") or subprocess.run(
    ["grep", "-oP", "(?<=^TELNYX_STT_API_KEY=).+", os.path.expanduser("~/.hermes/.env")],
    capture_output=True, text=True).stdout.strip()


async def main():
    T = {}
    t0 = time.time()
    content = open("/tmp/phone_test.webm", "rb").read()
    tmp = Path(tempfile.mkdtemp())
    in_path = tmp / "in.webm"
    in_path.write_bytes(content)
    mp3_path = in_path.with_suffix(".mp3")
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", str(in_path), "-vn", "-ar", "16000", "-ac", "1",
        "-c:a", "libmp3lame", "-q:a", "5", str(mp3_path),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    T["ffmpeg"] = time.time() - t0

    t1 = time.time()
    ws_url = "wss://api.telnyx.com/v2/speech-to-text/transcription?transcription_engine=Cohere&input_format=mp3&language=ar"
    async with websockets.connect(ws_url, additional_headers={"Authorization": f"Bearer {KEY}"}, open_timeout=15) as ws:
        T["connect"] = time.time() - t1
        t2 = time.time()
        await ws.send(mp3_path.read_bytes())
        await ws.send('{"type":"CloseStream"}')
        T["send"] = time.time() - t2
        t3 = time.time()
        got = False
        first_final = None
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
                if first_final is None:
                    first_final = time.time() - t3
            elif d.get("type") == "CloseStream":
                break
        T["recv_until_final"] = first_final
        T["recv_extra_idle"] = (time.time() - t3) - (first_final or 0)
    print(json.dumps(T, indent=2))
    print(f"الإجمالي: {time.time()-t0:.2f}s")


asyncio.run(main())
