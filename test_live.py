"""اختبار بث حي كامل: نحاكي متصفح مستخدم يبث PCM 16k عبر WebSocket
ونقيس: زمن وصول كل جملة، الترجمة، وصول TTS — كأنه مؤتمر حقيقي متواصل."""
import asyncio
import json
import os
import struct
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

import websockets

KEY = os.environ.get("TELNYX_STT_API_KEY") or subprocess.run(
    ["grep", "-oP", "(?<=^TELNYX_STT_API_KEY=).+", os.path.expanduser("~/.hermes/.env")],
    capture_output=True, text=True).stdout.strip()


def make_pcm(text: str) -> bytes:
    """توليد PCM 16k mono من نص عبر Ultra TTS ثم تحويل ffmpeg."""
    import httpx
    r = httpx.post(
        "https://api.telnyx.com/v2/text-to-speech/speech",
        headers={"Authorization": f"Bearer {KEY}"},
        json={"text": text, "voice": "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062", "audio_format": "mp3"},
        timeout=30,
    )
    tmp = Path(tempfile.mkdtemp())
    mp3 = tmp / "s.mp3"
    mp3.write_bytes(r.content)
    raw = tmp / "s.raw"
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(mp3), "-ar", "16000", "-ac", "1", "-f", "s16le", str(raw)],
        capture_output=True, timeout=60,
    )
    return raw.read_bytes()


async def main():
    # جمل مؤتمر متتالية
    sentences = [
        "Good morning everyone, welcome to our annual conference.",
        "Today we will discuss the future of artificial intelligence in the Middle East.",
        "Our first speaker will talk about machine translation technology.",
        "Please welcome doctor Sarah Johnson to the stage.",
    ]
    pcms = []
    for s in sentences:
        pcm = await asyncio.to_thread(make_pcm, s)
        pcms.append(pcm)
        print(f"جهزت ({len(pcm)//32000:.1f}s): «{s[:50]}...»")

    print()
    print("════ البث الحي يبدأ ════")
    uri = "ws://localhost:8000/ws/live?lang=en&tts_lang=ar"
    async with websockets.connect(uri, open_timeout=20) as ws:
        ready = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        print("الخادم:", ready.get("message"))

        t_start = time.time()

        async def reader():
            """يستقبل نتائج البث بالتوازي مع الإرسال."""
            results = []
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    m = json.loads(raw)
                    if m["type"] == "source":
                        print(f"  +{time.time()-t_start:.1f}s 🎤 النص: «{m['text']}»")
                    elif m["type"] == "translation":
                        print(f"  +{time.time()-t_start:.1f}s 🌍 الترجمة: «{m['text']}»")
                    elif m["type"] == "tts":
                        print(f"  +{time.time()-t_start:.1f}s 🔊 صوت الترجمة جاهز")
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass
            return results

        async def sender():
            """يبث الجمل بفواصل طبيعية كأن متحدثاً يتكلم مع توقف قصير بين الجمل."""
            for i, pcm in enumerate(pcms):
                # بث realtime: chunk 8KB (~0.25s) كل 0.25s
                for j in range(0, len(pcm), 8000):
                    await ws.send(pcm[j:j+8000])
                    await asyncio.sleep(0.25)
                # فاصل بين الجمل (كما يتوقف المتحدث نفسه)
                if i < len(pcms) - 1:
                    await asyncio.sleep(1.2)
            # نبض صامت لضمان إخراج آخر جملة (endpointing يقطع بعد 300ms صمت)
            await asyncio.sleep(2)

        await asyncio.gather(sender(), asyncio.wait_for(reader(), timeout=75) if False else reader())

    print(f"════ انتهى البث في {time.time()-t_start:.1f}s ════")


if __name__ == "__main__":
    asyncio.run(main())
