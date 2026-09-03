"""قياس زمن الاستجابة الحقيقي للبث الحي: جملة واحدة بأقصى سرعة."""
import asyncio
import json
import time

import websockets

from test_live import make_pcm


async def main():
    pcm = await asyncio.to_thread(make_pcm, "This is a latency test for the live translator.")
    dur = len(pcm) // 32000
    print(f"جملة {dur}s صوت — إرسال فوري كامل ثم قياس الاستجابة...")
    t0 = time.time()
    uri = "ws://localhost:8000/ws/live?lang=en&tts_lang=ar"
    async with websockets.connect(uri, open_timeout=20) as ws:
        await asyncio.wait_for(ws.recv(), timeout=10)  # ready
        await ws.send(pcm)
        got = {}
        while len(got) < 3:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            m = json.loads(raw)
            got[m["type"]] = time.time() - t0
            if m["type"] == "source":
                print(f"  🎤 النص وصل بعد {got['source']:.2f}s من بدء الإرسال (الصوت {dur}s)")
            if m["type"] == "translation":
                print(f"  🌍 الترجمة: «{m['text']}»")
            if m["type"] == "tts":
                print(f"  🔊 صوت الترجمة جاهز بعد {got['tts']:.2f}s")
    base = got.get("source", dur)
    print()
    print(f"⏱️ من نهاية الكلام حتى النص: {got['source']-dur:.2f}s")
    print(f"⏱️ من النص حتى الترجمة: {got['translation']-got['source']:.2f}s")
    print(f"⏱️ من الترجمة حتى الصوت: {got['tts']-got['translation']:.2f}s")
    print(f"⏱️ الإجمالي (نهاية كلام → سماع جاهز): {got['tts']-dur:.2f}s")


asyncio.run(main())
