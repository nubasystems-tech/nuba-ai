"""الاختبار القاسي: مستمع لمحاضرة 1000 كلمة متواصلة، متحدث سريع 1.25x،
صوت منخفض جداً (-28dB) مع ضوضاء خفيفة — استماع من مسافة بعيدة.
نبث كأنه ميكروفون هاتف حقيقي (realtime) ونقيس ما يصل للطالب."""
import asyncio
import json
import struct
import time

import websockets

CHUNK = 8000  # 0.25s لكل chunk — نفس إيقاع المتصفح


async def main():
    pcm = open("/tmp/lecture_far.pcm", "rb").read()
    dur = len(pcm) // 32000
    print(f"🎯 محاضرة: 1000 كلمة / {dur}s متواصلة | متحدث سريع | صوت منخفض جداً")
    uri = "ws://localhost:8000/ws/live?lang=en&tts_lang=ar"
    ws = await websockets.connect(uri, open_timeout=20, max_size=2**22)
    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
    print(f"الحالة: {m.get('message','ready')}\n")

    t0 = time.time()
    results = {"source": 0, "translation": 0, "tts": 0, "words": 0}
    first_delays = []
    words_captured = 0
    finished = asyncio.Event()

    async def player():
        # بث realtime حقيقي: 0.25s صوت كل 0.25s — بلا توقف واحد طوال المحاضرة
        for i in range(0, len(pcm), CHUNK):
            await ws.send(pcm[i:i + CHUNK])
            await asyncio.sleep(0.25)
        # صمت ختامي
        await asyncio.sleep(0.5)
        await ws.send(struct.pack("<h", 0) * 16000)
        await asyncio.sleep(20)   # نجمع آخر النتائج
        finished.set()

    async def listener():
        last_speech_time = time.time()
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=25)
                m = json.loads(raw)
                t = time.time() - t0
                if m["type"] == "source":
                    results["source"] += 1
                    results["words"] += len(m["text"].split())
                    delay = t - (results["source"] * 0.25 + 0)  # تقريب
                    print(f"  +{t:6.1f}s 🎤 {m['text'][:70]}")
                elif m["type"] == "translation":
                    results["translation"] += 1
                    print(f"  +{time.time()-t0:6.1f}s 🌍 {m['text'][:70]}")
                elif m["type"] == "tts":
                    results["tts"] += 1
        except asyncio.TimeoutError:
            pass

    pt = asyncio.create_task(player())
    lt = asyncio.create_task(listener())
    await finished.wait()
    lt.cancel()
    try:
        await ws.close()
    except Exception:
        pass

    print("\n" + "=" * 62)
    print("📊 تقرير المستمع — المحاضرة القاسية (1000 كلمة سريعة منخفضة)")
    print("=" * 62)
    print(f"كلمات المحاضرة: 1000 | جمل التقطها التطبيق: {results['source']}")
    print(f"كلمات التُقطت فعلياً: {results['words']} ({results['words']/10:.0f}% من المحاضرة)")
    print(f"ترجمات وصلت: {results['translation']} | أصوات نطق: {results['tts']}")
    print(f"مدة الاختبار: {time.time()-t0:.0f}s")


asyncio.run(main())
