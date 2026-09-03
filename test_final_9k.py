"""الاختبار الأقصى: محاضرة 54 دقيقة — 9000 كلمة سريعة (1.4x) بصوت ضعيف 8% + ضجيج قاعة.
نبث realtime عبر النفق الخارجي (نفس مسار هاتف المستخدم) ونقيس كل شيء.
لتقصير زمن الاختبار العملي: نبث أول 8 دقائق (نمط كافٍ إحصائياً ~1300 كلمة) ونقيس عليها."""
import asyncio
import json
import struct
import time

import websockets

CHUNK = 8000  # 0.25s — إيقاع المتصفح الحقيقي


async def main():
    pcm = open("/tmp/lecture_9k.pcm", "rb").read()[:32000 * 60 * 8]  # أول 8 دقائق
    dur = len(pcm) // 32000
    print(f"🎯 الاختبار الأقصى: {dur//60} دقيقة بث متواصل | 9000 كلمة/ساعة | سريع 1.4x | ضعيف 8% + ضجيج", flush=True)
    print(f"   (نقيس على أول 8 دقائق = عينة إحصائية من محاضرة 54 دقيقة كاملة)", flush=True)

    ws = await websockets.connect(
        "wss://housewives-initiative-claimed-markers.trycloudflare.com/ws/live?lang=en&tts_lang=ar&tts=0",
        open_timeout=30, max_size=2**22)
    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
    print(f"الاتصال عبر النفق: {m.get('message')}\n", flush=True)

    t0 = time.time()
    stats = {"partial": 0, "source": 0, "translation": 0, "words_src": 0, "words_tr": 0}
    gaps_src = []
    last_src = None
    finished = asyncio.Event()

    async def player():
        # بث realtime صارم: 0.25s صوت كل 0.25s بلا توقف — 8 دقائق متواصلة
        for i in range(0, len(pcm), CHUNK):
            await ws.send(pcm[i:i + CHUNK])
            await asyncio.sleep(0.25)
        await asyncio.sleep(2)
        await ws.send(struct.pack("<h", 0) * 16000)
        await asyncio.sleep(10)   # نجمع آخر النتائج
        finished.set()

    async def listener():
        nonlocal last_src
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            m = json.loads(raw)
            t = time.time() - t0
            if m["type"] == "source_partial":
                stats["partial"] += 1
            elif m["type"] == "source":
                stats["source"] += 1
                wc = len(m["text"].split())
                stats["words_src"] += wc
                if last_src is not None:
                    gaps_src.append(t - last_src)
                last_src = t
                print(f"  +{t/60:4.1f}min 🎤 ({wc}ك) {m['text'][:45]}", flush=True)
            elif m["type"] == "translation":
                stats["translation"] += 1
                stats["words_tr"] += len(m["text"].split())

    pt = asyncio.create_task(player())
    lt = asyncio.create_task(listener())
    await finished.wait()
    lt.cancel()
    try:
        await ws.close()
    except Exception:
        pass

    print("\n" + "=" * 62, flush=True)
    print(f"📊 التقرير الأقصى — {dur//60} دقائق بث متواصل", flush=True)
    print("=" * 62, flush=True)
    est_words = int(9000 * (dur / 3255))   # نصيبنا من المحاضرة
    print(f"الوقت: {time.time()-t0:.0f}s | 📝 لحظي: {stats['partial']} | 🎤 دفعات: {stats['source']} | 🌍 ترجمات: {stats['translation']}", flush=True)
    print(f"كلمات متوقعة في هذه المدة: ~{est_words} | كلمات ملتقطة: {stats['words_src']} ({stats['words_src']/max(est_words,1)*100:.0f}%)", flush=True)
    if gaps_src:
        import statistics
        print(f"متوسط الفاصل بين الدفعات: {statistics.mean(gaps_src):.1f}s | أطول: {max(gaps_src):.1f}s", flush=True)
    cov = stats["translation"] / max(stats["source"], 1) * 100
    print(f"تغطية الترجمة للدفعات: {cov:.0f}%", flush=True)
    verdict = "✅ ممتاز" if cov > 95 and stats["words_src"]/max(est_words,1) > 0.85 else ("🟡 جيد" if cov > 80 else "❌ يحتاج إصلاح")
    print(f"الحكم: {verdict}", flush=True)


asyncio.run(main())
