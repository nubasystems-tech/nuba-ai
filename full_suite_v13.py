"""الاختبار الشامل النهائي v13.3 — كل الأوضاع:
1) live إنجليزي→عربي (سرعة + صوت)
2) duo عربي↔إنجليزي (اتجاهات + سرعة)
3) duo بنفس طلب هاتفه الخاطئ ar↔ar (الحارس يصحح)
4) /api/stt (وضع التسجيل عبر الخيط الجديد — Claude #6)
5) جملة قصيرة Hi (لا حجب)
6) المقاييس (بلا تسريب)"""
import asyncio
import json
import sys
import time
import urllib.request

sys.path.insert(0, '/home/ubuntu/translator-app')
from test_live import make_pcm

PASS, FAIL = [], []


async def test_live():
    import websockets
    t_end = None
    tr_at = None
    src = 0
    async with websockets.connect("ws://localhost:8000/ws/live?lang=en&tts_lang=ar&tts=1") as ws:
        json.loads(await ws.recv())
        marks = []

        async def rd():
            nonlocal tr_at, src
            try:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=40))
                    if m.get("type") == "source":
                        src += 1
                        marks.append(m["text"][:30])
                    elif m.get("type") == "translation":
                        tr_at = time.time()
                        marks.append("🌍 " + m["text"][:30])
            except Exception:
                pass

        rt = asyncio.create_task(rd())
        pcm = make_pcm("Good evening everyone, welcome to our conference today")
        for i in range(0, len(pcm), 3200):
            await ws.send(pcm[i:i + 3200]); await asyncio.sleep(0.2)
        t_end = time.time()
        await asyncio.sleep(12)
        rt.cancel()
    ok = src >= 1 and tr_at and (tr_at - t_end) < 3
    PASS.append("live en→ar") if ok else FAIL.append("live en→ar")
    print(f"1) live: جمل={src} ترجمة={'+' + f'{tr_at - t_end:.1f}s' if tr_at else 'لا'} {'✅' if ok else '❌'}")
    for m in marks[:4]:
        print("   ", m)


async def test_duo():
    import websockets
    events = []
    t_end = None
    async with websockets.connect("ws://localhost:8000/ws/duo?lang_a=ar&lang_b=en&tts=1") as ws:
        json.loads(await ws.recv())

        async def rd():
            try:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=45))
                    if m.get("type") in ("source", "translation"):
                        events.append((m.get("type"), m.get("speaker_lang"), m.get("target_lang"), m["text"][:26], time.time()))
            except Exception:
                pass

        rt = asyncio.create_task(rd())
        pcm = make_pcm("صباح الخير أصدقائي الأعزاء")
        for i in range(0, len(pcm), 3200):
            await ws.send(pcm[i:i + 3200]); await asyncio.sleep(0.2)
        await asyncio.sleep(6)
        pcm = make_pcm("Thank you my friend, I will see you tomorrow")
        for i in range(0, len(pcm), 3200):
            await ws.send(pcm[i:i + 3200]); await asyncio.sleep(0.2)
        t_end = time.time()
        await asyncio.sleep(12)
        rt.cancel()
    srcs = [e for e in events if e[0] == "source"]
    trs = [e for e in events if e[0] == "translation"]
    ar_ok = any(e[2] == "en" for e in trs if e[1] == "ar")
    en_ok = any(e[2] == "ar" for e in trs if e[1] == "en")
    first_tr = trs[0][4] - t_end if trs else None
    ok = ar_ok and en_ok and first_tr is not None and first_tr < 3.5
    PASS.append("duo اتجاهان") if ok else FAIL.append("duo اتجاهان")
    print(f"2) duo: عربي→en {'✅' if ar_ok else '❌'} | en→عربي {'✅' if en_ok else '❌'} | آخر ترجمة +{first_tr:.1f}s {'✅' if ok else '❌'}" if first_tr else "2) duo: لا ترجمة ❌")
    for e in events[:6]:
        print(f"    {e[0]} [{e[1]}→{e[2]}]: {e[3]}")


async def test_phone_request():
    import websockets
    got = []
    async with websockets.connect("ws://localhost:8000/ws/duo?lang_a=ar&lang_b=ar&tts=1") as ws:
        ready = json.loads(await ws.recv())
        langs = ready.get("langs")

        async def rd():
            try:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    if m.get("type") in ("source", "translation"):
                        got.append(m.get("text", "")[:24])
            except Exception:
                pass

        rt = asyncio.create_task(rd())
        pcm = make_pcm("صباح الخير")
        for i in range(0, len(pcm), 3200):
            await ws.send(pcm[i:i + 3200]); await asyncio.sleep(0.2)
        await asyncio.sleep(10)
        rt.cancel()
    ok = langs == ["en", "ar"] and len(got) >= 1
    PASS.append("حارس ar↔ar") if ok else FAIL.append("حارس ar↔ar")
    print(f"3) طلب هاتفه ar↔ar → صححه الحارس إلى {langs} | ترجمة وصلت: {'✅' if got else '❌'}")
    for g in got[:2]:
        print("   ", g)


async def test_stt_recording():
    # وضع التسجيل عبر Claude #6 (خيط مستقل)
    # WAV حقيقي (ترويسة RIFF) — make_pcm يعيد PCM خام يرفضه ffmpeg كـwav
    import io, wave
    raw_pcm = make_pcm("This is a recording mode test for the speech to text endpoint")
    _buf = io.BytesIO()
    _w = wave.open(_buf, 'wb'); _w.setnchannels(1); _w.setsampwidth(2); _w.setframerate(16000)
    _w.writeframes(raw_pcm)
    pcm = _buf.getvalue()
    import urllib.request
    boundary = "----nubatest"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; "
            f"filename=\"test.wav\"\r\nContent-Type: audio/wav\r\n\r\n").encode() + pcm + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        "http://localhost:8000/api/stt?lang=en",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    t0 = time.time()
    try:
        r = urllib.request.urlopen(req, timeout=90)
        d = json.loads(r.read())
        text = d.get("text", "")
        dt = time.time() - t0
        ok = "recording" in text.lower() or "test" in text.lower()
        PASS.append("/api/stt") if ok else FAIL.append("/api/stt")
        print(f"4) /api/stt (خيط مستقل): {dt:.1f}s → «{text[:40]}» {'✅' if ok else '❌'}")
    except Exception as e:
        FAIL.append("/api/stt")
        print(f"4) /api/stt فشل: {str(e)[:60]} ❌")


async def test_short():
    import websockets
    got = []
    async with websockets.connect("ws://localhost:8000/ws/duo?lang_a=ar&lang_b=en&tts=1") as ws:
        json.loads(await ws.recv())

        async def rd():
            try:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                    if m.get("type") == "source":
                        got.append(m["text"])
            except Exception:
                pass

        rt = asyncio.create_task(rd())
        pcm = make_pcm("Hi")
        for i in range(0, len(pcm), 3200):
            await ws.send(pcm[i:i + 3200]); await asyncio.sleep(0.2)
        await asyncio.sleep(10)
        rt.cancel()
    ok = len(got) >= 1
    PASS.append("جملة قصيرة Hi") if ok else FAIL.append("جملة قصيرة Hi")
    print(f"5) «Hi» القصيرة: {'مرت ✓ ' + got[0] if got else 'حُجبت ❌'}")


async def main():
    print("═" * 50)
    print("الاختبار الشامل v13.3 — كل الأوضاع")
    print("═" * 50)
    await test_live()
    await test_duo()
    await test_phone_request()
    await test_stt_recording()
    await test_short()
    # المقاييس
    r = urllib.request.urlopen("http://localhost:8000/api/metrics", timeout=10)
    m = r.read().decode()
    print("\n6) المقاييس:")
    print("   " + m.replace("\n", " | "))
    print("\n" + "═" * 50)
    print(f"🏆 نجح: {len(PASS)} | ❌ فشل: {len(FAIL)} {FAIL if FAIL else '— كل الاختبارات خضراء'}")


asyncio.run(main())
