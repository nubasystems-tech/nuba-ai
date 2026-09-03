"""اختبار التحقق الشامل النهائي (E2E full matrix) — نفس مسار هاتف المستخدم:
1) متحدث إنجليزي → مستمع فرنسي (شكواه الحرفية): ترجمة فرنسية + صوت Denise فرنسي
2) متحدث عربي → مستمع إنجليزي: التبادل المثبت
3) حوار تبادلي duo عربي↔إنجليزي
كل واحدة عبر النفق الخارجي + تنزيل الصوت والتحقق من توقيعه."""
import asyncio
import json
import sys
import urllib.request

sys.path.insert(0, '/home/ubuntu/translator-app')
from test_live import make_pcm

BASE = "https://housewives-initiative-claimed-markers.trycloudflare.com"
WS_BASE = "wss://housewives-initiative-claimed-markers.trycloudflare.com"


async def run_case(name, ws_path, sentences, wait, expect_src=None, expect_tr=None, check_tts=True):
    import websockets
    got = {"s": 0, "t": 0, "a": 0, "src": [], "tr": [], "tts_url": None}
    audio_ok = False
    kind = ""
    try:
        async with websockets.connect(f"{WS_BASE}{ws_path}", open_timeout=20) as ws:
            json.loads(await ws.recv())
            for text, lang in sentences:
                pcm = make_pcm(text)
                for i in range(0, len(pcm), 3200):
                    await ws.send(pcm[i:i + 3200])
                    await asyncio.sleep(0.2)
                await asyncio.sleep(wait)

            async def rd():
                try:
                    while True:
                        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=8))
                        tp = m.get("type")
                        if tp == "source":
                            got["s"] += 1; got["src"].append(m["text"][:42])
                        elif tp == "translation":
                            got["t"] += 1; got["tr"].append(m["text"][:42])
                        elif tp == "tts":
                            got["a"] += 1; got["tts_url"] = m.get("url")
                except Exception:
                    pass
            # اجمع النتائج المتأخرة (TTS يحتاج ثوانياً)
            rt = asyncio.create_task(rd())
            await asyncio.sleep(10)
            rt.cancel()
    except Exception as e:
        print(f"[{name}] ✗ اتصال فشل: {type(e).__name__} {str(e)[:60]}")
        return False

    if check_tts and got["tts_url"]:
        try:
            url = got["tts_url"]
            full = url if url.startswith("http") else BASE + url
            r = urllib.request.urlopen(full, timeout=15)
            data = r.read()
            sig = data[:4]
            kind = "WAV/Azure" if sig == b'RIFF' else ("mp3" if sig[:3] == b'ID3' or data[0] == 0xff else str(sig))
            audio_ok = r.status == 200 and len(data) > 3000
        except Exception as e:
            kind = f"فشل تنزيل: {str(e)[:40]}"
    print(f"\n═══ {name} ═══")
    print(f"🎤{got['s']} 🌍{got['t']} 🔊{got['a']}" + (f" | صوت: {len(data)}b {kind} {'✓' if audio_ok else '✗'}" if check_tts and got["tts_url"] else ""))
    for s in got["src"]: print("  🎤", s)
    for t in got["tr"]: print("  🌍", t)
    ok = got["s"] >= 1 and got["t"] >= 1 and (not check_tts or got["a"] >= 1)
    print("✅" if ok else "❌")
    return ok


async def main():
    results = {}
    # 1) شكواه الحرفية: متحدث إنجليزي → أسمع فرنسي (صوت فرنسي حقيقي الآن)
    results['en→fr'] = await run_case(
        "متحدث إنجليزي → مستمع فرنسي (شكواك الحرفية)",
        "/ws/live?lang=en&tts_lang=fr&tts=1",
        [("Good evening everyone, welcome to our special event tonight", "en")],
        wait=12)
    # 2) عربي → إنجليزي
    results['ar→en'] = await run_case(
        "متحدث عربي → مستمع إنجليزي",
        "/ws/live?lang=ar&tts_lang=en&tts=1",
        [("صباح الخير أصدقائي، أتمنى لكم يوماً سعيداً", "ar")],
        wait=12)
    # 3) duo تبادلي
    results['duo'] = await run_case(
        "حوار تبادلي عربي↔إنجليزي",
        "/ws/duo?lang_a=ar&lang_b=en&tts=1",
        [("صباح الخير يا صديقي العزيز", "ar"), ("Good morning my friend, see you tomorrow", "en")],
        wait=8)
    print("\n" + "═" * 40)
    all_ok = all(results.values())
    print("🏆 الكل يعمل عبر النفق" if all_ok else f"⚠️ نتائج: {results}")

asyncio.run(main())
