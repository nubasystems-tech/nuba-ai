"""اختبار وضع القاعة متعددة اللغات: نقاش بحث — أستاذ وطلاب بلغات مختلفة مع ضجيج.
المتحدثون: أستاذ إنجليزي، طالب عربي، طالب فرنسي، طالب صيني — بالتناوب.
نقيس: هل تتبدل الترجمة تلقائياً حسب لغة كل متحدث؟ + الضجيج في الخلفية."""
import asyncio
import json
import struct
import subprocess
import time

import httpx
import websockets

KEY = ""  # يُملأ من env
FAN_WS = "ws://localhost:8000/ws/multi?tts_lang=ar"

# نقاش بحث حقيقي — 4 لغات تتبادل الحديث
DISCUSSION = [
    ("en", "The research methodology needs a larger sample size to be conclusive.", "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"),
    ("ar", "أنا أرى أن العينة الحالية كافية والنتائج واضحة جداً.", "Telnyx.Bayan.Ahmed"),
    ("fr", "Nous devons refaire l'expérience avant de publier les résultats.", "Telnyx.KokoroTTS.ff_siwis"),
    ("en", "Perhaps we can combine both approaches in the next phase.", "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"),
    ("ar", "فكرة ممتازة، أقترح البدء الأسبوع القادم.", "Telnyx.Bayan.Ahmed"),
]


async def make_voice(text, voice, fname, noise=True):
    for attempt in range(3):
        try:
            r = httpx.post("https://api.telnyx.com/v2/text-to-speech/speech",
                          headers={"Authorization": f"Bearer {KEY}"},
                          json={"text": text, "voice": voice, "audio_format": "mp3"}, timeout=40)
            if r.status_code == 200 and len(r.content) > 2000:
                break
            print(f"  (TTS محاولة {attempt+1}: HTTP {r.status_code})")
        except Exception as e:
            print(f"  (TTS خطأ {type(e).__name__} — إعادة {attempt+1})")
        await asyncio.sleep(2)
    else:
        raise SystemExit("فشل TTS 3 مرات")
    open(fname, "wb").write(r.content)
    # ضجيج القاعة: خفض الصوت قليلاً + ضوضاء خلفية (كافيتريا/قاعة نقاش)
    dur_probe = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration","-of","csv=p=0",fname],capture_output=True,text=True).stdout.strip()
    d = float(dur_probe)+0.5 if dur_probe else 10
    af = f"anoisesrc=color=brown:amplitude=0.003:d={d}[n];[0:a]volume=0.5[m];[m][n]amix=inputs=2:duration=first[a]" if noise else "[0:a]volume=0.5[a]"
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-loglevel", "error", "-i", fname,
        "-filter_complex", af, "-map", "[a]",
        "-ar", "16000", "-ac", "1", "-f", "s16le", "pipe:1",
        stdout=subprocess.PIPE)
    pcm, _ = await p.communicate()
    return pcm


async def main():
    global KEY
    import os
    KEY = os.environ["TELNYX_STT_API_KEY"]

    print("🎯 محاكاة: نقاش بحث في قاعة متعددة اللغات + ضجيج", flush=True)
    # نجهز أصوات المتحدثين
    clips = []
    for i, (lang, text, voice) in enumerate(DISCUSSION):
        pcm = await make_voice(text, voice, f"/tmp/dis_{i}.mp3")
        clips.append((lang, pcm))
        print(f"  متحدث {lang}: {len(pcm)//32000}s", flush=True)

    ws = await websockets.connect(FAN_WS, open_timeout=25, max_size=2**22)
    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
    print(f"القاعة: {m.get('message')}\n")

    t0 = time.time()
    seen = []

    async def player():
        # المتحدثون يتناوبون — فترات صمت قصيرة بينهم (ضجيج القاعة مستمر)
        for lang, pcm in clips:
            for i in range(0, len(pcm), 8000):
                await ws.send(pcm[i:i+8000])
                await asyncio.sleep(0.25)
            await asyncio.sleep(0.8)  # تبديل متحدث
        await asyncio.sleep(1)
        await ws.send(struct.pack("<h", 0) * 16000)

    async def listener():
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            m = json.loads(raw)
            t = time.time() - t0
            if m["type"] == "source":
                flag = {"ar": "🇸🇦", "en": "🇬🇧", "fr": "🇫🇷", "zh-CN": "🇨🇳"}.get(m.get("speaker_lang", ""), "🌐")
                print(f"  +{t:5.1f}s {flag} {m['text'][:55]}")
                seen.append((m.get("speaker_lang"), m["text"]))
            elif m["type"] == "translation":
                print(f"        🌍 → {m['text'][:55]}")

    pt = asyncio.create_task(player())
    lt = asyncio.create_task(listener())
    await pt
    await asyncio.sleep(10)
    lt.cancel()
    try:
        await ws.close()
    except Exception:
        pass

    print("\n" + "=" * 60)
    print("📊 تقرير القاعة متعددة اللغات")
    print("=" * 60)
    exp = [l for l, _, _ in DISCUSSION]
    got = [l for l, _ in seen]
    print(f"متحدثون متوقعون بالترتيب: {exp}")
    print(f"لغات التقطها النظام بالترتيب: {got}")
    match = sum(1 for a, b in zip(exp, got) if a == b)
    print(f"تبدل تلقائي صحيح: {match}/{len(exp)}")
    print(f"إجمالي جمل ملتقطة: {len(seen)}")


asyncio.run(main())
