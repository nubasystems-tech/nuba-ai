"""اختبار السبع جولات: نقاش عن العلوم والتطور بلغات متناوبة في قاعة واحدة.
7 جمل متتالية: أستاذ وطلاب يتناقشون عن التطور العلمي — كل واحد بلغته."""
import asyncio
import json
import struct
import subprocess
import time

import httpx
import websockets

KEY = ""
FAN_WS = "ws://localhost:8000/ws/multi?tts_lang=ar"

# 7 جولات نقاش عن العلوم والتطور — لغات متناوبة (الطالب العربي + زملاء دوليون)
ROUNDS = [
    ("en", "Charles Darwin published the theory of evolution in eighteen fifty nine.", "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"),
    ("ar", "نظرية التطور غيّرت فهمنا للحياة على الأرض بشكل جذري.", "Telnyx.Bayan.Ahmed"),
    ("fr", "La sélection naturelle explique l'adaptation des espèces.", "Telnyx.KokoroTTS.ff_siwis"),
    ("en", "Modern genetics confirmed Darwin's predictions with DNA evidence.", "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"),
    ("ar", "الجينات الحديثة أثبتت صحة تنبؤات داروين بالحمض النووي.", "Telnyx.Bayan.Ahmed"),
    ("fr", "Les mutations génétiques alimentent l'évolution des espèces.", "Telnyx.KokoroTTS.ff_siwis"),
    ("en", "Artificial intelligence is now accelerating scientific discovery itself.", "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"),
]


async def make_voice(text, voice, fname):
    for attempt in range(3):
        try:
            r = httpx.post("https://api.telnyx.com/v2/text-to-speech/speech",
                          headers={"Authorization": f"Bearer {KEY}"},
                          json={"text": text, "voice": voice, "audio_format": "mp3"}, timeout=40)
            if r.status_code == 200 and len(r.content) > 2000:
                break
        except Exception:
            pass
        await asyncio.sleep(2)
    else:
        raise SystemExit("فشل TTS")
    open(fname, "wb").write(r.content)
    dur = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration","-of","csv=p=0",fname],capture_output=True,text=True).stdout.strip()
    d = float(dur)+0.5 if dur else 10
    af = f"anoisesrc=color=brown:amplitude=0.003:d={d}[n];[0:a]volume=0.5[m];[m][n]amix=inputs=2:duration=first[a]"
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

    print("🎯 السبع جولات: نقاش العلوم والتطور — قاعة متعددة اللغات + ضجيج", flush=True)
    clips = []
    for i, (lang, text, voice) in enumerate(ROUNDS):
        pcm = await make_voice(text, voice, f"/tmp/evo_{i}.mp3")
        clips.append((lang, pcm))
        print(f"  جولة {i+1} ({lang}): {len(pcm)//32000}s", flush=True)

    ws = await websockets.connect(FAN_WS, open_timeout=25, max_size=2**22)
    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
    print(f"القاعة: {m.get('message')}\n", flush=True)

    t0 = time.time()
    seen = []

    async def player():
        for lang, pcm in clips:
            for i in range(0, len(pcm), 8000):
                await ws.send(pcm[i:i+8000])
                await asyncio.sleep(0.25)
            await asyncio.sleep(0.8)
        await asyncio.sleep(1)
        await ws.send(struct.pack("<h", 0) * 16000)

    async def listener():
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            m = json.loads(raw)
            t = time.time() - t0
            if m["type"] == "source":
                flag = {"ar": "🇸🇦", "en": "🇬🇧", "fr": "🇫🇷", "zh-CN": "🇨🇳", "tr": "🇹🇷"}.get(m.get("speaker_lang", ""), "🌐")
                print(f"  +{t:5.1f}s {flag} {m['text'][:60]}", flush=True)
                seen.append((m.get("speaker_lang"), m["text"]))
            elif m["type"] == "translation":
                print(f"        🌍 {m['text'][:60]}", flush=True)

    pt = asyncio.create_task(player())
    lt = asyncio.create_task(listener())
    await pt
    await asyncio.sleep(10)
    lt.cancel()
    try:
        await ws.close()
    except Exception:
        pass

    print("\n" + "=" * 62)
    print("📊 تقرير السبع جولات — نقاش العلوم والتطور")
    print("=" * 62)
    exp = [l for l, _, _ in ROUNDS]
    got = [l for l, _ in seen]
    match = sum(1 for a, b in zip(exp, got) if a == b)
    print(f"الجولات المتوقعة: {exp}")
    print(f"اللغات الملتقطة:  {got}")
    print(f"✅ التبديل التلقائي الصحيح: {match}/{len(exp)}")
    print(f"إجمالي الجمل الملتقطة: {len(seen)}")


asyncio.run(main())
