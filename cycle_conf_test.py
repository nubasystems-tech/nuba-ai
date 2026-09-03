"""دورة اختبار مؤتمر متعدد اللغات — أصوات ضعيفة + ضجيج + سرعة 1.35x
يبني المقاطع ثم يبثها realtime عبر /ws/multi ويسجل كل النتائج."""
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, "/home/ubuntu/translator-app")
os.chdir("/home/ubuntu/translator-app")
KEY = os.environ.get("TELNYX_STT_API_KEY") or subprocess.run(
    ["grep", "-oP", "(?<=^TELNYX_STT_API_KEY=).+", os.path.expanduser("~/.hermes/.env")],
    capture_output=True, text=True).stdout.strip()

VOICE = "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062"

def make_tts(text: str) -> bytes:
    import httpx
    r = httpx.post("https://api.telnyx.com/v2/text-to-speech/speech",
                    headers={"Authorization": f"Bearer {KEY}"},
                    json={"text": text, "voice": VOICE, "audio_format": "mp3"},
                    timeout=40)
    assert r.status_code == 200 and len(r.content) > 2000, f"TTS فشل {r.status_code}"
    return r.content

def degrade(mp3_bytes: bytes, tag: str) -> bytes:
    """كلام 10% + ضجيج قاعة + تسريع 1.35x → PCM 16k mono."""
    tmp = Path("/tmp/cyc1")
    m = tmp / f"{tag}.mp3"
    m.write_bytes(mp3_bytes)
    out = tmp / f"{tag}_raw.pcm"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", "-i", str(m),
        "-filter_complex",
        f"[0:a]volume=0.10,atempo=1.35[v];"
        f"anoisesrc=color=brown:amplitude=0.015:d=999[n];[v][n]amix=inputs=2:duration=first:weights=1 0.6[a]",
        "-map", "[a]", "-ar", "16000", "-ac", "1", "-f", "s16le", str(out),
    ], capture_output=True, timeout=120)
    return out.read_bytes()

SPEAKERS = [
    ("en", "Artificial intelligence is reshaping every industry in the world today"),
    ("ar", "الذكاء الاصطناعي يغير شكل المستقبل في كل المجالات"),
    ("fr", "Nous devons investir dans la recherche scientifique maintenant"),
    ("en", "The results show massive growth in the Middle East market"),
]

async def main():
    import websockets
    Path("/tmp/cyc1").mkdir(exist_ok=True)
    clips = []
    for i, (lang, text) in enumerate(SPEAKERS):
        mp3 = await asyncio.to_thread(make_tts, text)
        pcm = await asyncio.to_thread(degrade, mp3, f"s{i}")
        clips.append((lang, text, pcm))
        print(f"جهزت {i+1}/4 [{lang}] {len(pcm)//32000:.1f}s ضعيف+ضجيج+سريع", flush=True)

    results = {"source": 0, "translation": 0, "tts": 0, "pairs": [], "first_at": None}
    t0 = time.time()
    url = "ws://localhost:8000/ws/multi?tts_lang=ar"
    async with websockets.connect(url) as ws:
        ready = json.loads(await ws.recv())
        print("القاعة:", ready["message"], flush=True)

        async def reader():
            try:
                while True:
                    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=50))
                    t = m.get("type")
                    if t == "source":
                        results["source"] += 1
                        if results["first_at"] is None:
                            results["first_at"] = time.time() - t0
                        results["pairs"].append(("🎤", m.get("speaker_lang"), m["text"][:44]))
                    elif t == "translation":
                        results["translation"] += 1
                        results["pairs"].append(("🌍", m.get("speaker_lang"), m["text"][:44]))
                    elif t == "tts":
                        results["tts"] += 1
            except Exception:
                pass

        rt = asyncio.create_task(reader())
        for lang, text, pcm in clips:
            for i in range(0, len(pcm), 3200):
                await ws.send(pcm[i:i + 3200])
                await asyncio.sleep(0.2)
            await asyncio.sleep(4.5)
        await asyncio.sleep(10)
        rt.cancel()

    print(f"\n🎤 {results['source']}/4 | 🌍 {results['translation']} | 🔊 {results['tts']} | أول نتيجة +{results['first_at']:.1f}s" if results["first_at"] else "\nصفر نتائج!")
    for kind, sl, txt in results["pairs"]:
        print(f"  {kind} [{sl}] {txt}")
    got_langs = {sl for k, sl, _ in results["pairs"] if k == "🎤"}
    ok = results["source"] >= 3 and len(got_langs) >= 2
    print("✅ الدورة نجحت" if ok else "❌ الدورة فشلت", flush=True)

asyncio.run(main())
