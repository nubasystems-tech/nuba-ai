"""محاكاة مؤتمر GenAI كامل: خطاب 60 دقيقة إنجليزي → بث حي عبر /ws/live → قياس كل شيء.

بنية الاختبار:
- 12 فقرة × ~5 دقائق كلام = 60 دقيقة محاكاة (نستخدم تسريع 4x لتقليل زمن الاختبار الفعلي
  مع الحفاظ على تسلسل realtime بالنسبة للبث — البث يرسل بنفس معدل PCM الفعلي)
- للاختبار المُسرّع: نرسل أسرع من realtime بمعامل SPEED (جميع الأزمنة المقاسة تُقسم عليه)
- نقيس: عدد الجمل الملتقطة/المفقودة، دقة النص، زمن الترجمة، الانقطاعات، استهلاك الذاكرة
"""
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

SPEED = float(os.environ.get("SPEED", "4"))  # 4x = ساعة تصبح 15 دقيقة اختبار

# ===== نص مؤتمر GenAI واقعي: 14 فقرة ≈ محتوى ساعة كاملة =====
SECTIONS = [
    "Good morning everyone, and welcome to the Generative AI Summit. My name is Doctor Michael Reeves, and I am the chief technology officer at Nexus Labs. Over the next hour, we will explore how generative artificial intelligence is reshaping software development, content creation, and enterprise automation. Let me begin with a short story. Three years ago, a team of five engineers needed six months to build a basic customer service chatbot. Today, a single developer can build the same system in one afternoon. This is the scale of change we are witnessing.",
    "First, let us define what we mean by generative artificial intelligence. Unlike traditional software that follows explicit rules, generative models learn patterns from massive datasets and produce entirely new content. They write text, generate images, compose music, and even write computer code. The underlying technology is called a transformer architecture, introduced in twenty seventeen. It relies on a mechanism called attention, which allows the model to weigh the importance of different words in a sentence.",
    "The training process happens in two stages. Pre-training teaches the model language patterns from trillions of words scraped from the internet, books, and code repositories. Fine-tuning then specializes the model for specific tasks, using carefully curated examples. Human feedback reinforcement learning further aligns the model with human preferences, making responses more helpful and safer.",
    "Now let us talk about the economics. Training a frontier model costs tens of millions of dollars in compute. However, inference costs have dropped a hundred fold in two years. A million tokens of generation cost five dollars last year, and today it costs less than fifty cents. This price collapse is what enables the startup explosion we see across the Middle East and Africa.",
    "In our region specifically, we see three major adoption patterns. First, customer service automation, where companies replace expensive call centers with multilingual AI agents. Second, content localization, where marketing materials are adapted to Arabic, Turkish, and French automatically. Third, software modernization, where legacy government systems are documented and upgraded with AI assistance.",
    "Let me share a concrete case study from our work in Riyadh. A telecommunications company with fourteen million subscribers was losing customers due to slow support. Average waiting time was eleven minutes. We deployed an Arabic speaking AI agent that handles billing questions, package upgrades, and technical troubleshooting. Today, the agent resolves sixty eight percent of calls without human intervention, and customer satisfaction rose from three point one to four point six out of five.",
    "The technical architecture behind such an agent is worth examining. It starts with speech recognition, converting the customer voice into text. A language model then understands the intent and retrieves relevant information from a knowledge base. Finally, speech synthesis delivers the answer in natural Arabic. Each stage now has dedicated providers, and the entire pipeline can respond in under one second.",
    "However, I must be honest about the limitations. These models hallucinate. They confidently state wrong facts. They sometimes invent laws, statistics, or citations that do not exist. In customer service, a hallucinated promise can create legal liability. Therefore, production systems require guardrails, retrieval from verified sources, and human escalation paths. Never deploy a language model in a regulated industry without these protections.",
    "The second major challenge is latency. Conversations feel natural only when responses arrive quickly. Research shows that a one second delay breaks the illusion of intelligence. This is why we moved from recorded processing to live streaming pipelines, and why edge computing matters for real time voice applications. Our measurements in Dubai showed that streaming cut perceived latency from eight seconds to under one second.",
    "Let me now address the question everyone asks: will artificial intelligence replace software engineers? My answer is no, but it will change what engineers do. The job is shifting from writing code to designing systems, reviewing machine output, and owning outcomes. Junior developers who adopt these tools are now as productive as seniors were five years ago. Teams that reject adoption will simply be outcompeted.",
    "Looking forward, I see three trends dominating the next two years. First, multimodal models that understand voice, image, and video together. Second, small specialized models that run on phones and work offline, protecting privacy. Third, agent frameworks that allow models to use tools, browse the web, and complete multi step tasks autonomously. The combination of these three will create what we call autonomous enterprise.",
    "Before I conclude, I want to acknowledge the risks. Data privacy regulations in our region are strictening. The Saudi data protection law and the Egyptian data protection law impose heavy penalties for misuse of personal information. Any AI system that processes customer data must implement encryption, access controls, and audit logs from day one. Compliance is not optional, it is a competitive advantage.",
    "My final message to this audience is simple. The window for building regionally relevant artificial intelligence is open now. Our languages, our dialects, and our cultural context are underserved by global models. The teams that build Arabic first products will own this market. Do not wait for permission. Start building this week.",
    "Thank you very much for your attention. I now welcome your questions, and I look forward to our discussion. The slides from today presentation will be shared with all registered attendees by email this evening.",
]


async def make_pcm(text: str) -> bytes:
    """نص → صوت Ultra → PCM 16k mono."""
    def _gen():
        return httpx.post(
            "https://api.telnyx.com/v2/text-to-speech/speech",
            headers={"Authorization": f"Bearer {KEY}"},
            json={"text": text, "voice": "Telnyx.Ultra.002622d8-19d0-4567-a16a-f99c7397c062", "audio_format": "mp3"},
            timeout=60,
        )
    r = await asyncio.to_thread(_gen)
    tmp = Path(tempfile.mkdtemp())
    mp3, raw = tmp / "s.mp3", tmp / "s.raw"
    mp3.write_bytes(r.content)
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", str(mp3), "-ar", "16000", "-ac", "1", "-f", "s16le", str(raw),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await proc.wait()
    return raw.read_bytes()


def norm(s: str) -> set:
    """كلمات جوهرية للمقارنة (بلا stop words وأرقام متغيرة)."""
    stop = {"the", "a", "an", "to", "of", "and", "in", "is", "are", "was", "were", "i", "my", "me", "we", "our",
            "that", "this", "it", "for", "on", "with", "at", "by", "from", "as", "be", "have", "has", "will"}
    words = [w.strip(",.!?;:").lower() for w in s.split()]
    return {w for w in words if w and w not in stop and len(w) > 2}


async def main():
    print(f"🎯 محاكاة مؤتمر GenAI ساعة كاملة (تسريع x{SPEED} → زمن اختبار ~{60/SPEED:.0f} دقيقة)")
    print("تجهيز الصوت...", flush=True)
    pcms = []
    for i, sec in enumerate(SECTIONS):
        pcm = await make_pcm(sec)
        pcms.append(pcm)
        print(f"  فقرة {i+1}/{len(SECTIONS)}: {len(pcm)/32000:.0f}s صوت — {len(sec.split())} كلمة", flush=True)

    # أزمنة المقارنة
    expected_words = set()
    for s in SECTIONS:
        expected_words |= norm(s)

    print("\n🔴 البث الحي يبدأ...\n", flush=True)
    results = {"source": [], "translation": [], "tts": [], "connects": 1, "errors": []}
    bytes_per_sec = 32000
    t_start = time.time()
    memory_samples = []

    uri = "ws://localhost:8000/ws/live?lang=en&tts_lang=ar"
    ws = await websockets.connect(uri, open_timeout=20, max_size=None)
    await asyncio.wait_for(ws.recv(), timeout=10)  # ready

    async def reader():
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=120)
                m = json.loads(raw)
                t = time.time() - t_start
                if m["type"] == "source":
                    results["source"].append((t, m["text"]))
                    print(f"  +{t*SPEED:6.0f}s 🎤 «{m['text'][:55]}»", flush=True)
                elif m["type"] == "translation":
                    results["translation"].append((t, m["text"]))
                    print(f"          🌍 «{m['text'][:55]}»", flush=True)
                elif m["type"] == "tts":
                    results["tts"].append(t)
        except asyncio.TimeoutError:
            results["errors"].append("reader timeout 120s")
        except Exception as e:
            results["errors"].append(f"reader: {type(e).__name__}: {str(e)[:80]}")

    async def sender():
        """يبث كل فقرة بسرعة SPEEDx realtime مع صمت 700ms بين الفقرات."""
        silence_700ms = b"\x00\x00" * int(16000 * 0.7)
        for i, pcm in enumerate(pcms):
            chunk = 16384
            for j in range(0, len(pcm), chunk):
                await ws.send(pcm[j:j + chunk])
                await asyncio.sleep((chunk / bytes_per_sec) / SPEED)
            if i < len(pcms) - 1:
                await ws.send(silence_700ms)
        # صمت ختامي أطول
        for _ in range(3):
            await ws.send(b"\x00\x00" * 16000)
            await asyncio.sleep(1 / SPEED)
        await asyncio.sleep(15)  # استقبال آخر النتائج
        try:
            await ws.close()
        except Exception:
            pass

    async def mem_sampler():
        while True:
            try:
                out = subprocess.run(
                    ["ps", "-o", "rss=", "-C", "python"],
                    capture_output=True, text=True)
                rss = sum(int(x) for x in out.stdout.split() if x.isdigit()) / 1024
                memory_samples.append(rss)
            except Exception:
                pass
            await asyncio.sleep(30)

    await asyncio.gather(
        sender(),
        reader(),
        mem_sampler(),
    )

    # ===== التقرير =====
    print("\n" + "=" * 62)
    print("📊 تقرير مؤتمر الساعة الكاملة")
    print("=" * 62)
    got_words = set()
    for _, t in results["source"]:
        got_words |= norm(t)
    recall = len(got_words & expected_words) / max(1, len(expected_words))
    print(f"الجمل الملتقطة: {len(results['source'])} | الترجمات: {len(results['translation'])} | أصوات: {len(results['tts'])}")
    print(f"استرجاع الكلمات: {recall:.0%} ({len(got_words & expected_words)}/{len(expected_words)} كلمة أساسية)")
    if results["source"] and results["translation"]:
        gaps = []
        si = 0
        for t_src, _ in results["source"]:
            while si < len(results["translation"]) and results["translation"][si][0] < t_src:
                si += 1
            if si < len(results["translation"]):
                gaps.append(results["translation"][si][0] - t_src)
        if gaps:
            gaps.sort()
            print(f"زمن النص→ترجمة: متوسط {sum(gaps)/len(gaps):.2f}s | p95 {gaps[int(len(gaps)*0.95)]:.2f}s")
    print(f"أخطاء: {results['errors'] if results['errors'] else 'لا شيء ✅'}")
    if memory_samples:
        print(f"ذاكرة الخادم: بداية {memory_samples[0]:.0f}MB → نهاية {memory_samples[-1]:.0f}MB (فرق {memory_samples[-1]-memory_samples[0]:+.0f}MB)")
    print(f"زمن الاختبار الفعلي: {time.time()-t_start:.0f}s (محاكاة {60:.0f} دقيقة كلام)")


asyncio.run(main())
