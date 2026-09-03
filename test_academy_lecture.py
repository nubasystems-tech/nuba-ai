"""محاكاة واقعية: طالب عربي في محاضرة AWS (الأكاديمية) يستخدم وضع البث الحي.
محاضرة 10 دقائق (مضغوطة 4x لتشغيل 2.5 دقيقة) عن Bedrock AgentCore — محتوى حقيقي من الأكاديمية.
نقيس: كل جملة يسمعها الطالب، متوسط زمن الوصول، المفقودات، والسلوك العام."""
import asyncio
import json
import struct
import time

import websockets

from test_live import make_pcm

# محتوى محاضرة حقيقية من أكاديمية AWS (Bedrock AgentCore + Strands SDK)
LECTURE = [
    "Good morning everyone, and welcome to the agentic AI workshop.",
    "Today we will build an agent using Amazon Bedrock AgentCore.",
    "AgentCore is a managed runtime for deploying AI agents.",
    "It provides built in memory, tools, identity, and observability.",
    "The first step is to install the Strands SDK using pip.",
    "Strands lets you define tools with simple Python functions.",
    "Each tool gets a doc string, and the model calls it automatically.",
    "Next we provision the runtime environment in the AWS console.",
    "You will need Python 3.12 or later for this workshop.",
    "Now let me show you how memory works across sessions.",
    "The agent stores session state in the managed memory table.",
    "This allows the agent to remember previous conversations.",
    "For example, if you told it your favorite color yesterday,",
    "the agent will recall that color in today's session.",
    "Next, we attach tools using the tool registry.",
    "Tools can call Lambda functions, query databases, or browse the web.",
    "Guardrails protect your agent from harmful content.",
    "You can configure guardrails in the Bedrock console.",
    "Finally, we deploy the agent and test it from the portal.",
    "The web UI connects to your agent through a websocket.",
    "Let us now switch to the demo and build this together.",
    "Remember, the region for this workshop is us west two.",
    "Any questions so far about the runtime or the tools?",
]

async def main():
    uri = "ws://localhost:8000/ws/live?lang=en&tts_lang=ar"
    print("🎯 محاكاة: طالب عربي في محاضرة AWS — وضع البث الحي")
    ws = await websockets.connect(uri, open_timeout=20)
    m = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
    print(f"الحالة: {m.get('message','ready')}")

    # نولّد صوت المحاضرة كاملاً مسبقاً (TTS مرة واحدة — أسرع بكثير)
    print("📡 يولّد صوت المحاضرة (22 جملة)...")
    t_gen = time.time()
    chunks = []
    for i, sent in enumerate(LECTURE):
        pcm = await asyncio.to_thread(make_pcm, sent)
        chunks.append(pcm)
        print(f"  [{i+1}/22] جاهز: {sent[:40]}...")
    print(f"✅ الصوت جاهز خلال {time.time()-t_gen:.0f}s\n")

    # نبدأ "المحاضرة" — نبث بسرعة 4x (كأن المحاضرة الحقيقية أطول)
    SPEED = 1
    t0 = time.time()
    received = []     # (زمن الوصول، الجملة الأصلية، الترجمة)
    translations = {}
    stats = {"src": 0, "tr": 0}

    async def player():
        for pcm in chunks:
            for i in range(0, len(pcm), 8000):
                await ws.send(pcm[i:i+8000])
                await asyncio.sleep(0.25 / SPEED)
            await asyncio.sleep(0.15)  # وقفة طبيعية بين الجمل
        # صمت ختامي
        await ws.send(struct.pack("<h", 0) * 32000)

    async def listener():
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=45)
            m = json.loads(raw)
            t = time.time() - t0
            if m["type"] == "source":
                stats["src"] += 1
                print(f"  +{t:6.1f}s 🎤 {m['text']}")
                received.append((t, m["text"]))
            elif m["type"] == "translation":
                stats["tr"] += 1
                translations[len(translations)] = (t, m["text"])
                print(f"  +{t:6.1f}s 🌍 {m['text']}")

    play_task = asyncio.create_task(player())
    listen_task = asyncio.create_task(listener())
    try:
        await play_task
        await asyncio.sleep(12)  # نجمع آخر النتائج
    finally:
        listen_task.cancel()
        try:
            await ws.close()
        except Exception:
            pass

    # التقرير
    print("\n" + "=" * 60)
    print("📊 تقرير الطالب: ماذا حدث في التطبيق خلال المحاضرة؟")
    print("=" * 60)
    print(f"جمل المحاضرة: 22 | جمل التقطها التطبيق: {stats['src']} | ترجمات وصلت: {stats['tr']}")
    if received:
        gaps = []
        prev_end = 0
        for t, txt in received:
            gaps.append(t - prev_end)
            prev_end = t
        print(f"أول نتيجة: +{received[0][0]:.1f}s | متوسط الفاصل بين الجمل: {sum(gaps)/len(gaps):.1f}s")
    # مطابقة المحتوى: كم جملة من 22 وصلت نصاً سليماً
    matched = 0
    for t, txt in received:
        low = txt.lower()
        for orig in LECTURE:
            o = orig.lower()[:30]
            if o[:20] in low:
                matched += 1
                break
    print(f"مطابقة المحتوى: {matched}/22 جملة موجودة في نص المحاضرة")

asyncio.run(main())
