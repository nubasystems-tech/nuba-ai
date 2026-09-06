"""وضع القاعة متعددة اللغات (Multi-Lang Live) — نقاش بحث بلغات مختلطة.
المعمارية: fan-out — نفس PCM يُبث بالتوازي إلى محرك لكل لغة مرشحة:
  العربية → Cohere | الإنجليزية → nova-3 en | الفرنسية → nova-3 fr | الصينية → nova-3 zh
المحرك الذي "يفهم" الجملة يعيدها بثقة عالية → نعرف لغة المتحدث تلقائياً →
نترجم للغة المستمع وننطق بصوته المختار. الترجمة تتبدل تلقائياً مع كل متحدث.

ملاحظة قياس: nova-3 monolingual لا يبدل لغته داخل اتصال واحد (تحقق مباشر) —
إعادة البث لعدة محركات هي الحل الصحيح والمستخدم في منتجات الترجمة الفعلية.
"""
import asyncio
import metrics
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json
import time
from typing import Optional

import websockets
import stt_bridge
from fastapi import WebSocket, WebSocketDisconnect

from telnyx_primitives import translate_sync, tts_sync

import sys
sys.path.insert(0, "/home/ubuntu/translator-app")
import os
TELNYX_KEY = os.environ.get("TELNYX_STT_API_KEY", "")

# 🔧 تنفيذ مخصص محدود (سقف 6 خيوط) لـ translate_sync/tts_sync — نفس منطق
# live_mode.py: to_thread الافتراضي يشارك مسبح خيوط عام قد يزاحمه ضغط آخر.
_EXECUTOR = ThreadPoolExecutor(max_workers=6, thread_name_prefix="multi-xlate")

# 🔧 مراجعة Claude (تدقيق تأخير): القيمة الفعلية كانت 8000 عينة int16 =
# 500ms عند 16kHz (b"\x00\x00" يتكرر 8000 مرة = 16000 بايت = 8000 عينة)،
# لا 1 ثانية كما يوحي الاسم — نصححها لتطابق الاسم وتزيد الهامش فوق endpointing=300ms.
SILENCE_1S = b"\x00\x00" * 16000    # 🔧 Claude #13: 1 ثانية صمت حقيقية 16k mono (16000 عينة)
SILENCE_250MS = b"\x00\x00" * 2000  # 125ms (تاريخياً — لا يعتمد عليه نبض duo)

# محرك لكل لغة مرشحة (القاعة): عربي/إنجليزي/فرنسي/صيني/تركي
FAN_LANGS = ["ar", "en", "fr", "zh-CN", "tr"]


def _engine_for(lang: str):
    """توجيه مثبت بالقياس الميداني 2026-09-03 (16:11 → 16:36):
    - ar → Cohere: التقاط 100% وسريع (conf دائماً 0.00 — الوزن المحايد في التحكيم).
    - en → Cohere: قياس 16:11/16:14: Cohere-en التقط الجملة الإنجليزية المتدهورة
      نصاً صحيحاً وبعد الصدى العربي بـ0.13s فقط (نفس نافذة التحكيم → قاعدة
      الكرسي الأجنبي تحذف الصدى). بينما nova-3-en داخل uvicorn صامت كلياً
      اليوم (ولا حتى interims — خلافاً لخارجه حيث يعمل conf=0.998).
    - fr → nova-3: قياس 16:25: داخل uvicorn وصل conf=1.00 لكن متأخراً ~9s
      (الحد المعماري المعروف — يُتوَّج في نافذة لاحقة فيصحح الجملة).
      Cohere-fr صامت على الفرنسية المتدهورة (16:11/16:14: لم يرسل شيئاً).
    - zh/tr → nova-3: مرشحان إضافيان؛ هلوساتهما يصفّيها الفلتر اللغوي.
    مراجعة يومية: إن عاد nova-3-en حياً داخل uvicorn (اختبر بسجل خام) يمكن
    إعادة en→nova-3 للاستفادة من إشارة الثقة الحقيقية."""
    if lang in ("ar", "en"):
        return "Cohere", ""
    return "Deepgram", "&model=deepgram/nova-3"


def _upstream_url(lang: str, force_engine: str = None):
    engine, extra = (_engine_for(lang) if not force_engine else (force_engine, ""))
    ep = 300  # جمل ناضجة للجميع: الشظايا القصيرة (ep=100) كانت تخسر أمام جملة المحرك الآخر الكاملة
    return (f"wss://api.telnyx.com/v2/speech-to-text/transcription?"
            f"transcription_engine={engine}&input_format=linear16&sample_rate=16000"
            f"&language={lang}{extra}&endpointing={ep}")


def _agc(pcm: bytes, state: list) -> bytes:
    """المعالج الطيفي الفائق — نفس live_mode._agc_amplify (numpy):
    highpass 300Hz + lowpass + قمع ضجيج طيفي + كشف تذبذب/hysteresis + AGC حتى x250.
    (كان x25 قديماً: العربية/الفرنسية بصوت 10% فشلت في دورة المؤتمر 2026-09-03
    بينما نفس الصوت عبر live_mode التُقط كاملاً — الفرق هو هذا المعالج.)"""
    from live_mode import _agc_amplify
    return _agc_amplify(pcm, state)


def detect_text_lang(text: str) -> str:
    """كشف لغة سريع من النص (لعرضها بجانب الجملة)."""
    if any("\u0600" <= c <= "\u06FF" for c in text):
        return "ar"
    if any("\u4E00" <= c <= "\u9FFF" for c in text):
        return "zh-CN"
    return "en"


async def _open_upstream(lang: str, retries: int = 3, force_engine: str = None):
    """فتح اتصال STT للغة مع إعادة المحاولة. force_engine: تجاوز المحرك
    (fallback الجلسة إلى Cohere عندما يصمت nova-3 من جهة Telnyx)."""
    headers = {"Authorization": f"Bearer {TELNYX_KEY}"}
    last = None
    for a in range(retries):
        try:
            return await websockets.connect(
                _upstream_url(lang, force_engine), additional_headers=headers, open_timeout=15)
        except Exception as e:
            last = e
            await asyncio.sleep(1 + a)
    print(f"[multi] فشل فتح {lang}: {last}", flush=True)
    return None


async def duo_lang_worker(user_ws: WebSocket, lang_a: str, lang_b: str, tts_on: bool = True):
    metrics.inc("ws_sessions_total")
    metrics.inc("ws_active")
    # 🛡️ حارس المجموعة الفارغة (سجل 2026-09-03 17:0x: هاتف المستخدم أرسل ar↔ar
    # خمس مرات = محركان عربيان مكرران = صفر ترجمة فعلية!): لو اللغتان متطابقتان
    # نستبدل الثانية بالإنجليزية تلقائياً (المستخدم عربي — التبادل المنطقي)
    if lang_a == lang_b:
        lang_b = "en" if lang_a == "ar" else "ar"
        print(f"[duo] ⚠️ لغتان متطابقتان — بدلنا الثانية إلى {lang_b}", flush=True)
    """وضع الحوار التبادلي: لغتان فقط — يكشف المتحدث تلقائياً ويترجم دائماً
    إلى اللغة الأخرى (بالعكس). عربي يتكلم → نص+صوت إنجليزي؛ إنجليزي يتكلم → نص+صوت عربي."""
    # 🧪 ترتيب تشخيصي: اللغة الأجنبية أولاً (Cohere ar الثاني كان يلتقط كل شيء
    # بينما الأجنبي يصمت — نختبر هل للترتيب أثر)
    pair = [lang_b, lang_a]
    upstreams = {}
    # 🔧 فتح متوازٍ (قياس 14:48): الفتح التتابعي كان يبطئ fr عن أول صوت
    # (المحاكاة المتزامنة عملت بينما التتابع صامت) — نفتح الاثنين معاً
    _up_born = {}
    opened = await asyncio.gather(*[_open_upstream(l) for l in pair],
                                 return_exceptions=True)
    for lang, ws_up in zip(pair, opened):
        if ws_up and not isinstance(ws_up, Exception):
            upstreams[lang] = ws_up
            _up_born[lang] = time.time()
    if len(upstreams) < 2:
        await user_ws.send_json({"type": "error", "message": "تعذر فتح محركي اللغتين"})
        return
    other = {lang_a: lang_b, lang_b: lang_a}
    print(f"[duo] وضع تبادلي {lang_a}↔{lang_b} — محركان جاهزان", flush=True)
    # 🩺 حارس المحركات: إن بقي محرك أبكم (ولا رسالة خام واحدة) أثناء بث
    # نشط فعلي، نعيد فتحه + collector جديد.
    # 🎯 قياس 16:12 (دورة بسجل): الحارس القديم (8s من أول chunk) قتل محرك ar
    # السليم أثناء صمت ما بعد الجملة 1 — الجملة استُكمل بثها عند 3.6s ونتيجتها
    # تحتاج endpointing، فقتلها الحارس → فقدت كلياً. الشرط الجديد: لا إعادة فتح
    # إلا إذا كان البث نشطاً (chunk وصل خلال آخر 3s) — الصمت بين الجمل طبيعي.
    engine_got = {}          # lang → آخر لحظة استلام خام
    last_chunk = [0.0]        # آخر لحظة وصل فيها chunk من المستخدم
    _guard_started = [False]
    _reborn = {}             # lang → عدد محاولات الحارس

    async def _engine_guard():
        await asyncio.sleep(10)
        while not finished:
            await asyncio.sleep(2)
            now = time.time()
            active = now - last_chunk[0] < 3.0
            if not active:
                continue
            for l in list(upstreams.keys()):
                # 🔧 Claude #9: عضوية "not in" كان يعمى الحارس بعد أول استلام
                # — محرك يتكلم مرة ثم يصمت ساعة لا يُشفى أبداً. فحص زمني:
                last_got = engine_got.get(l, 0)
                if now - last_got > 15 if last_got else True:
                    # (لم يستلم شيئاً أبداً، أو صمت >15s أثناء بث نشط)
                    # 🎯 صبر عمر الاتصال (قياس 16:25 و16:39): لا قتل قبل 20s —
                    # nova-3-fr يرسل نتيجته ~9s بعد نهاية الكلام داخل uvicorn.
                    born = _up_born.get(l, now)
                    if now - born < 20.0:
                        continue
                    # fallback الجلسة: nova-3 صامت من جهة Telnyx — بعد أول
                    # إعادة فتح فاشلة نحوّل اللغة إلى Cohere نهائياً للجلسة.
                    force = "Cohere" if _reborn.get(l, 0) >= 1 else None
                    _reborn[l] = _reborn.get(l, 0) + 1
                    print(f"[duo] {l}: أبكم رغم بث نشط (محاولة {_reborn[l]}) "
                          f"{'→ fallback Cohere' if force else '— نعيد الفتح'}", flush=True)
                    try:
                        await upstreams[l].close()
                    except Exception:
                        pass
                    new_up = await _open_upstream(l, force_engine=force)
                    if new_up:
                        upstreams[l] = new_up
                        _up_born[l] = time.time()
                        asyncio.create_task(collector(l, new_up))
                    break   # واحد كل جولة (لا نغرق الافتتاحات)
    await user_ws.send_json({"type": "ready",
                             "message": f"الحوار التبادلي جاهز — تكلم بأي لغة ({lang_a}/{lang_b})",
                             "langs": pair})

    agc_state = [18.0, True, 0]
    finished = False
    PENDING = {}
    SENT_AT = {}
    _recent_crowned = []   # (norm_text, t) للجمل المُتوَّجة حديثاً — لقاعدة الذيل اليتيم

    def text_key(text: str) -> str:
        return "".join(c.lower() for c in text if c.isalnum())[:40]

    def _is_orphan_tail(text: str, now: float) -> bool:
        """🎯 قياس 12:27 + Claude #8 backport 12:36: ذيل nova اليتيم يُبث كجملة
        مستقلة، واستمرار lowercase يسرق نافذة الجملة التالية. حجب: ≤4 كلمات
        محتوى في جملة مُتوَّجة خلال 12s، أو يبدأ lowercase (استمرار ذيل)."""
        _norm = lambda s: " ".join(s.lower().split())
        tn = _norm(text)
        if len(tn.split()) > 4:
            return False
        for prev, t in _recent_crowned[-8:]:
            if now - t > 12:
                continue
            if tn in prev or prev in tn:
                return True
            if text[:1].islower():
                return True
        return False

    async def pump():
        nonlocal finished
        try:
            while True:
                pcm = await user_ws.receive_bytes()
                _last_audio[0] = time.time()
                if not _guard_started[0]:
                    _guard_started[0] = True
                    asyncio.create_task(_engine_guard())
                last_chunk[0] = time.time()
                if len(pcm) > 65536:   # مهارة security: سقف حجم (chunk طبيعي ≤8KB)
                    continue
                pcm = await asyncio.to_thread(_agc, pcm, agc_state)
                # 🎯 إصلاح جذري (تشخيص 15:05): replay نفس chunks خارج uvicorn يعمل!
                # الصوت سليم — القاتل مسار gather+_to المعقد. نمط live المثبت:
                # إرسال مباشر متتالٍ، وأي فشل = إعادة فتح + collector جديد فوراً
                for lang, up in list(upstreams.items()):
                    try:
                        await up.send(pcm)
                    except Exception as e:
                        print(f"[duo] {lang}: فشل إرسال {type(e).__name__} — نعيد الفتح+collector", flush=True)
                        try:
                            await up.close()
                        except Exception:
                            pass
                        new_up = await _open_upstream(lang)
                        if new_up:
                            upstreams[lang] = new_up
                            asyncio.create_task(collector(lang, new_up))
        except Exception as e:
            print(f"[duo] pump انتهى: {type(e).__name__}", flush=True)
        finally:
            finished = True

    async def _deliver(lang, text, conf, now):
        # 🎯 قياس 12:24: صدى Cohere ar لجملة en وصل بعد الشظية الصحيحة بـ 5.4s
        # فافتتح نافذة مستقلة وتُوِّج كعربية معكوسة الاتجاه → نوسّع التجميع إلى 5.5s.
        if _is_orphan_tail(text, now):
            print(f"[duo] ذيل يتيم محجوب [{lang}]: {text[:40]}", flush=True)
            return
        gid = None
        for g, (t0_g, items) in PENDING.items():
            if (now - t0_g) < 5.5:
                gid = g
                break
        if gid is None:
            gid = f"g{int(now*1000)}"
            PENDING[gid] = (now, [])
        PENDING[gid][1].append((lang, text, conf))
        _ts = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int((now % 1) * 1000):03d}"
        print(f"[duo] متنافس {_ts} [{lang}] conf={conf:.2f}: {text[:48]}", flush=True)
        if len(PENDING[gid][1]) == 1:
            asyncio.create_task(_crown(gid))

    async def _crown(gid: str):
        # ⚡ توازن السرعة ضد الصدى (قياس ميداني 2026-09-03):
        # 6.1s كانت بطيئة جداً للمحادثة الحية (شكوى المستخدم: بطي جدا)
        # 2.8s تلتقط جملة المحركين + أغلب الصدى المتأخر، والترجمة تصلك فور التتويج
        # قياس 14:20: nova-3 fr يرسل نتيجته أبطأ من صدى Cohere ar بثوانٍ
        # 2.8s فازت بالهلوسة قبل وصول الجملة الفرنسية الصحيحة → 4.5s
        # ⚡ تتويج تكيّفي (قياس المحاضرة: median 2.75s لكن p95 5.99s):
        # الجملة الواضحة (متنافس وحيد) لا تحتاج انتظار صدى — 0.35s تكفي.
        await asyncio.sleep(0.35 if len(PENDING.get(gid, (0, []))[1]) == 1 else 0.6)
        if gid not in PENDING:
            return
        _, items = PENDING.pop(gid)
        if not items:
            return
        # 🛡️ فلتر لغوي صارم للتبادل: جملة إنجليزية لا تصدر عبر محرك العربية والعكس
        # (قياس اليوم: Cohere التقط جملة إنجليزية بثقة أعلى → انعكس الاتجاه خطأً)
        _ar_chars = lambda t: sum(1 for c in t if "\u0600" <= c <= "\u06FF")
        _FR_WORDS = {"nous", "vous", "ils", "elle", "sont", "dans", "pour", "avec",
                     "mais", "être", "peut", "tout", "cette", "leur", "sans", "sous",
                     "entre", "vers", "aussi", "déjà", "alors", "donc", "chez", "où"}
        def _lang_of(text):
            letters = [c for c in text if c.isalpha()]
            if not letters:
                return "en"
            a = _ar_chars(text)
            if a >= len(letters) * 0.4:   # عربي حقيقي: أغلب الحروف عربية
                return "ar"
            if any("\u4E00" <= c <= "\u9FFF" for c in text):
                return "zh-CN"
            words = {w.strip(".,!?;:").lower() for w in text.split()}
            if sum(1 for w in words if w in _FR_WORDS) >= 2:
                return "fr"
            return "en"
        # 🎯 عتبة ثقة انتقائية: هلوسات كل المحركات تأتي بثقة منخفضة (zh=0.07, tr مثال 0.74)
        # لكن Cohere ar يرسل conf=0.00 حتى لنصوصه الصحيحة (قياس دورتين) — نستثنيه
        # 🛡️ كاشف الهجين (قياس 14:20 و14:25): هلوسة Cohere ar للكلام الفرنسي
        # تخرج نصاً عربياً-لاتينياً ممزوجاً «بونجور مون امي» — كلام فرنسي عبر محرك خاطئ.
        # الجملة العربية الحقيقية لا تحوي كلمات لاتينية متعددة → نحجب الهجينة و
        # ننتظر نتيجة المحرك الصحيح (nova-3 fr) خلفها.
        def _latin_words(t):
            return sum(1 for w in t.split() if any(c.isascii() and c.isalpha() for c in w) and any(c.isascii() and c.isalpha() for c in w))
        valid = []
        for (l, t, c) in items:
            if _lang_of(t) != l:
                continue
            if l == "ar" and lang_b != "ar" and _latin_words(t) >= 2 and len(t.split()) <= 8:
                # نص قصير هجين عربي+لاتيني عبر محرك العربية = هلوسة كلام أجنبي
                continue
            valid.append((l, t, c))
        if not valid:
            # كل المتنافسين بلغة خاطئة (ضجيج/هلوسة) — نتجاهل النافذة كلياً
            return
        # 🎯 قاعدة الكرسي الأجنبي (قياس 16:12-16:39): صدى Cohere ar للكلام
        # الأجنبي نص عربي صافٍ (يجتاز الفلتر) لكنه إعادة صياغة عربية للمعنى.
        # الدليل على كلام أجنبي: مرشح أجنبي صالح في النافذة، أو نص لاتيني نقي
        # (≥3 كلمات، صفر حرف عربي) عبر محرك en/fr حتى لو رفضه الفلتر الصارم —
        # قياس 16:39: [en] التقط الفرنسية «Nous devons...» لكن _lang_of=fr أسقطه
        # → بقي الصدى «ندوغو انفستير...» وحيداً وتُوِّج بالعربية (اتجاه معكوس).
        _latin_pure = lambda t: (not any("\u0600" <= c <= "\u06FF" for c in t)) and len(t.split()) >= 3
        # 🛡️ إصلاح هلوسة ميدانية (لقطة طارق 12:04): كلام عربي حقيقي عبر ميكروفون
        # هاتف → صدى لاتيني هلوسي عبر en ("Stay with us", "Thank you" لم تُقل).
        # العربي الصحيح ≥5 كلمات لا يُحذف أبداً؛ اللاتيني يجب أن يكون جملة
        # كاملة ≥6 كلمات ليحسب دليلاً على كلام أجنبي حقيقي.
        ar_items = [it for it in valid if it[0] == "ar"]
        ar_long = any(len(it[1].split()) >= 5 for it in ar_items)
        foreign_long = any(l in ("en", "fr") and _latin_pure(t) and len(t.split()) >= 6
                           for (l, t, c) in items)
        foreign_proof = (any(it[0] != "ar" for it in valid) and not ar_long) or foreign_long
        if foreign_proof:
            valid = [it for it in valid if it[0] != "ar"]
        if not valid:
            return
        # 🎯 دمج شظايا نفس اللغة (قياس 16:39): Cohere قسّم الجملة العربية إلى
        # شظيتين في نفس النافذة («الذكاء الاصطناعي» + «يغير شكل المستقبل...»)
        # فتُوِّجت الأطول فقط وسقط المبتدأ — نحذف المتداخلين وندمج الباقي.
        by_lang = {}
        for it in valid:
            by_lang.setdefault(it[0], []).append(it)
        merged = []
        for l, cands in by_lang.items():
            keep = []
            for (l2, t2, c2) in cands:
                k2 = text_key(t2)
                dup = False
                for i, (l3, t3, c3) in enumerate(keep):
                    k3 = text_key(t3)
                    if k2 and (k2 in k3 or k3 in k2):
                        if len(t2) > len(t3):
                            keep[i] = (l2, t2, max(c2, c3))
                        dup = True
                        break
                if not dup:
                    keep.append((l2, t2, c2))
            if len(keep) > 1:
                keep = [(l, " ".join(t for (_, t, _) in keep),
                         max(c for (_, _, c) in keep))]
            merged.extend(keep)
        valid = merged
        def _fair(it):
            lang, text, conf = it
            detected = _lang_of(text)
            bonus = 0.5 if detected == lang else 0.0
            # 🎯 قياس 16:12 (duo بسجل): الجملة الإنجليزية عبر Cohere en وصلت
            # نصاً صحيحاً لكن conf=0.00 → وزن 0.0، فخسرت أمام صدى Cohere ar
            # (وزن 0.85 المحايد) وتُوِّج الاتجاه معكوساً. Cohere كله لا يرسل ثقة
            # حقيقية (دائماً 0.00/None) → الوزن المحايد 0.85 لأي conf==0.0،
            # ولأي لغة — وعندئذ يحسم bonus مطابقة اللغة (نص عبر محرك لغته).
            trust = 0.85 if conf == 0.0 else min(conf, 0.99)
            return (trust + bonus, len(text))
        # 🛡️ (سجل 06:07): «Suppose» — صدى صوت عربي سمعه محرك en ككلمة لاتينية
        # واحدة وترجمها «شكراً لك» = الهلوسة المرئية. متنافس لاتيني كلمة-واحدة
        # مع متنافس عربي بنفس النافذة = صدى مؤكد → حجب ويتوج العربي
        # 🎯 توسيع: الصدى القصير قد يصل كلمتين ("Merci beaucoup") أو بحروف
        # لاتينية مُشكّلة (accented — "café", "être") لا يجتازها isascii() —
        # الشرط الحاسم يبقى وجود منافس عربي في نفس النافذة (صدى مؤكد) واستبعاد
        # أي نص عربي فعلي (حروف عربية) من الحجب
        if len(valid) >= 2 and any(l == "ar" for l, t, c in valid):
            for echo in [(l, t, c) for l, t, c in valid
                         if l != "ar" and len(t.split()) <= 2
                         and not any("؀" <= ch <= "ۿ" for ch in t)]:
                valid.remove(echo)
                print(f"[duo] حجب صدى قصير: [{echo[0]}] '{echo[1][:20]}'", flush=True)
        best = max(valid, key=_fair)
        lang, text, conf = best
        # 🎯 فحص التكرار على الفائز الفعلي بعد كل الدمج/الفلترة — ليس على أول
        # مرشح خام في النافذة (items[0]) الذي قد لا يكون حتى الفائز
        now = time.time()
        key0 = text_key(text)[:25]
        for k, t in SENT_AT.items():
            if now - t < 12 and k[:25] == key0:
                return
        # 🔧 Claude #11: تفريغ القديم (تسريب بطيء عبر ساعات المؤتمر)
        for k in [k for k, t in SENT_AT.items() if now - t > 60]:
            SENT_AT.pop(k, None)
        SENT_AT[text_key(text)] = now
        _recent_crowned.append((" ".join(text.lower().split()), now))
        del _recent_crowned[:-32]
        while _recent_crowned and now - _recent_crowned[0][1] > 12:
            _recent_crowned.pop(0)
        target = other.get(lang, lang_b)
        metrics.inc("sentences_total")
        await user_ws.send_json({"type": "source", "text": text, "speaker_lang": lang,
                                 "target_lang": target})
        try:
            try:
                loop = asyncio.get_running_loop()
                translated = await asyncio.wait_for(
                    loop.run_in_executor(_EXECUTOR, translate_sync, text, target), timeout=8)
            except asyncio.TimeoutError:
                translated = None
        except Exception:
            translated = None
        if translated:
            await user_ws.send_json({"type": "translation", "text": translated,
                                     "speaker_lang": lang, "target_lang": target})
            if tts_on:
                async def _speak(txt=translated, tl=target):
                    url = await _tts_audio(txt, tl)
                    if url:
                        try:
                            await user_ws.send_json({"type": "tts", "url": url, "lang": tl})
                        except Exception:
                            pass
                asyncio.create_task(_speak())

    async def collector(lang: str, up):
        nonlocal finished
        while not finished:
            try:
                raw = await asyncio.wait_for(up.recv(), timeout=75)
                if isinstance(raw, bytes):
                    continue
                d = json.loads(raw)
                engine_got[lang] = time.time()
                print(f"[duo] [{lang}] خام: {str(d)[:90]}", flush=True)
                if "errors" in d:
                    continue
                if d.get("is_final") and d.get("transcript"):
                    text = str(d["transcript"]).strip()
                    if not text:
                        continue
                    conf = float(d.get("confidence", 0) or 0)
                    alnum_count = sum(1 for c in text if c.isalnum())
                    # 🎯 (سجل المستخدم 23:06): «Hi.» حُجبت كترحيب حقيقي!
                    # نسمح: كلمة+ واضحة الحروف. نحجب: الضجيج الرمزي القصير
                    if conf < 0.65 and alnum_count < 2:
                        continue
                    now = time.time()
                    await _deliver(lang, text, conf, now)
            except asyncio.TimeoutError:
                if finished:
                    return
            except Exception:
                if finished:
                    return
                try:
                    await up.close()
                except Exception:
                    pass
                new_up = await _open_upstream(lang)
                if new_up:
                    upstreams[lang] = new_up
                    # 🔧 مراجعة Claude #7: الاستدعاء الذاتي يبني stack لا يُفك
                    # (RecursionError بعد عدة انقطاعات) — مهمة جديدة بدلاً منه
                    asyncio.create_task(collector(lang, new_up))
                return
    _last_audio = [time.time()]   # يحدّثه pump مع كل chunk صوتي

    async def _release_pulse():
        """🎯 كاشف السرعة (نفس live المثبت): is_final لا يصل إلا مع صوت/صمت
        جديد بعده — نبض صامت كل 350ms عند خمول حقيقي يحرر النتائج فوراً.
        🔧 مراجعة Claude (تدقيق تأخير): المتصفح يرسل chunk كل 256ms بلا توقف
        (live.html) سواء تحدث المستخدم أو صمت — _last_audio[0] كان يتحدث مع
        كل chunk فيبقى "نشاطاً" دائماً ولا يهدأ 0.25s أبداً، فالنبض لا يُطلق
        عملياً. نقرأ بدل ذلك agc_state[6] (لحظة آخر كلام حقيقي اكتشفه AGC)."""
        _n_pulse = 0
        _last_pulse_t = 0.0
        while not finished:
            await asyncio.sleep(0.4)
            last_speech = agc_state[6] if len(agc_state) > 6 and agc_state[6] else 0.0
            idle = time.time() - last_speech
            # 🎯 (سجل 06:07): النبض المتكرر الأبدي جعل المحرك يعيد is_final
            # نفس الجملة كل نبضة = هلوسة تكرار! سلسلة قصيرة: نبضات فقط 3 ثوانٍ
            # بعد آخر كلام (تحرر النتيجة مرة) ثم صمت تام حتى كلام جديد
            if 0.25 < idle < 3.0:
                for _l, _u in list(upstreams.items()):
                    try:
                        await _u.send(SILENCE_1S)
                        _n_pulse += 1
                        if _n_pulse == 1:
                            print(f"[duo] 🫀 نبض التحرير يعمل (محرك {_l})", flush=True)
                    except Exception as _pe:
                        print(f"[duo] نبض فشل لمحرك {_l}: {type(_pe).__name__}", flush=True)


    collectors = [asyncio.create_task(collector(l, u)) for l, u in list(upstreams.items())]
    asyncio.create_task(_release_pulse())
    try:
        await pump()
    finally:
        finished = True
        metrics.dec("ws_active")
        for c_task in collectors:
            c_task.cancel()
        for up in upstreams.values():
            try:
                await up.close()
            except Exception:
                pass


async def multi_lang_worker(user_ws: WebSocket, tts_lang: str, fan_langs=None):
    """نقاش متعدد اللغات: بث fan-out + التقاط من أي محرك + ترجمة تلقائية."""
    fan = fan_langs or FAN_LANGS
    upstreams = {}
    _up_born = {}
    for lang in fan:
        ws_up = await _open_upstream(lang)
        if ws_up:
            upstreams[lang] = ws_up
            _up_born[lang] = time.time()
    print(f"[multi] فتحنا {len(upstreams)}/{len(fan)} اتصالات: {list(upstreams)}", flush=True)
    await user_ws.send_json({"type": "ready",
                             "message": f"القاعة متعددة اللغات جاهزة — {len(upstreams)} مستمعون نشطون",
                             "langs": list(upstreams)})

    agc_state = [18.0, True, 0]
    finished = False
    RECENT = {}
    _recent_crowned = []   # (norm_text, t) — لقاعدة الذيل اليتيم (نفس duo)
    # 🔧 مراجعة Claude (تدقيق تأخير) CRITICAL: pump() أدناه يكتب
    # _last_audio[0] لكن لا شيء في هذا الوضع كان يعرّفها — كل chunk صوتي في
    # وضع القاعة كان يرفع NameError فوراً ويقتل الجلسة عند أول صوت. تعريفها
    # هنا أيضاً يمكّن نبض تحرير مثل duo (كان غائباً كلياً في هذا الوضع).
    _last_audio = [time.time()]
    # 🩺 حارس محركات القاعة (نفس منطق duo بعد إصلاح 16:12): محرك أبكم رغم
    # بث نشط → إعادة فتح + collector. الصمت بين الجمل لا يستدعي القتل.
    engine_got = {}
    last_chunk = [0.0]
    _guard_started = [False]
    _reborn = {}

    async def _engine_guard():
        await asyncio.sleep(10)
        while not finished:
            await asyncio.sleep(2)
            now = time.time()
            if now - last_chunk[0] >= 3.0:
                continue
            for l in list(upstreams.keys()):
                # 🔧 Claude #9: عضوية "not in" كان يعمى الحارس بعد أول استلام
                # — محرك يتكلم مرة ثم يصمت ساعة لا يُشفى أبداً. فحص زمني:
                last_got = engine_got.get(l, 0)
                if now - last_got > 15 if last_got else True:
                    # (لم يستلم شيئاً أبداً، أو صمت >15s أثناء بث نشط)
                    # 🎯 صبر عمر الاتصال (قياس 16:25 و16:39 — نفس duo): لا قتل
                    # قبل 20s من عمر الاتصال؛ nova-3-fr داخل uvicorn أرسل
                    # نتيجته ~9s بعد نهاية الكلام (بث 7s + endpointing → ~16s
                    # عمر الاتصال). 14s قتلته قبل وصولها في دورة 16:39.
                    born = _up_born.get(l, now)
                    if now - born < 20.0:
                        continue
                    force = "Cohere" if _reborn.get(l, 0) >= 1 else None
                    _reborn[l] = _reborn.get(l, 0) + 1
                    print(f"[multi] {l}: أبكم رغم بث نشط (محاولة {_reborn[l]}) "
                          f"{'→ fallback Cohere' if force else '— نعيد الفتح'}", flush=True)
                    try:
                        await upstreams[l].close()
                    except Exception:
                        pass
                    new_up = await _open_upstream(l, force_engine=force)
                    if new_up:
                        upstreams[l] = new_up
                        _up_born[l] = time.time()
                        asyncio.create_task(collector(l, new_up))
                    break

    def text_key(text: str) -> str:
        return "".join(c.lower() for c in text if c.isalnum())[:40]

    def _is_orphan_tail(text: str, now: float) -> bool:
        """🎯 قياس 12:36 (multi): (1) ذيل nova "today." بُث كجملة مستقلة؛
        (2) استمرار lowercase "in the world today." وصل بعد 2.8s من نهاية نافذة
        جملته فسرق نافذة الجملة العربية التالية. حجب أي متنافس ≤4 كلمات محتوى
        في جملة مُوِّجت خلال آخر 12s، أو يبدأ بحرف صغير (استمرار) خلال 12s."""
        _norm = lambda s: " ".join(s.lower().split())
        tn = _norm(text)
        if len(tn.split()) > 4:
            return False
        for prev, t in _recent_crowned[-8:]:
            if now - t > 12:
                continue
            if tn in prev or prev in tn:
                return True
            # استمرار واضح: يبدأ lowercase = ذيل جملة سابقة مُتوَّجة
            # (قياس 12:38: "today." كلمة واحدة فلتت من قاعدة ≥2 كلمات)
            if text[:1].islower():
                return True
        return False

    async def pump():
        """من المتصفح → AGC → كل الاتصالات بالتوازي (parallel send — لا تسلسل يعرقل التدفقات)."""
        nonlocal finished
        try:
            while True:
                pcm = await user_ws.receive_bytes()
                _last_audio[0] = time.time()
                if not _guard_started[0]:
                    _guard_started[0] = True
                    asyncio.create_task(_engine_guard())
                last_chunk[0] = time.time()
                if len(pcm) > 65536:   # مهارة security: سقف حجم (chunk طبيعي ≤8KB)
                    continue
                pcm = await asyncio.to_thread(_agc, pcm, agc_state)

                async def _to(lang, up):
                    if up.state.name == "OPEN":
                        try:
                            await up.send(pcm)
                            return True
                        except Exception:
                            pass
                    return False

                results = await asyncio.gather(
                    *[_to(l, u) for l, u in list(upstreams.items())], return_exceptions=True)
                # إعادة فتح الاتصالات التي فشل إرسالها (بالتوازي أيضاً)
                dead = [(l, results[i] is False) for i, (l, _) in enumerate(upstreams.items())]
                for lang, failed in dead:
                    if failed and lang in upstreams:
                        try:
                            await upstreams[lang].close()
                        except Exception:
                            pass
                        new_up = await _open_upstream(lang)
                        if new_up:
                            upstreams[lang] = new_up
                            # 🔧 مراجعة Claude (تدقيق تأخير): كان يُستبدل upstream
                            # بلا collector جديد — المحرك المعاد فتحه يبقى أبكم
                            # (لا أحد يقرأ نتائجه) حتى ينقذه حارس المحركات بعد
                            # 20s+ — نفس إصلاح duo لمسار collector نفسه.
                            collectors.append(asyncio.create_task(collector(lang, new_up)))
                        else:
                            upstreams.pop(lang, None)
        except (WebSocketDisconnect, Exception) as e:
            print(f"[multi] pump انتهى: {type(e).__name__}", flush=True)
            finished = True

    # 🏆 قاعة التحكيم: المتنافسون على نفس الجملة خلال نافذة 600ms
    PENDING = {}      # group_id -> [(lang, text, conf), ...]
    SENT_AT = {}      # آخر لحظة أرسلنا فيها جملة (منع تصادم النوافذ)

    async def _adjudicate(lang: str, text: str, conf: float, now: float):
        """كل محرك يقدم مرشحه؛ ننتظر 600ms ثم نتوج الأفضل: أعلى ثقة ثم أطول نص."""
        # تجميع: الجمل المتقاربة زمنياً = نفس النافذة
        if _is_orphan_tail(text, now):
            print(f"[multi] ذيل يتيم محجوب [{lang}]: {text[:40]}", flush=True)
            return
        gid = None
        for g, (t0_g, items) in PENDING.items():
            # 🎯 قياس 12:27→12:40 (4 دورات): فجوة وصول المرشح الصحيح بعد هلوسة ar
            # تتذبذب 4.1–5.7s (en و fr) → نافذة 5.8s تجمعهما ويحسم وزن الثقة.
            # فجوات الجمل المتجاورة الحقيقية: 7.8–16.4s — فوق النافذة بأمان.
            if (now - t0_g) < 5.8:
                gid = g
                break
        if gid is None:
            gid = f"g{int(now*1000)}"
            PENDING[gid] = (now, [])
        PENDING[gid][1].append((lang, text, conf))
        _ts = time.strftime("%H:%M:%S", time.localtime(now)) + f".{int((now % 1) * 1000):03d}"
        print(f"[multi] متنافس {_ts} [{lang}] conf={conf:.2f}: {text[:48]}", flush=True)
        # نبدأ التحكيم مرة واحدة لكل نافذة
        if len(PENDING[gid][1]) == 1:
            asyncio.create_task(_crown(gid))

    async def _crown(gid: str):
        # 🔧 Claude #5 نافذة تكيفية بالسقف: متنافس واحد = سقف 2.5s، عدة
        # متنافسين = سقف 7.0s (جمع المرشح الصحيح المتأخر خلف هلوسة أسرع —
        # قياس 12:40: en 5.2s / fr 5.7s). لكن السقف كان نوماً أعمى ثابتاً:
        # جملة بمتنافس واحد واضح تنتظر 2.5s كاملة حتى لو لم يظهر أي منافس
        # ثانٍ منذ اللحظة الأولى.
        # 🎯 مراجعة Claude (تدقيق تأخير): استطلاع كل 0.3s بدل نوم كتلة واحدة
        # — نتوّج فور سكون حقيقي (0.8s بلا وافد جديد بعد حد أدنى 0.6s، مطابق
        # لحساسية duo المثبتة) مع إبقاء نفس سقفي 2.5/7.0s كحد أقصى مضمون
        # للحالات المتنازع عليها فعلاً — لا تغيير على الصحة، فقط على السرعة
        # حين لا يوجد تنافس حقيقي.
        start = time.time()
        last_growth = start
        prev_count = 1
        while gid in PENDING:
            await asyncio.sleep(0.3)
            if gid not in PENDING:
                break
            cur_count = len(PENDING[gid][1])
            if cur_count > prev_count:
                last_growth = time.time()
                prev_count = cur_count
            now_p = time.time()
            elapsed = now_p - start
            quiet = now_p - last_growth
            cap = 2.5 if prev_count == 1 else 7.0
            if elapsed >= cap or (elapsed >= 0.6 and quiet >= 0.8):
                break
        if gid not in PENDING:
            return
        _, items = PENDING.pop(gid)
        if not items:
            return
        # 🛡️ الفلتر اللغوي الصارم (نفس duo — قياس دورة المؤتمر 2026-09-03):
        # صوت 10% + ضجيج + سرعة 1.35x: كل الجمل صدرت عبر محرك ar!
        # (الإنجليزية عبر ar ترجمت عربية→عربية، والفرنسية هلوسة عربية) —
        # الحل: نحجب أي متنافس نصه بلغة غير لغة محركه، ونحجب النافذة
        # كلياً لو كل المتنافسين بلغات خاطئة.
        _ar_chars = lambda t: sum(1 for c in t if "\u0600" <= c <= "\u06FF")
        _FR_WORDS = {"nous", "vous", "ils", "elle", "sont", "dans", "pour", "avec",
                     "mais", "être", "peut", "tout", "cette", "leur", "sans", "sous",
                     "entre", "vers", "aussi", "déjà", "alors", "donc", "chez", "où"}
        def _lang_of(text):
            letters = [c for c in text if c.isalpha()]
            if not letters:
                return "en"
            a = _ar_chars(text)
            if a >= len(letters) * 0.4:   # عربي حقيقي: أغلب الحروف عربية
                return "ar"
            if any("\u4E00" <= c <= "\u9FFF" for c in text):
                return "zh-CN"
            # 🎯 قياس 12:36 (multi): النص الفرنسي صُنّف "en" دائماً → مرشح fr لا
            # يجتاز الفلتر أبداً → الجملة fr لم تُتوَّج رغم conf=1.00.
            words = {w.strip(".,!?;:").lower() for w in text.split()}
            if sum(1 for w in words if w in _FR_WORDS) >= 2:
                return "fr"
            return "en"
        # 🎯 عتبة ثقة انتقائية: هلوسات كل المحركات تأتي بثقة منخفضة (zh=0.07, tr مثال 0.74)
        # لكن Cohere ar يرسل conf=0.00 حتى لنصوصه الصحيحة (قياس دورتين) — نستثنيه
        # 🛡️ كاشف الهجين (قياس 14:20 و14:25): هلوسة Cohere ar للكلام الفرنسي
        # تخرج نصاً عربياً-لاتينياً ممزوجاً «بونجور مون امي» — كلام فرنسي عبر محرك خاطئ.
        # الجملة العربية الحقيقية لا تحوي كلمات لاتينية متعددة → نحجب الهجينة و
        # ننتظر نتيجة المحرك الصحيح (nova-3 fr) خلفها.
        def _latin_words(t):
            return sum(1 for w in t.split() if any(c.isascii() and c.isalpha() for c in w) and any(c.isascii() and c.isalpha() for c in w))
        valid = []
        for (l, t, c) in items:
            if _lang_of(t) != l:
                continue
            if l == "ar" and _latin_words(t) >= 2 and len(t.split()) <= 8:
                # نص قصير هجين عربي+لاتيني عبر محرك العربية = هلوسة كلام أجنبي
                continue
            valid.append((l, t, c))
        if not valid:
            return  # كل المتنافسين بلغة خاطئة (ضجيج/هلوسة عبر محرك خاطئ)
        # 🎯 قاعدة الكرسي الأجنبي (نفس duo — قياس 16:11→16:39): الدليل على كلام
        # أجنبي يشمل نصاً لاتينياً نقياً (≥3 كلمات، صفر عربي) عبر en/fr حتى لو
        # أسقطه الفلتر الصارم (قياس 16:39: [en] التقط «Nous devons...» لكن
        # _lang_of=fr أسقطه → تُوِّج الصدى العربي وحيداً باتجاه معكوس).
        _latin_pure = lambda t: (not any("\u0600" <= c <= "\u06FF" for c in t)) and len(t.split()) >= 3
        # 🛡️ إصلاح هلوسة ميدانية (لقطة طارق 12:04): كلام عربي حقيقي عبر ميكروفون
        # هاتف → صدى لاتيني هلوسي عبر en ("Stay with us", "Thank you" لم تُقل).
        # العربي الصحيح ≥5 كلمات لا يُحذف أبداً؛ اللاتيني يجب أن يكون جملة
        # كاملة ≥6 كلمات ليحسب دليلاً على كلام أجنبي حقيقي.
        ar_items = [it for it in valid if it[0] == "ar"]
        ar_long = any(len(it[1].split()) >= 5 for it in ar_items)
        foreign_long = any(l in ("en", "fr") and _latin_pure(t) and len(t.split()) >= 6
                           for (l, t, c) in items)
        foreign_proof = (any(it[0] != "ar" for it in valid) and not ar_long) or foreign_long
        if foreign_proof:
            valid = [it for it in valid if it[0] != "ar"]
        if not valid:
            return
        # 🎯 دمج شظايا نفس اللغة (قياس 16:39): الجملة العربية قُسمت شظيتين في
        # نافذة واحدة فتُوِّجت الأطول فقط — ندمج غير المتداخل ونتوج الكامل.
        by_lang = {}
        for it in valid:
            by_lang.setdefault(it[0], []).append(it)
        merged = []
        for l, cands in by_lang.items():
            keep = []
            for (l2, t2, c2) in cands:
                k2 = text_key(t2)
                dup = False
                for i, (l3, t3, c3) in enumerate(keep):
                    k3 = text_key(t3)
                    if k2 and (k2 in k3 or k3 in k2):
                        if len(t2) > len(t3):
                            keep[i] = (l2, t2, max(c2, c3))
                        dup = True
                        break
                if not dup:
                    keep.append((l2, t2, c2))
            if len(keep) > 1:
                keep = [(l, " ".join(t for (_, t, _) in keep),
                         max(c for (_, _, c) in keep))]
            merged.extend(keep)
        valid = merged
        def _fair(it):
            lang, text, conf = it
            detected = _lang_of(text)
            bonus = 0.5 if detected == lang else 0.0
            # 🎯 قياس 16:11-16:14 (multi بسجل): en/fr عبر Cohere يرسلان النص
            # الصحيح بـ conf=0.00 → وزن 0.0 فخسرا أمام صدى ar (0.85) — 4/4
            # باتجاه معكوس. الوزن المحايد لأي conf==0.0 (Cohere كله لا يرسل
            # ثقة حقيقية)، ويحسم bonus مطابقة اللغة والنص الأطول.
            trust = 0.85 if conf == 0.0 else min(conf, 0.99)
            return (trust + bonus, len(text))
        best = max(valid, key=_fair)
        lang, text, conf = best
        # 🎯 فحص التكرار على الفائز الفعلي بعد كل الدمج/الفلترة — ليس على أول
        # مرشح خام في النافذة (items[0]) الذي قد لا يكون حتى الفائز
        now = time.time()
        key0 = text_key(text)[:25]
        for k, t in SENT_AT.items():
            if now - t < 12 and k[:25] == key0:
                return
        # 🔧 Claude #11: تفريغ القديم (تسريب بطيء عبر ساعات المؤتمر)
        for k in [k for k, t in SENT_AT.items() if now - t > 60]:
            SENT_AT.pop(k, None)
        SENT_AT[text_key(text)] = now
        _recent_crowned.append((" ".join(text.lower().split()), now))
        del _recent_crowned[:-32]
        while _recent_crowned and now - _recent_crowned[0][1] > 12:
            _recent_crowned.pop(0)
        await user_ws.send_json({"type": "source", "text": text, "speaker_lang": lang})
        metrics.inc("sentences_total")
        # 🎯 قياس 12:40: الجملة 4 en (شظية nova conf≥0.30) ذابت في نافذة جملة fr
        # فبُثّت واحدة فقط. تمريرة ثانية: جملة حقيقية أخرى داخل النافذة بلغة مختلفة
        # وثقة ≥0.30 (صدى Cohere ar conf=0.00 لا يعبر) تُتوَّج كذلك.
        second = None
        for l, t, c in valid:
            if l == lang or t == text:
                continue
            if l != "ar" and c >= 0.30 and text_key(t) not in SENT_AT:
                if second is None or len(t) > len(second[1]):
                    second = (l, t, c)
        # 🛡️ نفس فحص الفائز الأول: مسح البادئة 25 حرف على SENT_AT + الذيل
        # اليتيم قبل الإرسال — تمريرة ثانية كانت تتجاوز كلا الفحصين
        if second is not None:
            l2, t2, c2 = second
            k2 = text_key(t2)[:25]
            _dupe2 = any(now - t < 12 and k[:25] == k2 for k, t in SENT_AT.items())
            if _dupe2 or _is_orphan_tail(t2, now):
                second = None
        if second:
            l2, t2, c2 = second
            SENT_AT[text_key(t2)] = now
            _recent_crowned.append((" ".join(t2.lower().split()), now))
            del _recent_crowned[:-32]
            while _recent_crowned and now - _recent_crowned[0][1] > 12:
                _recent_crowned.pop(0)
            await user_ws.send_json({"type": "source", "text": t2, "speaker_lang": l2})
            try:
                loop = asyncio.get_running_loop()
                translated2 = await asyncio.wait_for(
                    loop.run_in_executor(_EXECUTOR, translate_sync, t2, tts_lang), timeout=8)
            except Exception:
                translated2 = None
            if translated2:
                await user_ws.send_json({"type": "translation", "text": translated2,
                                         "speaker_lang": l2})
                async def _speak2(txt=translated2):
                    url = await _tts_audio(txt, tts_lang)
                    if url:
                        try:
                            await user_ws.send_json({"type": "tts", "url": url})
                        except Exception:
                            pass
                asyncio.create_task(_speak2())
        try:
            loop = asyncio.get_running_loop()
            translated = await asyncio.wait_for(
                loop.run_in_executor(_EXECUTOR, translate_sync, text, tts_lang), timeout=8)
        except Exception:
            translated = None
        if translated:
            await user_ws.send_json({"type": "translation", "text": translated,
                                     "speaker_lang": lang})
            async def _speak(txt=translated):
                url = await _tts_audio(txt, tts_lang)
                if url:
                    await user_ws.send_json({"type": "tts", "url": url})
            asyncio.create_task(_speak())

    async def collector(lang: str, up):
        """جمع نتائج محرك لغة واحدة — الجملة تصل مع هوية لغتها."""
        nonlocal finished
        while not finished:
            try:
                raw = await asyncio.wait_for(up.recv(), timeout=75)
                if isinstance(raw, bytes):
                    continue
                d = json.loads(raw)
                engine_got[lang] = time.time()
                if "errors" in d:
                    print(f"[multi] [{lang}] خطأ من المحرك: {str(d)[:70]}", flush=True)
                    continue
                if d.get("is_final") and d.get("transcript"):
                    text = str(d["transcript"]).strip()
                    if not text:
                        continue
                    # مرشح الجودة: تجاهل الضجيج (ثقة منخفضة أو جملة قصيرة رمزية)
                    conf = float(d.get("confidence", 0) or 0)
                    print(f"[multi] [{lang}] خام conf={conf:.2f}: {text[:44]}", flush=True)
                    alnum_count = sum(1 for c in text if c.isalnum())
                    # 🎯 (سجل المستخدم 23:06): «Hi.» حُجبت كترحيب حقيقي!
                    # نسمح: كلمة+ واضحة الحروف. نحجب: الضجيج الرمزي القصير
                    if conf < 0.65 and alnum_count < 2:
                        continue  # ضجيج التُقط كنص (قياس: «。」 من محرك zh على ضجيج قاعة)
                    # 🏆 آلية "الفائز الأفضل": محركات متعددة قد تلتقط نفس الجملة بلغاتها —
                    # نجمع المتنافسين 600ms ونرسل الأقوى فقط (أعلى ثقة ثم أطول نص)
                    now = time.time()
                    await _adjudicate(lang, text, conf, now)
                    continue
            except asyncio.TimeoutError:
                if finished:
                    return
            except Exception:
                if finished:
                    return
                # إعادة فتح الاتصال
                try:
                    await up.close()
                except Exception:
                    pass
                new_up = await _open_upstream(lang)
                if new_up:
                    upstreams[lang] = new_up
                    # 🔧 نفس إصلاح duo (Claude #7): الاستدعاء الذاتي المباشر
                    # يبني stack لا يُفك (RecursionError بعد عدة انقطاعات) —
                    # مهمة جديدة بدلاً منه
                    asyncio.create_task(collector(lang, new_up))
                return

    async def _release_pulse():
        """🎯 مراجعة Claude (تدقيق تأخير): وضع القاعة لم يملك أي نبض تحرير
        إطلاقاً (خلافاً لـ live/duo) — is_final هنا كان يعتمد كلياً على
        endpointing الطبيعي للمحرك، وحده لا يُطلق فعلياً عندما يستمر AGC في
        تضخيم ضجيج الغرفة (مُصلح الآن في live_mode._agc_amplify). نفس نمط
        duo: نبض صامت 350ms بعد آخر كلام حقيقي (agc_state[6])، وسلسلة قصيرة
        3 ثوانٍ فقط لتفادي تكرار is_final لنفس الجملة."""
        _n_pulse = 0
        while not finished:
            await asyncio.sleep(0.4)
            last_speech = agc_state[6] if len(agc_state) > 6 and agc_state[6] else 0.0
            idle = time.time() - last_speech
            if 0.25 < idle < 3.0:
                for _l, _u in list(upstreams.items()):
                    try:
                        await _u.send(SILENCE_1S)
                        _n_pulse += 1
                        if _n_pulse == 1:
                            print(f"[multi] 🫀 نبض التحرير يعمل (محرك {_l})", flush=True)
                    except Exception as _pe:
                        print(f"[multi] نبض فشل لمحرك {_l}: {type(_pe).__name__}", flush=True)

    collectors = [asyncio.create_task(collector(l, u)) for l, u in list(upstreams.items())]
    asyncio.create_task(_release_pulse())
    try:
        await pump()
    finally:
        finished = True
        metrics.dec("ws_active")
        for c_task in collectors:
            c_task.cancel()
        for up in upstreams.values():
            try:
                await up.close()
            except Exception:
                pass


TTS_DIR = Path("/home/ubuntu/translator-app/static/tts_audio")
TTS_DIR.mkdir(parents=True, exist_ok=True)
import time as _tmod


async def _tts_audio(text: str, lang: str):
    """الجيل الجديد: TTS → mp3 24k → ملف ثابت + URL خفيف.
    الحجة القاتلة (سجل 2026-09-03 13:21): دفع b64 17KB داخل WS عبر النفق
    المؤقت يقتل الاتصال فوراً (WebSocketDisconnect 1006) — الترجمات تتوقف كلها!
    GET خفيف لملف 9KB لا يكسر شيئاً (نفس آلية الصور والصفحات)."""
    try:
        import subprocess as _sp
        try:
            loop = asyncio.get_running_loop()
            p = await asyncio.wait_for(loop.run_in_executor(_EXECUTOR, tts_sync, text, lang), timeout=10)
        except asyncio.TimeoutError:
            print("[multi] TTS تجاوز 10s — نتجاهله", flush=True)
            return None
        raw = p.read_bytes()
        suffix = p.suffix
        p.unlink(missing_ok=True)
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-loglevel", "error", "-f", "wav" if suffix == ".wav" else "mp3",
            "-i", "pipe:0",
            "-ar", "16000", "-ac", "1", "-c:a", "libmp3lame", "-b:a", "24k",
            "-f", "mp3", "pipe:1",
            stdin=_sp.PIPE, stdout=_sp.PIPE, stderr=_sp.DEVNULL,
        )
        small, _ = await proc.communicate(raw)
        if not small:
            small = raw
        fname = f"t{_tmod.time_ns()}.mp3"
        (TTS_DIR / fname).write_bytes(small)
        # 🔧 مراجعة Claude #4: prune بالعمر لا بالعدّ — العدّ كان يحذف ملفات جلسات
        # نشطة قبل تنزيلها (سبب مباشر لـ"TTS لا يُسمع")
        try:
            cutoff = _tmod.time() - 600   # نحتفظ 10 دقائق
            for f in TTS_DIR.glob("t*.mp3"):
                if f.stat().st_mtime < cutoff:
                    f.unlink(missing_ok=True)
        except Exception:
            pass
        return f"/tts_audio/{fname}"
    except Exception as e:
        print(f"[multi] TTS فشل: {type(e).__name__}: {str(e)[:60]}", flush=True)
        return None


async def multi_lang_endpoint(websocket: WebSocket, tts_lang: str = "ar"):
    await websocket.accept()
    try:
        await multi_lang_worker(websocket, tts_lang)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[multi] endpoint خطأ: {type(e).__name__}: {str(e)[:80]}", flush=True)
