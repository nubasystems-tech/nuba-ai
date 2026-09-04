<div align="center">

# **N** — Nuba AI

**نظام عالمي لكسر حواجز التواصل البشري** — مترجم فوري حي للمؤتمرات

*Real-time conference translator with AI speech processing*

🇸🇩 من أرض النوبة — الذهب والرمال والنخيل والنهر 🥇🏜️🌴🌊

</div>

---

## نظرة عامة | Overview

مترجم فوري احترافي يعمل لحظياً أثناء كلام المتحدث — يتحمل الصوت الضعيف، الضجيج، المتحدث السريع، وساعات المؤتمرات المتواصلة.

- ⚡ **ترجمة فورية**: النص يصل **+0.3 ثانية** بعد نهاية الكلام (مقيس)
- 🔊 **صوت حقيقي لكل لغة**: عربي (Bayan)، إنجليزي (Huda)، فرنسي (Denise)، صيني (Xiaoxiao)، تركي (Emel)
- 🧠 **معالج صوت طيفي numpy**: يرفع الهمس حتى ×250 مع قمع ضجيج القاعة
- 🔁 **حوار تبادلي تلقائي**: يكشف لغة المتحدث ويترجم للطرف الآخر بلا أزرار
- 🏛️ **قاعة متعددة اللغات**: 5 محركات متوازية + تحكيم ذكي بالنص والثقة
- 📊 **مراقبة حية**: عدادات جلسات/جمل/ترجمات/أخطاء على `/api/metrics`

## البنية المعمارية | Architecture

```
متصفح الهاتف (WebSocket PCM 16k)
        │
        ▼
FastAPI (uvicorn :8000)
        │
        ├── /ws/live   وضع اللغة الواحدة — نبض تحرير النتائج كل 350ms
        ├── /ws/duo    حوار تبادلي — محركان + تحكيم + حارس لغات
        ├── /ws/multi  قاعة 5 محركات — تتويج تكيفي 2.5s/7s
        ├── /api/stt   وضع التسجيل — عبر stt_bridge (خيط مستقل)
        └── /api/voice جسر المكالمات (Telnyx Call Control)
        │
        ▼
stt_bridge (خيط مستقل بحلقة خاصة — يعزل Telnyx عن event loop)
        │
        ▼
Telnyx STT (nova-3 / Cohere) → الترجمة → TTS → URL صوتي خفيف
```

## اكتشافات هندسية موثقة | Engineering Discoveries

1. **محركات Telnyx تحتجز النتائج** حتى يصل صوت/صمت جديد بعدها — الحل: نبض تحرير صامت (من 8.8s → 0.3s)
2. **نبض b64 داخل WebSocket يكسر الاتصال** (1006) — الحل: الصوت عبر URL خفيف
3. **FastAPI يتجاهل Query() في توقيع File+Form** — الحل: قراءة يدوية من request
4. **فحص صوت أندرويد بلا قيود يدوية** + GainNode ×3 — القيود كانت تخنق الميكروفونات

## التشغيل | Quick Start

```bash
# المتطلبات: Python 3.11 + ffmpeg
pip install fastapi uvicorn websockets httpx numpy python-multipart

export TELNYX_STT_API_KEY=your_key_here   # من portal.telnyx.com

python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

افتح `http://localhost:8000/app3d/app.html` — اسمح بالميكروفون — اضغط 🎙

## الاختبارات | Tests

```bash
python full_suite_v13.py   # 5/5: live/duo/guard/stt/short + metrics
python e2e_full_matrix.py  # مصفوفة اللغات عبر النفق
```

آخر نتيجة موثقة (v13.3): **5/5 خضراء** — live +0.6s، duo بالاتجاهين، صفر تسريب، صفر هلوسة.

## الأمان | Security

- المفاتيح من البيئة فقط (لا مفاتيح في الكود)
- رؤوس أمان (nosniff/X-Frame/Permissions-Policy)
- سقف حجم 64KB على مدخلات الصوت (مضاد DoS)
- صمت TTS عبر ملفات مؤقتة بلا كشف مسارات

## التاريخ | History

10 نقاط استعادة موثقة في git — من v11.1 (أول مصفوفة خضراء) إلى v13.3 (كل ملاحظات مراجعة Claude Code منفذة).

---

<div align="center">

**N** — شعار واحد. هوية واحدة.

Made with 🥇 from Nubian heritage — *Meroe · Gebel Barkal · Taharqa*

</div>
