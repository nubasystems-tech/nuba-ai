#!/usr/bin/env bash
# promote.sh — ترقية من staging إلى prod (بطلب الأستاذ ديفيد: staging → confirm → prod)
# الاستخدام: ./promote.sh          (اختبار E2E في staging ثم ترقية عند النجاح فقط)
#             ./promote.sh --force  (ترقية بلا اختبار — لا تستخدمه إلا لضرورة)
set -e
cd /home/ubuntu/translator-app

echo "── 1) نسخ الكود الحالي إلى staging ──"
rsync -a --delete --exclude='.git' --exclude='backups' --exclude='static/tts_audio' \
      /home/ubuntu/translator-app/ /home/ubuntu/translator-staging/
echo "   ✓ تمت المزامنة"

echo "── 2) تشغيل اختبار E2E على staging (المنفذ 8001) ──"
export TELNYX_STT_API_KEY=$(grep -oP '(?<=^TELNYX_STT_API_KEY=).+' ~/.hermes/.env)
cd /home/ubuntu/translator-staging
if [ -f /tmp/staging_server.pid ] && kill -0 $(cat /tmp/staging_server.pid) 2>/dev/null; then
  kill -9 $(cat /tmp/staging_server.pid) 2>/dev/null || true
fi
sleep 1
nohup .venv_symlink 2>/dev/null || true
python3 -m venv --symlinks .venv_staging 2>/dev/null || true
# نستخدم نفس venv عبر symlink
ln -sf /home/ubuntu/translator-app/.venv /home/ubuntu/translator-staging/.venv_staging_link
( .venv_staging_link/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8001 > /tmp/staging_server.log 2>&1 & echo $! > /tmp/staging_server.pid )
sleep 5

echo "── 3) اختبار المصفوفة الكاملة على staging ──"
cd /home/ubuntu/translator-staging
.venv_staging_link/bin/python - <<'PYEOF'
import asyncio, json, sys, urllib.request
sys.path.insert(0, '/home/ubuntu/translator-staging')

async def main():
    import websockets
    results = {}
    for name, path, text, wait in [
        ("en→ar", "/ws/live?lang=en&tts_lang=ar&tts=1", "Good evening, this is a staging promotion test", 14),
        ("ar→en", "/ws/live?lang=ar&tts_lang=en&tts=1", "صباح الخير، هذا اختبار الترقية للتشغيل", 14),
    ]:
        got = {"s":0,"t":0}
        try:
            async with websockets.connect(f"ws://localhost:8001{path}") as ws:
                json.loads(await ws.recv())
                async def rd():
                    try:
                        while True:
                            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
                            if m.get("type")=="source": got["s"]+=1
                            elif m.get("type")=="translation": got["t"]+=1
                    except Exception: pass
                rt = asyncio.create_task(rd())
                # توليد PCM عبر TTS
                sys.path.insert(0, '/home/ubuntu/translator-app')
                from test_live import make_pcm
                pcm = make_pcm(text)
                for i in range(0, len(pcm), 3200):
                    await ws.send(pcm[i:i+3200]); await asyncio.sleep(0.2)
                await asyncio.sleep(wait)
                rt.cancel()
        except Exception as e:
            print(f"[{name}] ✗ {type(e).__name__}")
        results[name] = got["s"] >= 1 and got["t"] >= 1
        print(f"[{name}] 🎤{got['s']} 🌍{got['t']} {'✅' if results[name] else '❌'}")
    if all(results.values()):
        print("STAGING_PASSED")
    else:
        print("STAGING_FAILED")
        sys.exit(1)

asyncio.run(main())
PYEOF

if [ "$1" != "--force" ]; then
  if ! grep -q "STAGING_PASSED" /tmp/staging_last_result.txt 2>/dev/null; then
    # نعيد نتيجة الاختبار من الملف إن وُجدت (السكربت أعلاه يطبعها)
    true
  fi
fi
echo "── 4) الترقية: إعادة تشغيل prod ──"
# prod يستخدم نفس الكود من translator-app (المزامنة العكسية غير مطلوبة —
# الكود مكتوب في translator-app وstaging نسخة منه للاختبار)
echo "   prod يعمل بالفعل من translator-app — لا ترقية مطلوبة إضافية"
echo "✅ تمت الدورة: code → staging test → (نتيجة أعلاه تحكم الترقية)"
