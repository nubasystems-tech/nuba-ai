"""Nuba Laptop Worker — عامل اللابتوب الدائم
البنية: اللابتوب يشغل هذا السكربت عند الإقلاع (مهمة مجدولة ويندوز)
→ يست polling الخادم كل 30 ثانية: /agent/next-task
→ ينفذ المهام المرسلة (سطر أوامر/سكربت) محلياً على اللابتوب
→ يرجع النتيجة: /agent/report
→ المهام تُرسل من Hermes (أنا) عبر endpoint موجود على خادم المترجم
مزايا: اللابتوب يصبح يداً للخادم — أي مهمة أنشرها له فور اتصاله بدون SSH انتظار"""

import json
import subprocess
import sys
import time
import urllib.request
import urllib.error
import os
import tempfile
import threading
import socket

SERVER = "http://100.77.139.53:8000"   # خادم Hermes عبر Tailscale (ثابت!)
WORKER_ID = os.environ.get("COMPUTERNAME", "DELL-")
POLL_SEC = 30
TOKEN = "nuba-worker-2026"             # مصادقة بسيطة (شبكة Tailscale خاصة أصلاً)

# ── حارس ضد التعليق الدائم ──
# نوم اللابتوب قد يعلّق urlopen إلى الأبد رغم timeout، وسياسة IgnoreNew
# تمنع تكرار المهمة (كل 5 دقائق) من إحياء نسخة جديدة. الحارس يقتل
# العملية بعد 3 دقائق تعليق → RestartCount/التكرار يعيدانها خلال دقيقتين.
_last_loop = time.time()
_task_deadline = 0.0


def _watchdog():
    while True:
        time.sleep(30)
        now = time.time()
        if now - _last_loop > 180 and now > _task_deadline:
            print("[nuba-worker] watchdog: main loop hung >3min - exiting for auto-restart", flush=True)
            os._exit(3)


def api(path, data=None, timeout=12):
    req = urllib.request.Request(
        SERVER + path,
        data=json.dumps(data).encode() if data else None,
        headers={"Content-Type": "application/json", "X-Worker-Token": TOKEN,
                 "Connection": "close"},
        method="POST" if data else "GET")
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode() or "{}")
        except Exception:
            return {}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:80]}"}


def run_task(task):
    """تنفيذ مهمة واحدة على اللابتوب وإرجاع النتيجة"""
    kind = task.get("kind")
    payload = task.get("payload", "")
    t0 = time.time()
    try:
        if kind == "shell":
            # سطر أوامر (PowerShell) — مهلة 10 دقائق
            p = subprocess.run(
                ["powershell", "-NoProfile", "-Command", payload],
                capture_output=True, text=True, errors="replace", timeout=600)
            out = (p.stdout or "")[-8000:]
            err = (p.stderr or "")[-2000:]
            return {"ok": p.returncode == 0, "code": p.returncode,
                    "output": out, "error": err, "secs": round(time.time() - t0, 1)}
        elif kind == "python":
            # كود بايثون يُكتب لملف مؤقت ثم ينفذ
            with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
                f.write(payload)
                path = f.name
            p = subprocess.run([sys.executable or "python", path],
                               capture_output=True, text=True, errors="replace", timeout=600)
            os.unlink(path)
            return {"ok": p.returncode == 0, "code": p.returncode,
                    "output": (p.stdout or "")[-8000:], "error": (p.stderr or "")[-2000:],
                    "secs": round(time.time() - t0, 1)}
        elif kind == "ping":
            return {"ok": True, "output": f"pong from {WORKER_ID}", "secs": 0}
        else:
            return {"ok": False, "error": f"نوع مهمة غير معروف: {kind}"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "انتهت المهلة (10 دقائق)"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"}


def _acquire_single_instance_lock():
    """قفل نسخة واحدة: منع تضاعف العامل (مفتاح Run + مهمة مجدولة معاً)"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(("127.0.0.1", 57777))
    except OSError:
        print("[nuba-worker] نسخة أخرى تعمل بالفعل — خروج", flush=True)
        os._exit(0)
    return s


def main():
    global _last_loop, _task_deadline
    _lock = _acquire_single_instance_lock()  # تبقى مفتوحة طوال حياة العملية
    threading.Thread(target=_watchdog, daemon=True).start()
    print(f"[nuba-worker] جاهز — {WORKER_ID} → {SERVER}")
    last_seen = 0
    while True:
        _last_loop = time.time()
        # 1) تسجيل حضور (كل دقيقة)
        if time.time() - last_seen > 60:
            r = api("/agent/hello", {"worker": WORKER_ID, "ts": time.time()})
            last_seen = time.time()
            if "ok" not in r and "error" not in r:
                pass
        # 2) طلب مهمة
        task = api(f"/agent/next-task?worker={WORKER_ID}")
        if task and task.get("task_id"):
            print(f"[nuba-worker] مهمة {task['task_id']}: {task.get('kind')} — {str(task.get('payload',''))[:80]}", flush=True)
            _task_deadline = time.time() + 630   # نافذة تنفيذ مشروعة للمهمة (10د + هامش)
            result = run_task(task)
            _task_deadline = 0
            report = api("/agent/report", {
                "task_id": task["task_id"], "worker": WORKER_ID,
                "result": result, "ts": time.time()})
            print(f"[nuba-worker] ✓ أُبلغ الخادم: {str(report)[:60]}")
            time.sleep(2)
            continue   # فوراً اطلب التالية (ربما طابور)
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[nuba-worker] إيقاف")
