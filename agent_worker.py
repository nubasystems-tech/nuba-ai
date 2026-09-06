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
            # 🪟 الوضع المرئي: نافذة حية على شاشة الابتوب تعرض التنفيذ لحظياً
            # (طلب طارق: رؤية العامل يعمل كزميل) + التقاط النتيجة للخادم
            base = os.path.dirname(os.path.abspath(__file__))
            tdir = os.path.join(base, "tasks")
            os.makedirs(tdir, exist_ok=True)
            tid = str(int(time.time() * 1000))
            ps1 = os.path.join(tdir, f"t_{tid}.ps1")
            out_f = os.path.join(tdir, f"t_{tid}.out")
            done_f = os.path.join(tdir, f"t_{tid}.done")
            cmd_f = os.path.join(tdir, f"t_{tid}.cmd")
            for f in (out_f, done_f):
                try:
                    os.unlink(f)
                except OSError:
                    pass
            with open(ps1, "w", encoding="utf-8-sig") as fh:
                fh.write(payload + "\n")
            banner = "".join(c if ord(c) < 128 else "?" for c in str(task.get("note", "remote task"))[:50])
            with open(cmd_f, "w", encoding="utf-8") as fh:
                fh.write(
                    "@echo off\r\n"
                    "chcp 65001 >nul\r\n"
                    "title NUBA AI WORKER - live task\r\n"
                    "color 0E\r\n"
                    "echo ==============================================\r\n"
                    "echo    NUBA AI WORKER - executing remote task live\r\n"
                    f"echo    {banner}\r\n"
                    "echo ==============================================\r\n"
                    "echo.\r\n"
                    f"powershell -NoProfile -ExecutionPolicy Bypass -File \"{ps1}\" > \"{out_f}\" 2>&1\r\n"
                    f"type \"{out_f}\"\r\n"
                    f"echo %ERRORLEVEL%> \"{done_f}\"\r\n"
                    "echo.\r\n"
                    "echo    TASK COMPLETE - window closes in 8s\r\n"
                    "timeout /t 8 >nul\r\n"
                )
            t0 = time.time()
            try:
                subprocess.Popen(["cmd", "/c", "start", "", cmd_f])
            except Exception as e:
                return {"ok": False, "error": f"launch failed: {e}"}
            while time.time() - t0 < 600:
                if os.path.exists(done_f):
                    time.sleep(0.3)
                    try:
                        code = int(open(done_f).read().strip() or 0)
                    except Exception:
                        code = 0
                    out = ""
                    try:
                        out = open(out_f, encoding="utf-8", errors="replace").read()[-8000:]
                    except Exception:
                        pass
                    return {"ok": code == 0, "code": code,
                            "output": out, "error": "", "secs": round(time.time() - t0, 1),
                            "visible": True}
                time.sleep(1.0)
            return {"ok": False, "error": "timeout waiting visible task"}
        if kind == "shell_hidden":
            # سطر أوامر (PowerShell) — مهلة 10 دقائق (خفي كما كان)
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
