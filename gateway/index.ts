/**
 * Nuba AI Gateway — بوابة ElysiaJS خارقة السرعة أمام خادم Python
 * 
 * البنية:  هاتف المستخدم → Elysia (:3000) → Python FastAPI (:8000)
 * المكاسب:
 *  - µWebSocket أصلي: توزيع البث أسرع بكثير
 *  - تمرير ثنائي الاتجاه للبث الصوتي (binary pass-through)
 *  - توجيه لغات + صفحة الواجهة من ذاكرة Elysia
 *  - نقطة نشر مستقبلية على Edge
 */
import Elysia from "elysia";
import { existsSync, readFileSync } from "fs";

const PY = "http://127.0.0.1:8000";

// ─── تحميل الواجهة مرة واحدة في الذاكرة (توزيع فوري) ───
const UI_DIR = new URL("../static3d/", import.meta.url).pathname;
const pages = {};
for (const [route, file] of [
  ["/", "app.html"],
  ["/app.html", "app.html"],
  ["/lite", "lite.html"],
]) {
  try {
    pages[route] = readFileSync(UI_DIR + file, "utf-8");
  } catch {
    console.log(`[gateway] تحذير: ${file} غير موجود`);
  }
}

// ─── تمرير HTTP بسيط للخادم الأساسي (صحة/مقاييس/ترجمة/تسجيل) ───
async function proxy(path, init) {
  const r = await fetch(PY + path, init);
  return new Response(r.body, {
    status: r.status,
    headers: { "content-type": r.headers.get("content-type") || "application/json" },
  });
}

const gateway = new Elysia()
  // الصفحات — من الذاكرة، أسرع ما يمكن
  .get("/", () => pages["/"] ?? "Nuba AI — الواجهة غير جاهزة")
  .get("/app.html", () => pages["/app.html"] ?? "غير موجود")
  .get("/lite", () => pages["/lite"] ?? "غير موجود")

  // حالة البوابة نفسها
  .get("/gw/health", () => ({
    status: "ok",
    service: "nuba-gateway",
    runtime: "bun-" + Bun.version,
    upstream: PY,
  }))

  // تمرير REST للخادم الأساسي
  .get("/api/health", () => proxy("/api/health"))
  .get("/api/metrics", () => proxy("/api/metrics"))
  .post("/api/translate", ({ body }) =>
    proxy("/api/translate", { method: "POST", body: JSON.stringify(body), headers: { "content-type": "application/json" } }))

  // ─── 🎯 قلب البوابة: WebSocket تمرير ثنائي للبث الصوتي ───
  .ws("/ws/:mode", {
    open(ws) {
      const url = new URL(ws.data.request.url);
      const mode = ws.data.params.mode;          // live | duo | multi
      const search = url.search;                 // ?lang=..&tts=..
      const target = `ws://127.0.0.1:8000/ws/${mode}${search}`;

      console.log(`[gateway] WS ${mode}${search.slice(0, 60)} → فتح`);

      // اتصال عميل للخادم الأساسي
      const upstream = new WebSocket(target);
      const queue = [];

      upstream.binaryType = "arraybuffer";
      upstream.onopen = () => {
        for (const item of queue) upstream.send(item);
        queue.length = 0;
      };
      upstream.onmessage = (ev) => {
        // رسائل النص (JSON ترجمات) تمر مباشرة
        if (typeof ev.data === "string") ws.send(ev.data);
        else ws.send(new Uint8Array(ev.data));
      };
      upstream.onclose = () => { try { ws.close(); } catch {} };
      upstream.onerror = () => { try { ws.close(); } catch {} };

      ws.data.upstream = upstream;
      ws.data.queue = queue;
    },

    message(ws, message) {
      console.log("[gw] message type:", typeof message, "len:", message?.length ?? message?.byteLength, "up:", ws.data.upstream ? ws.data.upstream.readyState : "none");
      const up = ws.data.upstream;
      if (!up || up.readyState !== 1) { ws.data.queue?.push(message); return; }
      if (typeof message === "string") {
        up.send(message);          // JSON نصي
      } else {
        up.send(new Uint8Array(message));   // بايتات صوت
      }
    },

    close(ws) {
      console.log("[gateway] WS أغلق");
      try { ws.data.upstream?.close(); } catch {}
    },
  })

  // ─── ملفات الصوت المولدة (TTS) — من القرص مباشرة ───
  .get("/tts_audio/:file", ({ params }) => {
    const p = new URL(`../static/tts_audio/${params.file}`, import.meta.url).pathname;
    if (!existsSync(p)) return new Response("not found", { status: 404 });
    return new Response(readFileSync(p), {
      headers: { "content-type": "audio/mp3", "cache-control": "public, max-age=300" },
    });
  })

  .listen(3000);

console.log(`🚀 Nuba Gateway — http://0.0.0.0:3000 → ${PY} (bun ${Bun.version})`);
