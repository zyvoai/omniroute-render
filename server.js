// ─────────────────────────────────────────────────────────────────────
//  ZYVO MODEL CONTROL CENTER
//  gateway + sequential model health lab (one model at a time, verified)
//
//  • proxies everything to the OmniRoute child (127.0.0.1:20128)
//  • scanner routes:
//      GET  /scan           → operator GUI (dark console)
//      GET  /scan/results   → rich per-model JSON (status/latency/reply)
//      GET  /scan/status    → compact summary
//      GET  /active-models  → working models for the zyvo wrapper
//      GET  /zyvo-config    → complete ready-to-swap zyvo.json
//      POST /scan/full      → probe the whole catalog
//      POST /scan/new       → probe only models never seen before
//      POST /scan/stop      → stop the current run after the live probe
//  • everything persists on the mounted volume (DATA_DIR)
// ─────────────────────────────────────────────────────────────────────

import http from "node:http"
import { spawn } from "node:child_process"
import fs from "node:fs"
import path from "node:path"

const PORT = Number(process.env.PORT || 10000)
const UPSTREAM_HOST = "127.0.0.1"
const UPSTREAM_PORT = Number(process.env.UPSTREAM_PORT || 20128)
// Railway volume mounts at DATA_DIR (/app/data) — providers DB + lab state
// survive every redeploy
const GATEWAY_DATA = process.env.DATA_DIR
  ? path.join(process.env.DATA_DIR, "gateway")
  : path.join(process.cwd(), "data")
const STATE_FILE = path.join(GATEWAY_DATA, "state.json")
const SCAN_KEY = process.env.OMNIROUTE_API_KEY || ""

const PROBE_TOKENS = 5
const PROBE_TIMEOUT_MS = 50_000   // slow free upstreams take 30-60s to first byte
const IMAGE_TIMEOUT_MS = 90_000   // image generation is slower
const CONCURRENCY = 1             // one model at a time — every test fully verified
const NEW_EVERY_MS = 15 * 60 * 1000    // catalog diff → probe unseen models
const ACTIVE_EVERY_MS = 24 * 60 * 60 * 1000 // daily health check
const LIMIT_EVERY_MS = 24 * 60 * 60 * 1000  // daily-limit models get another chance
const FULL_EVERY_MS = 7 * 24 * 60 * 60 * 1000 // weekly full rescan
const SKIP_PATTERNS = /(content-safety|safety-guard|nemoguard|riva-translate|embedding|rerank|whisper|tts|guard)/i

// ── state ──────────────────────────────────────────────────────────
let state = {
  updatedAt: 0,
  fullAt: 0,
  dailyAt: 0,
  catalogAt: 0,
  totalIds: 0,
  runTotal: 0,
  runDone: 0,
  scanning: null, // "full" | "new" | "active" | "daily" | "catalog" | null
  stopFlag: false,
  nowTesting: "", // the model currently under the microscope
  log: [],
  models: {},     // id → { status, label, latency, reply, error, strikes, lastChecked }
}
const load = () => {
  try {
    state = { ...state, ...JSON.parse(fs.readFileSync(STATE_FILE, "utf8")) }
    state.scanning = null // a restart kills in-flight probes; never inherit the flag
    state.stopFlag = false
  } catch {}
}
const save = () => {
  try {
    fs.mkdirSync(GATEWAY_DATA, { recursive: true })
    fs.writeFileSync(STATE_FILE + ".tmp", JSON.stringify(state))
    fs.renameSync(STATE_FILE + ".tmp", STATE_FILE)
  } catch {}
}
const note = (m) => {
  state.log = [`${new Date().toISOString()} ${m}`, ...state.log].slice(0, 60)
  console.log(`[lab] ${m}`)
}

// ── OmniRoute child ────────────────────────────────────────────────
const CHILD_DATA = path.join(GATEWAY_DATA, "omniroute")
fs.mkdirSync(CHILD_DATA, { recursive: true })
const child = spawn(process.execPath, ["node_modules/omniroute/dist/server-ws.mjs"], {
  env: {
    ...process.env,
    PORT: String(UPSTREAM_PORT),
    HOSTNAME: "0.0.0.0",
    NODE_ENV: "production",
    DATA_DIR: CHILD_DATA,
    // both matter: NODE_OPTIONS raises V8's heap ceiling, OMNIROUTE_MEMORY_MB
    // is what omniroute's own pressure guard reads
    OMNIROUTE_MEMORY_MB: "1024",
    NODE_OPTIONS: [process.env.NODE_OPTIONS, "--max-old-space-size=1024"].filter(Boolean).join(" "),
  },
  stdio: ["ignore", "pipe", "pipe"],
})
child.stdout.on("data", (d) => process.stdout.write(`[omniroute] ${d}`))
child.stderr.on("data", (d) => process.stderr.write(`[omniroute] ${d}`))
child.on("exit", (c) => {
  console.error(`[lab] omniroute exited (${c}) — restarting in 5s`)
  setTimeout(() => process.exit(1), 5000)
})

// ── probes ─────────────────────────────────────────────────────────
const post = (urlPath, payload, timeoutMs) =>
  new Promise((resolve) => {
    const body = JSON.stringify(payload)
    const t0 = Date.now()
    const req = http.request(
      {
        host: UPSTREAM_HOST,
        port: UPSTREAM_PORT,
        path: urlPath,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          ...(SCAN_KEY ? { Authorization: `Bearer ${SCAN_KEY}` } : {}),
        },
        timeout: timeoutMs,
      },
      (res) => {
        let raw = ""
        res.on("data", (d) => (raw += d))
        res.on("end", () => resolve({ code: res.statusCode, body: raw, ms: Date.now() - t0 }))
      }
    )
    req.on("timeout", () => req.destroy())
    req.on("error", () => resolve({ code: 0, body: "", ms: Date.now() - t0 }))
    req.end(body)
  })

const chatProbe = (id) =>
  post("/v1/chat/completions", { model: id, messages: [{ role: "user", content: "hi" }], max_tokens: PROBE_TOKENS }, PROBE_TIMEOUT_MS)

const imageProbe = (id) =>
  post("/v1/images/generations", { model: id, prompt: "a small red circle", n: 1 }, IMAGE_TIMEOUT_MS)

const errText = (body, code) => {
  try {
    return JSON.parse(body)?.error?.message || `HTTP ${code}`
  } catch {
    return code === 0 ? "Timeout / no response" : `HTTP ${code}`
  }
}

const replyText = (body) => {
  try {
    const c = JSON.parse(body)?.choices?.[0]?.message?.content
    return typeof c === "string" ? c.trim().slice(0, 140) : ""
  } catch {
    return ""
  }
}

// ── lab run (one model at a time, fully verified) ──────────────────
const prettyName = (id) => {
  const tail = id.includes("/") ? id.split("/").slice(1).join("/") : id
  return tail.replace(/[:_]/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()).replace(/\s+Free\b/i, " (Free)")
}

async function verifyOne(id) {
  // under the microscope — the GUI live-tracks this
  state.nowTesting = id
  state.models[id] = { ...(state.models[id] || {}), status: "testing", label: "পরীক্ষা চলছে…", lastChecked: Date.now() }
  if (state.runDone % 3 === 0) save()

  const chat = await chatProbe(id)
  const latency = chat.ms
  let rec = { latency, reply: "", error: "", strikes: 0, lastChecked: Date.now() }

  if (chat.code === 200) {
    rec.status = "active"
    rec.label = ""
    rec.reply = replyText(chat.body)
    if (!rec.reply) rec.reply = "(empty reply)" // 200 with nothing = suspicious but alive
  } else if (chat.code === 400) {
    // non-chat? try image generation before judging
    const img = await imageProbe(id)
    if (img.code === 200) {
      rec.status = "image"
      rec.label = "Image generation model"
      rec.reply = "(image ok)"
    } else {
      rec.status = "non-chat"
      rec.label = "Non-chat model"
      rec.error = errText(img.body, img.code || 400)
    }
  } else if (chat.code === 429) {
    rec.status = "daily-limit"
    rec.label = "⏳ Daily limit reached"
    rec.error = errText(chat.body, chat.code)
  } else if (chat.code === 401 || chat.code === 403) {
    rec.status = "no-access"
    rec.label = "No access (401)"
    rec.error = errText(chat.body, chat.code)
  } else if (chat.code === 402) {
    rec.status = "paid"
    rec.label = "Paid only (402)"
  } else if (chat.code === 404) {
    rec.status = "gone"
    rec.label = "Not found (404)"
  } else {
    // timeout / 5xx — needs 2 strikes to bury; slow-but-alive gets a 2nd chance
    const prev = state.models[id] || {}
    const strikes = (prev.status === "hanging" ? prev.strikes || 0 : 0) + 1
    if (strikes < 2 && prev.status !== "active") {
      rec.status = "hanging"
      rec.strikes = strikes
      rec.label = `Double-check ${strikes}/2 (${errText(chat.body, chat.code)})`
      state.models[id] = { ...prev, ...rec }
      return { quick: true }
    }
    rec.status = "hanging"
    rec.label = chat.code === 0 ? "Timeout" : `HTTP ${chat.code}`
    rec.error = errText(chat.body, chat.code)
  }
  state.models[id] = { ...(state.models[id] || {}), ...rec }
  return { quick: false }
}

async function runLab(ids, kind) {
  if (state.scanning) return
  state.scanning = kind
  state.stopFlag = false
  state.runTotal = ids.length
  state.runDone = 0
  if (kind === "full") state.totalIds = ids.length
  note(`${kind} lab started — ${ids.length} model(s), one at a time`)
  save()

  for (const id of ids) {
    if (state.stopFlag) { note("stopped by operator"); break }
    const r = await verifyOne(id)
    state.runDone++
    const m = state.models[id]
    if (!r.quick) note(`${id}: ${m.status}${m.label ? " — " + m.label : ""} (${m.latency}ms)`)
    if (state.runDone % 3 === 0) save()
  }
  state.nowTesting = ""
  state.updatedAt = kind === "full" || kind === "new" ? Date.now() : state.updatedAt
  if (kind === "full") state.fullAt = Date.now()
  if (kind === "daily") state.dailyAt = Date.now()
  state.scanning = null
  state.stopFlag = false
  note(`${kind} lab finished — active: ${activeList().length}`)
  save()
}

const kick = (kind, fn) => {
  if (state.scanning) return
  fn().catch((e) => {
    state.scanning = null
    note(`${kind} failed: ${e?.message || e}`)
    save()
  })
}

const listIds = () =>
  new Promise((resolve) => {
    http
      .get(
        {
          host: UPSTREAM_HOST,
          port: UPSTREAM_PORT,
          path: "/v1/models",
          headers: SCAN_KEY ? { Authorization: `Bearer ${SCAN_KEY}` } : {},
        },
        (res) => {
          let raw = ""
          res.on("data", (d) => (raw += d))
          res.on("end", () => {
            try {
              resolve(JSON.parse(raw).data.map((m) => m.id).filter((id) => !SKIP_PATTERNS.test(id)))
            } catch {
              resolve([])
            }
          })
        }
      )
      .on("error", () => resolve([]))
  })

const activeList = () => Object.entries(state.models).filter(([, v]) => v.status === "active").map(([id]) => id)
const limitList = () => Object.entries(state.models).filter(([, v]) => v.status === "daily-limit").map(([id]) => id)

const fullScan = () => kick("full", async () => runLab(await listIds(), "full"))
const newScan = () =>
  kick("new", async () => {
    const ids = (await listIds()).filter((id) => !state.models[id])
    if (!ids.length) { note("no unseen models — catalog unchanged"); return }
    await runLab(ids, "new")
  })
const newProviderScan = () => // GUI 🎇 button — probe unseen + hanging retries
  kick("new", async () => {
    const unseen = (await listIds()).filter((id) => !state.models[id])
    const shaky = Object.entries(state.models)
      .filter(([, v]) => v.status === "hanging")
      .map(([id]) => id)
    const ids = [...unseen, ...shaky]
    if (!ids.length) { note("nothing new and nothing shaky — catalog unchanged"); return }
    note(`new-provider scan: ${unseen.length} unseen + ${shaky.length} retry`)
    await runLab(ids, "new")
  })
const activeScan = () => kick("active", () => runLab(activeList(), "active"))
const dailyScan = () => kick("daily", () => runLab(limitList(), "daily"))
const stopScan = () => { if (state.scanning) { state.stopFlag = true; note("stop requested") } }

// boot: child needs 1-2 min; retry the first scan until the catalog flows
let bootTries = 0
const bootScan = () => {
  bootTries++
  if (bootTries > 20) return note("boot gave up — use Full Rescan")
  kick("full", async () => {
    const ids = await listIds()
    if (!ids.length) {
      note(`boot try ${bootTries}: child not ready — retry in 45s`)
      save()
      setTimeout(bootScan, 45_000)
      return
    }
    await runLab(ids, "full")
  })
}

// ── payloads ───────────────────────────────────────────────────────
const counts = () => {
  const c = {}
  for (const [, v] of Object.entries(state.models)) c[v.status] = (c[v.status] || 0) + 1
  return c
}

const detailList = () =>
  Object.entries(state.models).map(([id, v]) => ({
    id,
    name: prettyName(id),
    status: v.status,
    label: v.label || "",
    latency: v.latency || null,
    reply: v.reply || "",
    error: v.error || "",
    lastChecked: v.lastChecked || 0,
  }))

const activeModelsPayload = () => {
  if (Date.now() - state.updatedAt > ACTIVE_EVERY_MS && !state.scanning) activeScan()
  const entries = Object.entries(state.models)
  const active = entries.filter(([, v]) => v.status === "active")
    .map(([id]) => ({ id, name: prettyName(id), status: "active" }))
  const limited = entries.filter(([, v]) => v.status === "daily-limit")
    .map(([id, v]) => ({ id, name: `${prettyName(id)} · ⏳ ${v.label}`, status: "daily-limit" }))
  const image = entries.filter(([, v]) => v.status === "image")
    .map(([id]) => ({ id, name: `${prettyName(id)} · 🎨 image`, status: "image" }))
  return { updatedAt: state.updatedAt, scanning: state.scanning, counts: counts(), models: [...active, ...limited], image }
}

const zyvoConfigPayload = (host) => {
  const entries = Object.entries(state.models)
    .filter(([, v]) => ["active", "daily-limit", "image"].includes(v.status))
  if (!entries.length) return null
  const models = {}
  for (const [id, v] of entries) {
    const name = v.status === "daily-limit" ? `${prettyName(id)} · ⏳ ${v.label}` : prettyName(id)
    models[`omniroute/${id}`] = { name }
  }
  const first = entries.find(([, v]) => v.status === "active")
  return {
    $schema: "https://opencode.ai/config.json",
    model: `zyvo/omniroute/${first ? first[0] : entries[0][0]}`,
    provider: {
      zyvo: {
        name: "Zyvo",
        npm: "@ai-sdk/openai-compatible",
        options: { baseURL: `https://${host}/v1`, apiKey: process.env.OMNIROUTE_API_KEY || "" },
        models,
      },
    },
  }
}

// ── GUI ────────────────────────────────────────────────────────────
const GUI_HTML = `<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🤖 AI Model Tester Pro — Auto Fetch + Bulk Test</title>
<style>
  :root {
    --bg:#0a0e14; --card:#11161f; --card2:#161d29; --border:#1e2735;
    --blue:#4d9fff; --green:#3ddc84; --red:#ff5c6c; --yellow:#ffc94d; --purple:#a06bff;
    --text:#e8eef7; --muted:#7d8ba0;
  }
  * { margin:0; padding:0; box-sizing:border-box; font-family:'Segoe UI',Tahoma,sans-serif; }
  body { background:var(--bg); color:var(--text); min-height:100vh; }

  /* ===== Header ===== */
  .header {
    background:linear-gradient(135deg,#0f1724 0%,#16213a 100%);
    border-bottom:1px solid var(--border); padding:25px 20px; text-align:center;
  }
  .header h1 { font-size:30px; background:linear-gradient(90deg,#4d9fff,#a06bff); -webkit-background-clip:text; background-clip:text; color:transparent; }
  .header p { color:var(--muted); margin-top:6px; font-size:14px; }
  .container { max-width:1200px; margin:0 auto; padding:25px 20px 60px; }

  /* ===== Cards ===== */
  .card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:24px; margin-bottom:22px; box-shadow:0 4px 20px rgba(0,0,0,.3); }
  .card-title { font-size:16px; font-weight:bold; color:var(--blue); margin-bottom:16px; display:flex; align-items:center; gap:8px; }
  .card-title::before { content:''; width:4px; height:18px; background:var(--blue); border-radius:2px; }

  label { display:block; color:var(--muted); font-weight:bold; margin-bottom:6px; font-size:13px; }
  input[type=text], input[type=password], textarea, select {
    width:100%; background:var(--bg); border:1px solid var(--border); color:var(--text);
    border-radius:10px; padding:12px 14px; font-size:14px; margin-bottom:14px; outline:none; transition:border .2s;
  }
  input:focus, textarea:focus { border-color:var(--blue); box-shadow:0 0 0 3px rgba(77,159,255,.12); }
  textarea { min-height:140px; resize:vertical; font-family:'Consolas',monospace; font-size:13px; }

  .row { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; }

  /* ===== Buttons ===== */
  .btn { border:none; border-radius:10px; padding:12px 26px; font-size:15px; font-weight:bold; cursor:pointer; transition:.2s; color:#fff; }
  .btn:hover:not(:disabled) { transform:translateY(-2px); box-shadow:0 6px 16px rgba(0,0,0,.35); }
  .btn:disabled { opacity:.4; cursor:not-allowed; }
  .btn-primary { background:linear-gradient(135deg,#1f6feb,#4d9fff); }
  .btn-fetch { background:linear-gradient(135deg,#7b3ff2,#a06bff); }
  .btn-stop { background:linear-gradient(135deg,#d32f3f,#ff5c6c); }
  .btn-export { background:linear-gradient(135deg,#0d7a4f,#3ddc84); color:#04120a; }
  .btn-copy { background:linear-gradient(135deg,#8a6d1a,#ffc94d); color:#1a1400; }
  .btn-group { display:flex; gap:10px; flex-wrap:wrap; margin-top:6px; }

  /* ===== Server scanner card ===== */
  .srv-badge { padding:4px 14px; border-radius:20px; font-size:12px; font-weight:bold; display:inline-block; }
  .srv-on { background:rgba(61,220,132,.12); color:var(--green); border:1px solid rgba(61,220,132,.3); }
  .srv-idle { background:rgba(125,139,160,.12); color:var(--muted); border:1px solid var(--border); }
  .srv-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); gap:10px; margin-bottom:14px; }
  .srv-cell { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:10px; text-align:center; }
  .srv-cell b { font-size:20px; display:block; }
  .srv-cell span { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
  .now-test { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:10px 13px; font-family:Consolas,monospace; font-size:12px; color:var(--muted); margin-bottom:14px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .now-test b { color:var(--green); }

  /* ===== Stats ===== */
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:14px; margin-bottom:20px; }
  .stat { background:var(--card2); border:1px solid var(--border); border-radius:14px; padding:18px 12px; text-align:center; position:relative; overflow:hidden; }
  .stat::after { content:''; position:absolute; top:0; left:0; right:0; height:3px; }
  .stat.c-total::after { background:var(--blue); } .stat.c-ok::after { background:var(--green); }
  .stat.c-fail::after { background:var(--red); } .stat.c-run::after { background:var(--purple); }
  .stat.c-wait::after { background:var(--yellow); }
  .stat .num { font-size:30px; font-weight:800; }
  .stat .lbl { font-size:12px; color:var(--muted); margin-top:4px; }

  /* ===== Progress ===== */
  .progress-wrap { background:var(--card2); border:1px solid var(--border); border-radius:12px; padding:14px 18px; margin-bottom:20px; }
  .progress-info { display:flex; justify-content:space-between; font-size:13px; color:var(--muted); margin-bottom:8px; }
  .progress-bar { height:10px; background:var(--bg); border-radius:6px; overflow:hidden; }
  .progress-fill { height:100%; width:0%; background:linear-gradient(90deg,#1f6feb,#a06bff); border-radius:6px; transition:width .35s; }

  /* ===== Table ===== */
  .filter-btns { display:flex; gap:8px; margin-bottom:14px; flex-wrap:wrap; }
  .fbtn { background:var(--card2); color:var(--muted); border:1px solid var(--border); padding:7px 18px; border-radius:24px; font-size:13px; cursor:pointer; transition:.2s; }
  .fbtn:hover { color:var(--text); border-color:var(--blue); }
  .fbtn.active { background:var(--blue); color:#fff; border-color:var(--blue); }

  .tbl-wrap { max-height:560px; overflow:auto; border:1px solid var(--border); border-radius:14px; background:var(--card); }
  .tbl-wrap::-webkit-scrollbar { width:8px; height:8px; }
  .tbl-wrap::-webkit-scrollbar-thumb { background:var(--border); border-radius:4px; }
  table { width:100%; border-collapse:collapse; font-size:13px; min-width:700px; }
  th { background:var(--card2); color:var(--blue); padding:13px 12px; text-align:left; border-bottom:2px solid var(--border); position:sticky; top:0; z-index:2; font-size:12px; text-transform:uppercase; letter-spacing:.5px; }
  td { padding:11px 12px; border-bottom:1px solid var(--border); }
  tbody tr { transition:background .15s; }
  tbody tr:hover { background:rgba(77,159,255,.06); }
  tr.row-ok { border-left:3px solid var(--green); }
  tr.row-fail { border-left:3px solid var(--red); }
  tr.row-run { border-left:3px solid var(--purple); background:rgba(160,107,255,.05); }

  .badge { padding:4px 12px; border-radius:20px; font-size:12px; font-weight:bold; display:inline-block; }
  .badge-ok { background:rgba(61,220,132,.12); color:var(--green); border:1px solid rgba(61,220,132,.3); }
  .badge-fail { background:rgba(255,92,108,.12); color:var(--red); border:1px solid rgba(255,92,108,.3); }
  .badge-wait { background:rgba(255,201,77,.12); color:var(--yellow); border:1px solid rgba(255,201,77,.3); }
  .badge-run { background:rgba(160,107,255,.12); color:var(--purple); border:1px solid rgba(160,107,255,.3); }
  .badge-free { background:rgba(77,159,255,.12); color:var(--blue); border:1px solid rgba(77,159,255,.3); font-size:10px; padding:2px 8px; margin-left:6px; }

  .snippet { max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--muted); font-family:monospace; font-size:12px; }
  .err-icon { color:var(--red); cursor:help; }
  .model-name { font-weight:600; font-size:13px; }
  .latency-fast { color:var(--green); font-weight:bold; }
  .latency-mid { color:var(--yellow); font-weight:bold; }
  .latency-slow { color:var(--red); font-weight:bold; }

  .note { background:rgba(255,201,77,.07); border:1px solid rgba(255,201,77,.25); border-radius:10px; padding:13px 16px; font-size:13px; color:var(--yellow); margin-bottom:16px; }
  .log-box { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:12px; font-family:monospace; font-size:12px; color:var(--muted); max-height:130px; overflow-y:auto; margin-top:14px; }
  .log-box div { padding:2px 0; }
  .log-ok { color:var(--green); } .log-fail { color:var(--red); } .log-info { color:var(--blue); }
  .empty-state { text-align:center; color:var(--muted); padding:40px; }

  /* ===== Answer modal (উত্তর দেখার জায়গা) ===== */
  .ov { display:none; position:fixed; inset:0; background:rgba(0,0,0,.78); z-index:50; padding:20px; overflow-y:auto; }
  .ov.open { display:block; }
  .modal { background:var(--card); border:1px solid var(--border); border-radius:16px; max-width:680px; margin:50px auto; padding:22px; }
  .modal h3 { color:var(--blue); font-size:16px; margin-bottom:14px; word-break:break-all; }
  .closex { float:right; background:none; border:none; color:var(--muted); font-size:20px; cursor:pointer; }
  .mrow { display:flex; gap:10px; margin-bottom:9px; font-size:13px; }
  .mrow .k { color:var(--muted); min-width:120px; flex-shrink:0; }
  .mrow .v { word-break:break-all; }
  .ansbox { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:13px; font-family:Consolas,monospace; font-size:12.5px; white-space:pre-wrap; word-break:break-word; max-height:280px; overflow-y:auto; color:var(--text); }
</style>
<base target="_blank">
</head>
<body>

<div class="header">
  <h1>🤖 AI Model Tester Pro</h1>
  <p>Auto-fetch সব মডেল → একটা একটা করে গভীর টেস্ট → লাইভ রিপোর্ট (server scanner সহ)</p>
</div>

<div class="container">

  <!-- 🖥️ SERVER SCANNER (live) -->
  <div class="card">
    <div class="card-title">🖥️ Server Scanner — লাইভ অবস্থা</div>
    <div style="margin-bottom:14px">
      <span id="srvBadge" class="srv-badge srv-idle">লোড হচ্ছে…</span>
    </div>
    <div class="now-test">🔬 এখন পরীক্ষা চলছে: <b id="srvNow">—</b></div>
    <div class="srv-grid">
      <div class="srv-cell"><b id="svProbed">0</b><span>পরীক্ষিত</span></div>
      <div class="srv-cell"><b id="svTotal">0</b><span>মোট</span></div>
      <div class="srv-cell"><b id="svActive" style="color:var(--green)">0</b><span>✓ Active</span></div>
      <div class="srv-cell"><b id="svLimit" style="color:var(--yellow)">0</b><span>⏳ Limit</span></div>
    </div>
    <div class="btn-group">
      <button class="btn btn-fetch" onclick="srvGo('/scan/new')">🆕 নতুন model scan</button>
      <button class="btn btn-primary" onclick="srvGo('/scan/full')">🔄 Full Rescan</button>
      <button class="btn btn-stop" onclick="srvGo('/scan/stop')">⏹️ থামাও</button>
    </div>
  </div>

  <!-- ⚙️ SETTINGS -->
  <div class="card">
    <div class="card-title">⚙️ সেটআপ</div>
    <div class="note">✅ <b>Key ও Base URL auto-load হয়</b> এই gateway থেকে — সরাসরি "মডেল লিস্ট আনো" চাপলেই হবে। ব্রাউজার থেকে টেস্ট করলে তা <b>server scanner-এর রিপোর্টেও যোগ হবে না</b> — শুধু নিজে দেখার জন্য।</div>

    <label>🔑 API Key (auto)</label>
    <input type="password" id="apiKey" placeholder="auto-load হবে…">
    <div class="key-warn">🔒 Key শুধু ব্রাউজারেই থাকবে — কোনো সার্ভারে পাঠানো হচ্ছে না</div>

    <div class="row">
      <div>
        <label>🌐 Base URL</label>
        <input type="text" id="baseUrl" value="">
      </div>
      <div>
        <label>⏱️ Timeout (সেকেন্ড)</label>
        <input type="text" id="timeout" value="30">
      </div>
      <div>
        <label>🔢 Concurrent</label>
        <select id="conc">
          <option value="1" selected>১টা করে (গভীর টেস্ট)</option>
          <option value="3">৩টা করে</option>
          <option value="5">৫টা করে</option>
          <option value="8">৮টা করে (দ্রুত)</option>
        </select>
      </div>
    </div>

    <div class="btn-group">
      <button class="btn btn-fetch" id="btnFetch" onclick="fetchModels()">📡 মডেল লিস্ট আনো (Auto Fetch)</button>
      <button class="btn btn-primary" id="btnStart" onclick="startTest()">▶️ সব টেস্ট করো</button>
      <button class="btn btn-stop" id="btnStop" onclick="stopTest()" disabled>⏹️ থামাও</button>
    </div>
    <div class="btn-group" style="margin-top:10px;">
      <button class="btn btn-export" onclick="exportCSV()">📥 CSV ডাউনলোড</button>
      <button class="btn btn-copy" onclick="copyOk()">📋 Active কপি</button>
      <button class="btn btn-copy" style="background:linear-gradient(135deg,#5a3ea8,#8f6fff);color:#fff" onclick="copyFail()">📋 Fail লিস্ট কপি</button>
      <button class="btn" style="background:var(--card2);border:1px solid var(--border);color:var(--muted)" onclick="clearAll()">🗑️ Clear</button>
    </div>
    <div class="log-box" id="logBox"><div class="log-info">📝 লগ রেডি... "মডেল লিস্ট আনো" চাপুন</div></div>
  </div>

  <!-- 📊 STATS -->
  <div class="stats" id="statsBar" style="display:none;">
    <div class="stat c-total"><div class="num" id="stTotal" style="color:var(--blue)">0</div><div class="lbl">মোট মডেল</div></div>
    <div class="stat c-ok"><div class="num" id="stOk" style="color:var(--green)">0</div><div class="lbl">✅ Active</div></div>
    <div class="stat c-fail"><div class="num" id="stFail" style="color:var(--red)">0</div><div class="lbl">❌ Fail</div></div>
    <div class="stat c-run"><div class="num" id="stRun" style="color:var(--purple)">0</div><div class="lbl">⏳ চলমান</div></div>
    <div class="stat c-wait"><div class="num" id="stWait" style="color:var(--yellow)">0</div><div class="lbl">🕐 বাকি</div></div>
  </div>

  <!-- 📈 PROGRESS -->
  <div class="progress-wrap" id="progressWrap" style="display:none;">
    <div class="progress-info">
      <span id="progText">টেস্ট চলছে...</span>
      <span id="progPct">0%</span>
    </div>
    <div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div>
  </div>

  <!-- 📋 MODEL LIST -->
  <div class="card">
    <div class="card-title">📋 মডেল লিস্ট (<span id="modelCount">0</span> টা)</div>
    <textarea id="models" placeholder="'📡 মডেল লিস্ট আনো' চাপলে এখানে সব মডেল অটো আসবে... অথবা নিজে লিখুন (এক লাইনে একটা)"></textarea>
  </div>

  <!-- 📊 RESULTS -->
  <div class="filter-btns">
    <button class="fbtn active" onclick="setFilter('all',this)">সব</button>
    <button class="fbtn" onclick="setFilter('ok',this)">✅ Active</button>
    <button class="fbtn" onclick="setFilter('fail',this)">❌ Fail</button>
    <button class="fbtn" onclick="setFilter('run',this)">⏳ চলমান</button>
  </div>

  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>#</th><th>মডেল</th><th>প্রাইসিং</th><th>স্ট্যাটাস</th><th>⚡ Latency</th><th>Context</th><th>উত্তর</th>
      </tr></thead>
      <tbody id="tbody">
        <tr><td colspan="7" class="empty-state">🔍 এখনো কিছু হয়নি — "মডেল লিস্ট আনো" চাপুন</td></tr>
      </tbody>
    </table>
  </div>
</div>

<!-- উত্তর দেখার modal -->
<div class="ov" id="ov" onclick="if(event.target===this)closeM()">
  <div class="modal">
    <button class="closex" onclick="closeM()">✕</button>
    <h3 id="mName">—</h3>
    <div class="mrow"><span class="k">স্ট্যাটাস</span><span class="v" id="mStatus">—</span></div>
    <div class="mrow"><span class="k">⚡ Latency</span><span class="v" id="mLat">—</span></div>
    <div class="mrow"><span class="k">ℹ️ তথ্য</span><span class="v" id="mLabel">—</span></div>
    <div class="mrow"><span class="k">🕐 শেষ পরীক্ষা</span><span class="v" id="mTime">—</span></div>
    <div class="mrow"><span class="k">💬 পূর্ণ উত্তর</span></div>
    <div class="ansbox" id="mAns">—</div>
  </div>
</div>

<script>
let results = [], running = false, filter = 'all', modelMeta = {};

/* ===== LOG ===== */
function log(msg, cls) {
  const box = document.getElementById('logBox');
  const d = document.createElement('div');
  d.className = cls || 'log-info';
  d.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
  box.appendChild(d);
  box.scrollTop = box.scrollHeight;
}

/* ===== SERVER SCANNER (live) ===== */
async function srvTick() {
  try {
    const d = await (await fetch('/scan/results')).json()
    const c = d.counts || {}
    const b = document.getElementById('srvBadge')
    if (d.scanning) { b.textContent = '🟢 SCANNING: ' + d.scanning.toUpperCase(); b.className = 'srv-badge srv-on' }
    else { b.textContent = '⚪ IDLE'; b.className = 'srv-badge srv-idle' }
    document.getElementById('srvNow').textContent = d.nowTesting || '—'
    document.getElementById('svProbed').textContent = d.runDone ?? d.detail.length
    document.getElementById('svTotal').textContent = d.runTotal || d.detail.length
    document.getElementById('svActive').textContent = c.active || 0
    document.getElementById('svLimit').textContent = c['daily-limit'] || 0
  } catch(e) {}
}
async function srvGo(u) { await fetch(u, {method:'POST'}); setTimeout(srvTick, 600); setTimeout(srvTick, 2500) }
srvTick(); setInterval(srvTick, 4000)

/* ===== AUTO-LOAD CREDENTIALS ===== */
(async function autoCreds() {
  try {
    const cfg = await (await fetch('/zyvo-config')).json()
    const z = cfg.provider && cfg.provider.zyvo
    if (z && z.options) {
      document.getElementById('apiKey').value = z.options.apiKey || ''
      document.getElementById('baseUrl').value = z.options.baseURL || ''
      log('✅ Key ও Base URL auto-load হয়েছে — সরাসরি "মডেল লিস্ট আনো" চাপতে পারো', 'log-ok')
    }
  } catch(e) {}
})()

/* ===== AUTO FETCH MODELS ===== */
async function fetchModels() {
  const key = document.getElementById('apiKey').value.trim();
  const base = document.getElementById('baseUrl').value.trim().replace(/\/$/,'');
  if (!key) { alert('⚠️ প্রথমে API Key দিন!'); return; }
  const btn = document.getElementById('btnFetch');
  btn.disabled = true; btn.textContent = '⏳ লোড হচ্ছে...';
  log('API থেকে মডেল লিস্ট আনা হচ্ছে...', 'log-info');
  try {
    const res = await fetch(base + '/models', { headers: { 'Authorization': 'Bearer ' + key } });
    const data = await res.json();
    if (!res.ok) throw new Error((data.error && data.error.message) || ('HTTP ' + res.status));
    const models = data.data || [];
    log(models.length + ' টা মডেল পাওয়া গেছে!', 'log-ok');

    modelMeta = {};
    results = [];
    document.getElementById('models').value = '';
    let lines = [];
    models.forEach(m => {
      const id = m.id;
      modelMeta[id] = m;
      lines.push(id);
    });
    document.getElementById('models').value = lines.join('\n');
    document.getElementById('modelCount').textContent = models.length;
    render();
    log('✅ লিস্ট রেডি — এবার "▶️ সব টেস্ট করো" চাপুন', 'log-ok');
  } catch(e) {
    log('❌ Fetch fail: ' + e.message, 'log-fail');
    alert('❌ মডেল আনা যায়নি: ' + e.message);
  } finally {
    btn.disabled = false; btn.textContent = '📡 মডেল লিস্ট আনো (Auto Fetch)';
  }
}

/* ===== TEST ===== */
function getModels() {
  return document.getElementById('models').value.split('\n').map(s=>s.trim()).filter(Boolean);
}

function pricingInfo(id) {
  const m = modelMeta[id];
  if (!m || !m.pricing) return '—';
  const p = m.pricing;
  const inP = parseFloat(p.prompt || 0), outP = parseFloat(p.completion || 0);
  if (inP === 0 && outP === 0) return '🆓 FREE';
  return '$' + inP.toFixed(2) + '/M';
}

function contextLen(id) {
  const m = modelMeta[id];
  if (!m || !m.context_length) return '—';
  const c = m.context_length;
  return c >= 1e6 ? (c/1e6).toFixed(0)+'M' : c >= 1e3 ? (c/1e3).toFixed(0)+'K' : c;
}

async function testOne(model, key, url, timeoutSec) {
  const t0 = performance.now();
  const ctrl = new AbortController();
  const timer = setTimeout(()=>ctrl.abort(), timeoutSec*1000);
  try {
    const res = await fetch(url, {
      method:'POST',
      headers: { 'Authorization':'Bearer '+key, 'Content-Type':'application/json' },
      body: JSON.stringify({ model: model, messages: [{role:'user', content:'Reply with exactly one word: OK'}], max_tokens: 10 }),
      signal: ctrl.signal
    });
    const data = await res.json();
    const lat = Math.round(performance.now()-t0);
    if (!res.ok) throw new Error((data.error&&data.error.message)||('HTTP '+res.status));
    const reply = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content || '';
    if (!reply.trim()) throw new Error('খালি রেসপন্স');
    return { status:'ok', latency:lat, reply:reply.trim().slice(0,150) };
  } catch(e) {
    const lat = Math.round(performance.now()-t0);
    return { status:'fail', latency:lat, error: e.name==='AbortError' ? 'Timeout' : e.message };
  } finally { clearTimeout(timer); }
}

async function startTest() {
  const key = document.getElementById('apiKey').value.trim();
  if (!key) { alert('⚠️ প্রথমে API Key দিন!'); return; }
  const base = document.getElementById('baseUrl').value.trim().replace(/\/$/,'');
  const timeoutSec = parseInt(document.getElementById('timeout').value)||30;
  const conc = parseInt(document.getElementById('conc').value)||3;
  const models = getModels();
  if (!models.length) { alert('⚠️ মডেল লিস্ট খালি! আগে "মডেল লিস্ট আনো" চাপুন'); return; }

  running = true;
  document.getElementById('btnStart').disabled = true;
  document.getElementById('btnStop').disabled = false;
  document.getElementById('statsBar').style.display = 'grid';
  document.getElementById('progressWrap').style.display = 'block';

  results = models.map(m => ({ model:m, status:'wait', latency:null, reply:'', error:'' }));
  render();
  log('🚀 টেস্ট শুরু — ' + models.length + ' টা মডেল, concurrent: ' + conc, 'log-info');

  let idx = 0;
  async function worker() {
    while (running && idx < models.length) {
      const i = idx++;
      const r = results.find(x=>x.model===models[i]);
      r.status = 'run'; render();
      const out = await testOne(models[i], key, base + '/chat/completions', timeoutSec);
      Object.assign(r, out); render();
      if (out.status==='ok') log('✅ ' + models[i] + ' (' + out.latency + 'ms)', 'log-ok');
      else log('❌ ' + models[i] + ' — ' + out.error, 'log-fail');
    }
  }
  await Promise.all(Array.from({length:conc}, worker));

  running = false;
  document.getElementById('btnStart').disabled = false;
  document.getElementById('btnStop').disabled = true;
  const ok = results.filter(r=>r.status==='ok').length;
  const failList = results.filter(r=>r.status==='fail');
  log('🏁 শেষ! ✅ Active: ' + ok + ' / ❌ Fail: ' + failList.length, 'log-info');
  if (ok > 0) {
    const sorted = [...results].filter(r=>r.status==='ok').sort((a,b)=>a.latency-b.latency);
    log('⚡ সবচেয়ে ফাস্ট: ' + sorted[0].model + ' (' + sorted[0].latency + 'ms)', 'log-ok');
  }
  alert('🏁 টেস্ট শেষ!\n✅ Active: ' + ok + '\n❌ Fail: ' + (results.length - ok));
}

function stopTest(){ running = false; log('⏹️ থামানো হলো', 'log-info'); }

/* ===== RENDER ===== */
function esc(s){ const d=document.createElement('div'); d.textContent=String(s||''); return d.innerHTML; }

function render() {
  const tb = document.getElementById('tbody');
  const rows = results.filter(r => filter==='all' || r.status===filter);
  if (!rows.length) {
    tb.innerHTML = '<tr><td colspan="7" class="empty-state">' + (results.length ? 'এই ফিল্টারে কিছু নেই' : '🔍 মডেল লিস্ট আনুন') + '</td></tr>';
  } else {
    tb.innerHTML = rows.map((r,i) => {
      let badge, rowCls;
      if (r.status==='ok') { badge='<span class="badge badge-ok">✅ Active</span>'; rowCls='row-ok'; }
      else if (r.status==='fail') { badge='<span class="badge badge-fail">❌ Fail</span>'; rowCls='row-fail'; }
      else if (r.status==='run') { badge='<span class="badge badge-run">⏳ চলছে...</span>'; rowCls='row-run'; }
      else { badge='<span class="badge badge-wait">🕐 অপেক্ষা</span>'; rowCls=''; }
      let latHtml = '—';
      if (r.latency != null) {
        const cls = r.latency < 2000 ? 'latency-fast' : r.latency < 8000 ? 'latency-mid' : 'latency-slow';
        latHtml = '<span class="' + cls + '">' + r.latency + ' ms</span>';
      }
      const free = pricingInfo(r.model) === '🆓 FREE' ? '<span class="badge-free">FREE</span>' : '';
      const err = r.error ? ' <span class="err-icon" title="' + esc(r.error) + '">⚠️</span>' : '';
      const meta = modelMeta[r.model];
      const desc = meta && meta.description ? ' title="' + esc(meta.description.slice(0,120)) + '"' : '';
      return '<tr class="' + rowCls + '">' +
        '<td>' + (i+1) + '</td>' +
        '<td class="model-name"' + desc + '>' + esc(r.model) + free + '</td>' +
        '<td>' + pricingInfo(r.model) + '</td>' +
        '<td>' + badge + err + '</td>' +
        '<td>' + latHtml + '</td>' +
        '<td>' + contextLen(r.model) + '</td>' +
        '<td class="snippet" title="' + esc(r.reply||'') + '">' + esc(r.reply||'—') + '</td></tr>';
    }).join('');
  }
  updateStats();
}

function updateStats() {
  document.getElementById('stTotal').textContent = results.length;
  document.getElementById('stOk').textContent = results.filter(r=>r.status==='ok').length;
  document.getElementById('stFail').textContent = results.filter(r=>r.status==='fail').length;
  document.getElementById('stRun').textContent = results.filter(r=>r.status==='run').length;
  document.getElementById('stWait').textContent = results.filter(r=>r.status==='wait').length;
  const done = results.filter(r=>r.status==='ok'||r.status==='fail').length;
  const pct = results.length ? Math.round(done/results.length*100) : 0;
  document.getElementById('progressFill').style.width = pct + '%';
  document.getElementById('progPct').textContent = pct + '%';
  document.getElementById('progText').textContent = 'টেস্ট হচ্ছে: ' + done + '/' + results.length;
}

function setFilter(f, btn){
  filter = f;
  document.querySelectorAll('.fbtn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  render();
}

/* ===== EXPORT / COPY ===== */
function exportCSV(){
  if(!results.length){ alert('কোনো ডেটা নেই'); return; }
  let csv = 'Model,Pricing,Status,Latency_ms,Reply,Error\n';
  results.forEach(r=>{
    csv += '"'+r.model+'","'+pricingInfo(r.model)+'","'+r.status+'","'+(r.latency||'')+'","'+(r.reply||'').replace(/"/g,'""')+'","'+(r.error||'').replace(/"/g,'""')+'"\n';
  });
  const blob = new Blob([csv], {type:'text/csv;charset=utf-8'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob); a.download = 'model_test_' + new Date().toISOString().slice(0,19).replace(/[:T]/g,'-') + '.csv'; a.click();
}

function copyOk(){
  const ok = results.filter(r=>r.status==='ok').map(r=>r.model);
  if(!ok.length){ alert('কোনো active মডেল নেই'); return; }
  navigator.clipboard.writeText(ok.join('\n')).then(()=>alert('📋 ' + ok.length + ' টা active মডেল কপি হয়েছে!'));
}
function copyFail(){
  const fail = results.filter(r=>r.status==='fail').map(r=>r.model + '  [' + (r.error||'fail') + ']');
  if(!fail.length){ alert('কোনো fail নেই 🎉'); return; }
  navigator.clipboard.writeText(fail.join('\n')).then(()=>alert('📋 ' + fail.length + ' টা fail মডেল কপি হয়েছে!'));
}
function clearAll(){
  results = []; modelMeta = {};
  document.getElementById('models').value = '';
  document.getElementById('modelCount').textContent = '0';
  document.getElementById('statsBar').style.display = 'none';
  document.getElementById('progressWrap').style.display = 'none';
  render();
}
</script>
</body>
</html>
`

// ── http server: scanner routes + proxy ────────────────────────────
const send = (res, code, obj) => {
  res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" })
  res.end(JSON.stringify(obj))
}

const server = http.createServer((req, res) => {
  const u = req.url || "/"
  if (req.method === "GET" && (u === "/scan" || u === "/scan/")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" })
    return res.end(GUI_HTML)
  }
  if (req.method === "GET" && u.startsWith("/scan/results"))
    return send(res, 200, {
      scanning: state.scanning,
      nowTesting: state.nowTesting,
      runTotal: state.runTotal,
      runDone: state.runDone,
      updatedAt: state.updatedAt,
      counts: counts(),
      log: state.log.slice(0, 40),
      detail: detailList(),
    })
  if (req.method === "GET" && u.startsWith("/scan/status"))
    return send(res, 200, { scanning: state.scanning, counts: counts(), models: Object.keys(state.models).length, updatedAt: state.updatedAt, log: state.log.slice(0, 20) })
  if (req.method === "GET" && u.startsWith("/active-models"))
    return send(res, 200, activeModelsPayload())
  if (req.method === "GET" && u.startsWith("/zyvo-config")) {
    const host = req.headers.host || ""
    const cfg = zyvoConfigPayload(host)
    return cfg ? send(res, 200, cfg) : send(res, 503, { error: "no active models yet — run a scan first" })
  }
  if (req.method === "POST" && u.startsWith("/scan/full")) { if (!state.scanning) fullScan(); return send(res, 202, { started: !state.scanning ? "full" : "busy" }) }
  if (req.method === "POST" && u.startsWith("/scan/new")) { if (!state.scanning) newScan(); return send(res, 202, { started: !state.scanning ? "new" : "busy" }) }
  if (req.method === "POST" && u.startsWith("/scan/stop")) { stopScan(); return send(res, 200, { stopping: true }) }
  return proxy(req, res)
})

const proxy = (req, res) => {
  const isBodyful = req.method === "POST" || req.method === "PUT" || req.method === "PATCH"
  if (!isBodyful) {
    const up = http.request(
      { host: UPSTREAM_HOST, port: UPSTREAM_PORT, path: req.url, method: req.method, headers: { ...req.headers, host: `${UPSTREAM_HOST}:${UPSTREAM_PORT}` } },
      (ur) => { res.writeHead(ur.statusCode, ur.headers); ur.pipe(res) }
    )
    up.on("error", () => {
      if (!res.headersSent) res.writeHead(502, { "Content-Type": "application/json" })
      res.end(JSON.stringify({ error: "omniroute upstream not ready" }))
    })
    return req.pipe(up)
  }
  const chunks = []
  req.on("data", (c) => chunks.push(c))
  req.on("end", () => {
    let body = Buffer.concat(chunks)
    if (req.url.startsWith("/v1/")) body = stripPrefix(body)
    const headers = { ...req.headers }
    headers["content-length"] = String(body.length)
    delete headers["transfer-encoding"]
    const up = http.request(
      { host: UPSTREAM_HOST, port: UPSTREAM_PORT, path: req.url, method: req.method, headers },
      (ur) => { res.writeHead(ur.statusCode, ur.headers); ur.pipe(res) }
    )
    up.on("error", () => {
      if (!res.headersSent) res.writeHead(502, { "Content-Type": "application/json" })
      res.end(JSON.stringify({ error: "omniroute upstream not ready" }))
    })
    up.end(body)
  })
}

// zyvo writes model ids as "omniroute/<real-id>" so opencode keeps them under
// the Zyvo provider — strip that prefix before they reach OmniRoute.
const stripPrefix = (raw) => {
  try {
    const j = JSON.parse(raw.toString("utf8"))
    if (typeof j.model === "string" && j.model.startsWith("omniroute/")) {
      j.model = j.model.slice("omniroute/".length)
      return Buffer.from(JSON.stringify(j))
    }
  } catch {}
  return raw
}

load()
server.listen(PORT, () => console.log(`[lab] control center on :${PORT}, omniroute on :${UPSTREAM_PORT}`))

setTimeout(bootScan, 60_000)
setInterval(newScan, NEW_EVERY_MS)
setTimeout(newScan, 5 * 60 * 1000)
setInterval(activeScan, ACTIVE_EVERY_MS)
setInterval(dailyScan, LIMIT_EVERY_MS)
setInterval(fullScan, FULL_EVERY_MS)
