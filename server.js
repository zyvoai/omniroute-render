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
const GUI_HTML = `<!doctype html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ZYVO · Model Control Center</title>
<style>
  :root {
    --bg:#0a0e14; --card:#11161f; --card2:#161d29; --border:#1e2735;
    --blue:#4d9fff; --green:#3ddc84; --red:#ff5c6c; --yellow:#ffc94d; --purple:#a06bff;
    --text:#e8eef7; --muted:#7d8ba0;
  }
  * { margin:0; padding:0; box-sizing:border-box; font-family:'Segoe UI',Tahoma,sans-serif; }
  body { background:var(--bg); color:var(--text); min-height:100vh; }
  .header {
    background:linear-gradient(135deg,#0f1724 0%,#16213a 100%);
    border-bottom:1px solid var(--border); padding:22px 20px; text-align:center;
  }
  .header h1 { font-size:28px; background:linear-gradient(90deg,#4d9fff,#a06bff); -webkit-background-clip:text; background-clip:text; color:transparent; }
  .header p { color:var(--muted); margin-top:5px; font-size:13px; }
  .header .live { margin-top:10px; }
  .container { max-width:1100px; margin:0 auto; padding:22px 16px 60px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:20px; margin-bottom:18px; box-shadow:0 4px 20px rgba(0,0,0,.3); }
  .card-title { font-size:15px; font-weight:bold; color:var(--blue); margin-bottom:14px; display:flex; align-items:center; gap:8px; }
  .card-title::before { content:''; width:4px; height:16px; background:var(--blue); border-radius:2px; }
  .btns { display:flex; gap:10px; flex-wrap:wrap; }
  .btn { border:none; border-radius:10px; padding:12px 22px; font-size:14px; font-weight:bold; cursor:pointer; transition:.2s; color:#fff; }
  .btn:hover { transform:translateY(-2px); box-shadow:0 6px 16px rgba(0,0,0,.35); }
  .btn:disabled { opacity:.45; cursor:not-allowed; transform:none; }
  .b-full { background:linear-gradient(135deg,#1f6feb,#4d9fff); }
  .b-new { background:linear-gradient(135deg,#7b3ff2,#a06bff); }
  .b-stop { background:linear-gradient(135deg,#d32f3f,#ff5c6c); }
  .b-csv { background:linear-gradient(135deg,#0d7a4f,#3ddc84); color:#04120a; }
  .b-copy { background:linear-gradient(135deg,#8a6d1a,#ffc94d); color:#1a1400; }
  .now { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:11px 14px; margin-bottom:14px; font-family:Consolas,monospace; font-size:12px; color:var(--muted); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .now b { color:var(--grn,#3ddc84); }
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:12px; margin-bottom:16px; }
  .stat { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:16px 10px; text-align:center; position:relative; overflow:hidden; }
  .stat::after { content:''; position:absolute; top:0; left:0; right:0; height:3px; }
  .c-tot::after { background:var(--blue); } .c-ok::after { background:var(--green); }
  .c-img::after { background:var(--purple); } .c-lim::after { background:var(--yellow); }
  .c-fail::after { background:var(--red); } .c-run::after { background:#a06bff; }
  .stat .num { font-size:26px; font-weight:800; }
  .stat .lbl { font-size:11px; color:var(--muted); margin-top:3px; }
  .pwrap { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px 16px; margin-bottom:16px; }
  .pinfo { display:flex; justify-content:space-between; font-size:12px; color:var(--muted); margin-bottom:7px; }
  .pbar { height:10px; background:var(--bg); border-radius:6px; overflow:hidden; }
  .pfill { height:100%; width:0%; background:linear-gradient(90deg,#1f6feb,#a06bff); border-radius:6px; transition:width .4s; }
  input[type=text] { width:100%; background:var(--bg); border:1px solid var(--border); color:var(--text); border-radius:10px; padding:11px 13px; font-size:14px; outline:none; margin-bottom:12px; }
  input:focus { border-color:var(--blue); box-shadow:0 0 0 3px rgba(77,159,255,.12); }
  .fbtns { display:flex; gap:8px; margin-bottom:12px; flex-wrap:wrap; }
  .fb { background:var(--card); color:var(--muted); border:1px solid var(--border); padding:7px 18px; border-radius:24px; font-size:13px; cursor:pointer; transition:.2s; }
  .fb:hover { color:var(--text); border-color:var(--blue); }
  .fb.on { background:var(--blue); color:#fff; border-color:var(--blue); }
  .tbl-wrap { max-height:600px; overflow:auto; border:1px solid var(--border); border-radius:14px; background:var(--card); }
  .tbl-wrap::-webkit-scrollbar { width:8px; } .tbl-wrap::-webkit-scrollbar-thumb { background:var(--border); border-radius:4px; }
  table { width:100%; border-collapse:collapse; font-size:13px; min-width:640px; }
  th { background:var(--card2); color:var(--blue); padding:12px 11px; text-align:left; border-bottom:2px solid var(--border); position:sticky; top:0; z-index:2; font-size:11px; text-transform:uppercase; letter-spacing:.5px; }
  td { padding:10px 11px; border-bottom:1px solid var(--border); }
  tbody tr { transition:background .15s; cursor:pointer; }
  tbody tr:hover { background:rgba(77,159,255,.06); }
  tr.r-ok { border-left:3px solid var(--green); } tr.r-fail { border-left:3px solid var(--red); }
  tr.r-run { border-left:3px solid var(--purple); background:rgba(160,107,255,.05); }
  tr.r-img { border-left:3px solid var(--purple); } tr.r-lim { border-left:3px solid var(--yellow); }
  .badge { padding:4px 11px; border-radius:20px; font-size:11px; font-weight:bold; display:inline-block; white-space:nowrap; }
  .b-ok { background:rgba(61,220,132,.12); color:var(--green); border:1px solid rgba(61,220,132,.3); }
  .b-fail { background:rgba(255,92,108,.12); color:var(--red); border:1px solid rgba(255,92,108,.3); }
  .b-lim { background:rgba(255,201,77,.12); color:var(--yellow); border:1px solid rgba(255,201,77,.3); }
  .b-img { background:rgba(160,107,255,.12); color:var(--purple); border:1px solid rgba(160,107,255,.3); }
  .b-run { background:rgba(77,159,255,.12); color:var(--blue); border:1px solid rgba(77,159,255,.3); }
  .lat-f { color:var(--green); font-weight:bold; } .lat-m { color:var(--yellow); font-weight:bold; } .lat-s { color:var(--red); font-weight:bold; }
  .snip { max-width:260px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--muted); font-family:Consolas,monospace; font-size:12px; }
  .mname { font-weight:600; font-size:12px; word-break:break-all; }
  .empty { text-align:center; color:var(--muted); padding:36px; }
  .logbox { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:11px; font-family:Consolas,monospace; font-size:11px; color:var(--muted); max-height:140px; overflow-y:auto; margin-top:12px; }
  .logbox div { padding:2px 0; word-break:break-all; }
  /* modal — উত্তর দেখার জায়গা */
  .ov { display:none; position:fixed; inset:0; background:rgba(0,0,0,.75); z-index:50; padding:20px; overflow-y:auto; }
  .ov.open { display:block; }
  .modal { background:var(--card); border:1px solid var(--border); border-radius:16px; max-width:680px; margin:40px auto; padding:22px; }
  .modal h3 { color:var(--blue); font-size:16px; margin-bottom:14px; word-break:break-all; }
  .mrow { display:flex; gap:10px; margin-bottom:9px; font-size:13px; }
  .mrow .k { color:var(--muted); min-width:110px; }
  .mrow .v { word-break:break-all; }
  .ansbox { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:13px; font-family:Consolas,monospace; font-size:12.5px; white-space:pre-wrap; word-break:break-word; max-height:260px; overflow-y:auto; color:var(--text); }
  .closex { float:right; background:none; border:none; color:var(--muted); font-size:20px; cursor:pointer; }
  .badge { padding:3px 11px; border-radius:20px; font-size:11px; font-weight:bold; display:inline-block; white-space:nowrap; }
</style>
</head>
<body>

<div class="header">
  <h1>🤖 ZYVO · Model Control Center</h1>
  <p>প্রতিটা model একটা একটা করে গভীরভাবে পরীক্ষা — latency, উত্তর, error সব লাইভ</p>
  <div class="live"><span id="badge" class="badge b-run">লোড হচ্ছে…</span></div>
</div>

<div class="container">

  <div class="card">
    <div class="card-title">🎛️ নিয়ন্ত্রণ</div>
    <div class="now">🔬 এখন পরীক্ষা চলছে: <b id="nowT">—</b></div>
    <div class="btns">
      <button class="btn b-new" onclick="go('/scan/new')">🆕 নতুন model scan</button>
      <button class="btn b-full" onclick="go('/scan/full')">🔄 Full Rescan</button>
      <button class="btn b-stop" onclick="go('/scan/stop')">⏹️ থামাও</button>
    </div>
    <div class="btns">
      <button class="btn b-csv" onclick="csv()">📥 CSV ডাউনলোড</button>
      <button class="btn b-copy" onclick="copyList('active')">📋 Active কপি</button>
      <button class="btn b-copy" style="background:linear-gradient(135deg,#5a3ea8,#8f6fff);color:#fff" onclick="copyList('fail')">📋 Fail কপি</button>
    </div>
  </div>

  <div class="stats">
    <div class="stat c-tot"><div class="num" id="c-tot" style="color:var(--blue)">0</div><div class="lbl">মোট মডেল</div></div>
    <div class="stat c-ok"><div class="num" id="c-act" style="color:var(--green)">0</div><div class="lbl">✅ Active</div></div>
    <div class="stat c-img"><div class="num" id="c-img" style="color:var(--purple)">0</div><div class="lbl">🎨 Image</div></div>
    <div class="stat c-lim"><div class="num" id="c-lim" style="color:var(--yellow)">0</div><div class="lbl">⏳ Limit</div></div>
    <div class="stat c-fail"><div class="num" id="c-fail" style="color:var(--red)">0</div><div class="lbl">❌ সমস্যা</div></div>
    <div class="stat c-run"><div class="num" id="c-left" style="color:#a06bff">0</div><div class="lbl">🕐 বাকি</div></div>
  </div>

  <div class="pwrap">
    <div class="pinfo"><span id="pinfo">—</span><span id="pct">0%</span></div>
    <div class="pbar"><div class="pfill" id="pfill"></div></div>
  </div>

  <input type="text" id="q" placeholder="🔍 খুঁজো… (model নাম লিখলেই ফিল্টার)" oninput="render()">
  <div class="fbtns">
    <button class="fb on" data-f="all" onclick="setF('all',this)">সব</button>
    <button class="fb" data-f="active" onclick="setF('active',this)">✅ Active</button>
    <button class="fb" data-f="image" onclick="setF('image',this)">🎨 Image</button>
    <button class="fb" data-f="limit" onclick="setF('limit',this)">⏳ Limit</button>
    <button class="fb" data-f="testing" onclick="setF('testing',this)">⏳ চলছে</button>
    <button class="fb" data-f="bad" onclick="setF('bad',this)">❌ সমস্যা</button>
  </div>

  <div class="tbl-wrap">
    <table>
      <thead><tr><th>#</th><th>মডেল</th><th>স্ট্যাটাস</th><th>⚡ Latency</th><th>উত্তর / কারণ</th></tr></thead>
      <tbody id="tb"><tr><td colspan="5" class="empty">লোড হচ্ছে…</td></tr></tbody>
    </table>
  </div>

  <div class="card" style="margin-top:18px">
    <div class="card-title">📜 স্ক্যানার লগ</div>
    <div class="logbox" id="lg"><div>লোড হচ্ছে…</div></div>
  </div>
</div>

<div class="ov" id="ov" onclick="if(event.target===this)closeM()">
  <div class="modal">
    <button class="closex" onclick="closeM()">✕</button>
    <h3 id="mName">—</h3>
    <div class="mrow"><span class="k">স্ট্যাটাস</span><span class="v" id="mStatus">—</span></div>
    <div class="mrow"><span class="k">⚡ Latency</span><span class="v" id="mLat">—</span></div>
    <div class="mrow"><span class="k">ℹ️ তথ্য</span><span class="v" id="mLabel">—</span></div>
    <div class="mrow"><span class="k">🕐 শেষ পরীক্ষা</span><span class="v" id="mTime">—</span></div>
    <div class="mrow"><span class="k">উত্তর / কারণ</span></div>
    <div class="ansbox" id="mAns">—</div>
  </div>
</div>

<script>
let DATA = {detail:[]}, F = 'all', Q = ''
const $ = i => document.getElementById(i)
const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;')

async function tick(){
  try{
    const d = await (await fetch('/scan/results')).json()
    DATA = d
    const c = d.counts || {}
    $('c-tot').textContent = d.detail.length
    $('c-act').textContent = c.active || 0
    $('c-img').textContent = c.image || 0
    $('c-lim').textContent = c['daily-limit'] || 0
    $('c-fail').textContent = (c.hanging||0)+(c['no-access']||0)+(c.paid||0)+(c.gone||0)+(c['non-chat']||0)
    $('c-left').textContent = d.runTotal ? Math.max(0, d.runTotal-(d.runDone||0)) : 0
    const b = $('badge')
    if (d.scanning){ b.textContent='🟢 SCANNING: '+d.scanning.toUpperCase(); b.className='badge b-run' }
    else { b.textContent='⚪ IDLE — scan শেষ '+ (d.updatedAt? new Date(d.updatedAt).toLocaleTimeString():''); b.className='badge b-lim' }
    $('nowT').textContent = d.nowTesting || '— (এই মুহূর্তে কোনো টেস্ট চলছে না)'
    const tot = d.runTotal || d.detail.length
    const done = d.runDone ?? d.detail.length
    const pct = tot ? Math.min(100, Math.round(done/tot*100)) : 0
    $('pfill').style.width = pct+'%'
    $('pct').textContent = pct+'%'
    $('pinfo').textContent = 'পরীক্ষা হয়েছে: '+done+' / '+tot
    const lg = $('lg')
    lg.innerHTML = (d.log||[]).map(l=>'<div>'+esc(l)+'</div>').join('')
    lg.scrollTop = 0
    render()
  }catch(e){ $('badge').textContent='⚠️ gateway offline' }
}
function go(u){ fetch(u,{method:'POST'}).then(()=>setTimeout(tick,800)) }
function setF(f,el){ F=f; document.querySelectorAll('.fb').forEach(x=>x.classList.remove('on')); el.classList.add('on'); render() }

function bucket(s){
  if(s==='active') return 'active'
  if(s==='image') return 'image'
  if(s==='daily-limit') return 'limit'
  if(s==='testing') return 'testing'
  return 'bad'
}
const BN = {active:['b-ok','✅ Active'],fail:['b-fail','❌ সমস্যা'],limit:['b-lim','⏳ Limit'],image:['b-img','🎨 Image'],testing:['b-run','⏳ চলছে'],bad:['b-fail','❌ সমস্যা']}

function render(){
  const q = Q.toLowerCase()
  let rows = DATA.detail.filter(r => F==='all' || bucket(r.status)===F)
  if (q) rows = rows.filter(r => (r.id+' '+r.name+' '+(r.label||'')).toLowerCase().includes(q))
  const ord = {testing:0, active:1, image:2, 'daily-limit':3, hanging:4, 'no-access':5, paid:6, gone:7, 'non-chat':8}
  rows.sort((a,b)=>(ord[a.status]??9)-(ord[b.status]??9) || a.id.localeCompare(b.id))
  $('tb').innerHTML = rows.length ? rows.map((r,i)=>{
    const b = bucket(r.status)
    const [cls, txt] = BN[b] || ['b-fail', r.status]
    const lat = r.latency!=null ? '<span class="'+(r.latency<2000?'lat-f':r.latency<8000?'lat-m':'lat-s')+'">'+r.latency+' ms</span>' : '—'
    const why = esc((r.reply || r.label || '—').slice(0,80))
    return '<tr class="r-'+b+'" onclick="openM(\''+esc(r.id).replace(/'/g,"\\'")+'\')">'+
      '<td>'+(i+1)+'</td><td class="mname">'+esc(r.id)+'</td>'+
      '<td><span class="badge '+cls+'">'+txt+'</span></td><td>'+lat+'</td><td class="snip">'+why+'</td></tr>'
  }).join('') : '<tr><td colspan="5" class="empty">— এই ফিল্টারে কিছু নেই —</td></tr>'
}

let CUR = null
function openM(id){
  const r = DATA.detail.find(x=>x.id===id)
  if(!r) return
  CUR = r
  $('mName').textContent = r.id
  $('mStatus').innerHTML = '<span class="badge '+(BN[bucket(r.status)]||['b-fail',r.status])[0]+'">'+(BN[bucket(r.status)]||['',r.status])[1]+'</span>'
  $('mLat').textContent = r.latency!=null ? r.latency+' ms' : '—'
  $('mLabel').textContent = r.label || '—'
  $('mTime').textContent = r.lastChecked ? new Date(r.lastChecked).toLocaleString() : '—'
  $('mAns').textContent = r.reply || r.error || r.label || '—'
  $('ov').classList.add('open')
}
function closeM(){ $('ov').classList.remove('open') }

function csv(){
  if(!DATA.detail.length){ alert('কোনো ডেটা নেই'); return }
  let c = 'Model,Status,Latency_ms,Info\n'
  DATA.detail.forEach(r=>{ c += '"'+r.id+'","'+r.status+'","'+(r.latency||'')+'","'+String(r.reply||r.label||'').replace(/"/g,"'")+'"\n' })
  const a = document.createElement('a')
  a.href = URL.createObjectURL(new Blob([c],{type:'text/csv;charset=utf-8'}))
  a.download = 'zyvo-models-' + new Date().toISOString().slice(0,10) + '.csv'; a.click()
}
function copyList(which){
  let ids
  if(which==='active') ids = DATA.detail.filter(r=>r.status==='active').map(r=>r.id)
  else ids = DATA.detail.filter(r=>bucket(r.status)==='bad').map(r=>r.id+'  ['+(r.label||'') +']')
  if(!ids.length){ alert('তালিকা খালি'); return }
  navigator.clipboard.writeText(ids.join('\n')).then(()=>alert('📋 '+ids.length+' টা কপি হয়েছে!'))
}
tick(); setInterval(tick, 3000)
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
