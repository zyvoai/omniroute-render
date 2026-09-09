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
<html lang="bn"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ZYVO · Model Control Center</title>
<style>
:root{--bg:#070b09;--card:#0f1512;--card2:#131a16;--line:#1d2620;--ink:#e9f2ec;--mut:#7f9187;
--grn:#3ddc84;--blu:#4d9fff;--amb:#ffc94d;--red:#ff5c6c;--vio:#a06bff}
*{margin:0;box-sizing:border-box;font-family:'Segoe UI',system-ui,sans-serif}
body{background:var(--bg);color:var(--ink);min-height:100vh}
.wrap{max-width:860px;margin:0 auto;padding:18px 14px 60px}
header{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap}
h1{font-size:22px}
h1 .z{color:var(--grn)}
.badge{font-size:11px;font-weight:800;padding:4px 12px;border-radius:99px;letter-spacing:.04em}
.b-on{background:var(--grn);color:#04120a;animation:pulse 1.2s infinite}
.b-idle{background:var(--line);color:var(--mut)}
@keyframes pulse{50%{opacity:.55}}
.now{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 14px;margin-bottom:12px;font-family:ui-monospace,monospace;font-size:12px;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.now b{color:var(--grn)}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:12px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px;text-align:center}
.stat b{font-size:24px;display:block}
.stat span{font-size:10px;color:var(--mut);text-transform:uppercase;letter-spacing:.06em}
.s-a b{color:var(--grn)}.s-i b{color:var(--vio)}.s-l b{color:var(--amb)}.s-f b{color:var(--red)}.s-t b{color:var(--blu)}.s-tot b{color:var(--ink)}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;margin-bottom:12px}
.pbar{height:9px;background:var(--line);border-radius:99px;overflow:hidden;margin:10px 0 6px}
.pbar>div{height:100%;width:0%;background:linear-gradient(90deg,var(--grn),var(--blu));transition:width .5s}
.pmeta{display:flex;justify-content:space-between;font-size:12px;color:var(--mut)}
.btns{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
button{border:0;border-radius:12px;padding:12px 18px;font-weight:700;font-size:13px;cursor:pointer;color:#fff;transition:.15s}
button:hover{filter:brightness(1.15)}
button:disabled{opacity:.45}
.b-new{background:#0e7a4f}.b-full{background:#1f6feb}.b-stop{background:#7a1f2b}.b-csv{background:#333;color:var(--ink)}
input[type=text]{width:100%;background:#0b100d;border:1px solid var(--line);color:var(--ink);border-radius:10px;padding:11px 13px;font-size:14px;outline:none;margin-bottom:10px}
input:focus{border-color:var(--grn)}
.fbtns{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.fb{background:var(--card);color:var(--mut);border:1px solid var(--line);border-radius:99px;padding:6px 16px;font-size:12px;cursor:pointer}
.fb.on{background:var(--grn);color:#04120a;border-color:var(--grn);font-weight:700}
.tbl{max-height:520px;overflow:auto;border:1px solid var(--line);border-radius:14px}
table{width:100%;border-collapse:collapse;font-size:12px;min-width:560px}
th{position:sticky;top:0;background:var(--card2);color:var(--mut);text-transform:uppercase;font-size:10px;letter-spacing:.06em;padding:10px;text-align:left;border-bottom:1px solid var(--line)}
td{padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:top}
.mono{font-family:ui-monospace,monospace;font-size:11px}
.st{padding:3px 10px;border-radius:99px;font-size:10px;font-weight:800;white-space:nowrap}
.st-a{background:rgba(61,220,132,.14);color:var(--grn)}
.st-t{background:rgba(77,159,255,.14);color:var(--blu)}
.st-i{background:rgba(160,107,255,.14);color:var(--vio)}
.st-l{background:rgba(255,201,77,.14);color:var(--amb)}
.st-f{background:rgba(255,92,108,.14);color:var(--red)}
.lat-f{color:var(--grn);font-weight:700}.lat-m{color:var(--amb)}.lat-s{color:var(--red)}
.rep{color:var(--mut);max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.log{background:#060a08;border:1px solid var(--line);border-radius:12px;padding:10px;font-family:ui-monospace,monospace;font-size:11px;color:var(--mut);max-height:150px;overflow:auto;margin-top:14px}
.sub{color:var(--mut);font-size:12px}
</style></head><body>
<div class="wrap">
<header>
  <h1><span class="z">ZYVO</span> · Model Control Center</h1>
  <span id="badge" class="badge b-idle">IDLE</span>
</header>
<div class="sub" style="margin-bottom:10px">একটা একটা করে গভীরভাবে পরীক্ষা — latency, উত্তর, error সব লাইভ। নতুন provider add করলে ১৫ মিনিটে নিজেই ধরবে।</div>
<div class="now">🔬 এখন পরীক্ষা চলছে: <b id="nowT">—</b></div>
<div class="stats">
  <div class="stat s-tot"><b id="c-tot">0</b><span>Total</span></div>
  <div class="stat s-a"><b id="c-act">0</b><span>✓ Active</span></div>
  <div class="stat s-i"><b id="c-img">0</b><span>🎨 Image</span></div>
  <div class="stat s-l"><b id="c-lim">0</b><span>⏳ Limit</span></div>
  <div class="stat s-f"><b id="c-fail">0</b><span>Unusable</span></div>
  <div class="stat s-t"><b id="c-run">0</b><span>Left</span></div>
</div>
<div class="card">
  <div class="pmeta"><span id="kind">—</span><span id="pct">0%</span></div>
  <div class="pbar"><div id="bar"></div></div>
  <div class="pmeta"><span id="done">0</span> probed · <span id="left">0</span> left</div>
  <div class="btns">
    <button class="b-new" onclick="go('/scan/new')">🆕 নতুন model scan</button>
    <button class="b-full" onclick="go('/scan/full')">🔄 Full rescan</button>
    <button class="b-stop" onclick="go('/scan/stop')">⏹ Stop</button>
    <button class="b-csv" onclick="csv()">📥 CSV</button>
  </div>
</div>
<input type="text" id="q" placeholder="🔍 খুঁজো… (model নাম লিখলেই ফিল্টার হবে)" oninput="render()">
<div class="fbtns">
  <button class="fb on" data-f="all" onclick="setF('all',this)">সব</button>
  <button class="fb" data-f="active" onclick="setF('active',this)">✓ Active</button>
  <button class="fb" data-f="image" onclick="setF('image',this)">🎨 Image</button>
  <button class="fb" data-f="limit" onclick="setF('limit',this)">⏳ Limit</button>
  <button class="fb" data-f="bad" onclick="setF('bad',this)">❌ Unusable</button>
</div>
<div class="tbl"><table>
<thead><tr><th>#</th><th>Model</th><th>Status</th><th>⚡</th><th>উত্তর / কারণ</th></tr></thead>
<tbody id="tb"><tr><td colspan="5" class="sub">লোড হচ্ছে…</td></tr></tbody>
</table></div>
<div class="log" id="lg"></div>
</div>
<script>
let DATA={detail:[]},F='all'
const esc=s=>String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;')
const $=i=>document.getElementById(i)
const BAD={hanging:'⏱ Timeout',noaccess:'🚫 No access',paid:'💳 Paid',gone:'❌ Gone',nonchat:'🚫 Non-chat',testing:'⏳ Testing',limit:'⏳ Limit',image:'🎨 Image',active:'✓ Active'}
async function tick(){
  try{
    const d=await(await fetch('/scan/results')).json()
    DATA=d
    const c=d.counts||{}
    $('c-tot').textContent=d.detail.length
    $('c-act').textContent=c.active||0
    $('c-img').textContent=c.image||0
    $('c-lim').textContent=c['daily-limit']||0
    $('c-fail').textContent=(c.hanging||0)+(c['no-access']||0)+(c.paid||0)+(c.gone||0)+(c['non-chat']||0)
    const run=d.runTotal?(d.runTotal-(d.runDone||0)):0
    $('c-run').textContent=run
    const b=$('badge')
    if(d.scanning){b.textContent='SCANNING: '+d.scanning.toUpperCase();b.className='badge b-on'}
    else{b.textContent='IDLE';b.className='badge b-idle'}
    $('nowT').textContent=d.nowTesting||'—'
    $('kind').textContent=d.scanning?('scan: '+d.scanning):'শেষ scan: '+(d.updatedAt?new Date(d.updatedAt).toLocaleString():'—')
    const tot=d.runTotal||d.detail.length,pct=d.runTotal?Math.min(100,Math.round((d.runDone||0)/d.runTotal*100)):100
    $('bar').style.width=pct+'%';$('pct').textContent=pct+'%'
    $('done').textContent=d.runDone||d.detail.length;$('left').textContent=run
    const lg=$('lg');lg.innerHTML=(d.log||[]).map(l=>'<div>'+esc(l)+'</div>').join('');lg.scrollTop=0
    render()
  }catch(e){$('nowT').textContent='(gateway unreachable)'}
}
function bucket(s){
  if(s==='active')return'active'
  if(s==='image')return'image'
  if(s==='daily-limit')return'limit'
  if(['testing'].includes(s))return'testing'
  if(['hanging','no-access','paid','gone','non-chat'].includes(s))return'bad'
  return'other'
}
function render(){
  const q=($('q').value||'').toLowerCase()
  let rows=DATA.detail.filter(r=>F==='all'||bucket(r.status)===F)
  if(q)rows=rows.filter(r=>r.id.toLowerCase().includes(q)||r.name.toLowerCase().includes(q))
  const order={testing:0,active:1,image:2,'daily-limit':3,hanging:4,'no-access':5,paid:6,gone:7,'non-chat':8}
  rows.sort((a,b)=>(order[a.status]??9)-(order[b.status]??9)||a.id.localeCompare(b.id))
  $('tb').innerHTML=rows.length?rows.map((r,i)=>{
    const b=bucket(r.status)
    const cls=b==='active'?'st-a':b==='image'?'st-i':b==='limit'?'st-l':b==='testing'?'st-t':b==='bad'?'st-f':'st-l'
    const lat=r.latency!=null?('<span class="'+(r.latency<2000?'lat-f':r.latency<8000?'lat-m':'lat-s')+'">'+r.latency+'ms</span>'):'—'
    const why=esc(r.reply||r.label||'')
    return '<tr><td class="mono">'+(i+1)+'</td><td class="mono">'+esc(r.id)+'</td>'+
      '<td><span class="st '+cls+'">'+(BAD[b]||r.status)+'</span></td><td>'+lat+'</td><td class="rep">'+why+'</td></tr>'
  }).join(''):'<tr><td colspan="5" class="sub">— কিছু নেই —</td></tr>'
}
function setF(f,el){F=f;document.querySelectorAll('.fb').forEach(x=>x.classList.remove('on'));el.classList.add('on');render()}
async function go(u){await fetch(u,{method:'POST'});tick()}
function csv(){
  const rows=[['Model','Status','Latency_ms','Info']].concat(DATA.detail.map(r=>[r.id,r.status,r.latency||'',(r.reply||r.label||'').replace(/"/g,"'")]))
  const blob=new Blob([rows.map(r=>r.map(x=>'"'+String(x).replace(/"/g,'""')+'"').join(',')).join('\\n')],{type:'text/csv'})
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='zyvo-models.csv';a.click()
}
tick();setInterval(tick,3000)
</script></body></html>`

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
