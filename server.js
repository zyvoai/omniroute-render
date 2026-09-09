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
  // ALWAYS serve — even with zero models — so credentials auto-load everywhere;
  // the phone wrapper refuses empty model lists on its side
  const entries = Object.entries(state.models)
    .filter(([, v]) => ["active", "daily-limit", "image"].includes(v.status))
  const models = {}
  for (const [id, v] of entries) {
    const name = v.status === "daily-limit" ? `${prettyName(id)} · ⏳ ${v.label}` : prettyName(id)
    models[`omniroute/${id}`] = { name }
  }
  const first = entries.find(([, v]) => v.status === "active")
  return {
    $schema: "https://opencode.ai/config.json",
    model: `zyvo/omniroute/${first ? first[0] : "auto/best-coding"}`,
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
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ZYVO · Model Scanner</title>
<style>
*{box-sizing:border-box;margin:0}
body{background:#0B0B0B;color:#F2F2F4;font:15px/1.5 -apple-system,'Segoe UI',Roboto,sans-serif;padding:16px;max-width:720px;margin:0 auto}
h1{font-size:20px;letter-spacing:.02em;margin-bottom:4px}
.sub{color:#8A8A8E;font-size:12px;margin-bottom:16px}
.card{background:#161618;border:1px solid #26262A;border-radius:16px;padding:16px;margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-bottom:12px}
.stat{background:#161618;border:1px solid #26262A;border-radius:14px;padding:12px 14px}
.stat b{font-size:22px;display:block}
.stat span{font-size:11px;color:#8A8A8E;text-transform:uppercase;letter-spacing:.06em}
.ok b{color:#4ADE80}.lim b{color:#FBBF24}.hang b{color:#94A3B8}.no b{color:#F87171}
.pbar{height:8px;background:#26262A;border-radius:99px;overflow:hidden;margin:8px 0 4px}
.pbar>div{height:100%;background:#4ADE80;width:0%;transition:width .6s}
button{width:100%;padding:14px;border:0;border-radius:14px;background:#2563EB;color:#fff;font-weight:700;font-size:15px;margin-bottom:16px}
h2{font-size:12px;color:#8A8A8E;text-transform:uppercase;letter-spacing:.08em;margin:18px 0 8px}
ul{list-style:none}
li{background:#161618;border:1px solid #26262A;border-radius:10px;padding:10px 12px;margin-bottom:6px;font-size:13px;word-break:break-all}
li small{color:#8A8A8E}
.badge{font-size:11px;padding:3px 10px;border-radius:99px;font-weight:700}
.b-scan{background:#FBBF24;color:#000}.b-idle{background:#26262A;color:#8A8A8E}
.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px}
</style></head><body>
<h1>ZYVO · Model Scanner</h1>
<div class="sub">live health check of every model on this gateway — refreshes automatically</div>
<div class="card">
  <div style="display:flex;justify-content:space-between;align-items:center">
    <span id="badge" class="badge b-idle">idle</span>
    <span class="sub" id="updated"></span>
  </div>
  <div class="pbar"><div id="bar"></div></div>
  <div class="sub"><span id="probed">0</span> / <span id="total">0</span> models probed</div>
  <button style="background:#7b3ff2" onclick="go('new')">&#127881; Scan New</button>
  <button onclick="rescan()">&#x1f504; Full Rescan</button>
</div>
<div class="grid">
  <div class="stat ok"><b id="c-active">0</b><span>&#10003; Active</span></div>
  <div class="stat lim"><b id="c-limit">0</b><span>&#9203; Daily limit</span></div>
  <div class="stat hang"><b id="c-hang">0</b><span>Hanging</span></div>
  <div class="stat no"><b id="c-no">0</b><span>No access / Paid</span></div>
</div>
<h2>&#9989; Verified working (zyvo-তে এরাই দেখাবে)</h2>
<ul id="active"></ul>
<h2>&#9203; Daily limit reached (রোজ recheck হয়)</h2>
<ul id="limit"></ul>
<h2>&#10060; Not usable</h2>
<ul id="other"></ul>
<script>
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
async function refresh(){
  try{
    const d = await (await fetch('/scan/status')).json()
    const c = d.counts || {}
    document.getElementById('c-active').textContent = c.active || 0
    document.getElementById('c-limit').textContent = c['daily-limit'] || 0
    document.getElementById('c-hang').textContent = (c.hanging || 0)
    document.getElementById('c-no').textContent = (c['no-access'] || 0) + (c.paid || 0) + (c.gone || 0)
    const b = document.getElementById('badge')
    if (d.scanning) { b.textContent = 'SCANNING: ' + d.scanning; b.className = 'badge b-scan' }
    else { b.textContent = 'idle'; b.className = 'badge b-idle' }
    document.getElementById('updated').textContent = d.updatedAt ? new Date(d.updatedAt).toLocaleString() : ''
    const probed = d.total || 0
    document.getElementById('probed').textContent = probed
    document.getElementById('total').textContent = d.totalIds || probed
    document.getElementById('bar').style.width = (d.totalIds ? Math.min(100, probed / d.totalIds * 100) : 0) + '%'
    const det = d.detail || []
    const put = (id, arr) => { document.getElementById(id).innerHTML = arr.length ? arr.map(x =>
      '<li class="mono">' + esc(x.id) + (x.label ? ' <small>' + esc(x.label) + '</small>' : '') + '</li>').join('') : '<li class="sub">— none —</li>' }
    put('active', det.filter(x => x.status === 'active'))
    put('limit', det.filter(x => x.status === 'daily-limit'))
    put('other', det.filter(x => !['active','daily-limit'].includes(x.status)))
  }catch(e){ document.getElementById('badge').textContent = 'offline…' }
}
async function rescan(){ await fetch('/scan/full', {method:'POST'}); refresh() }
async function go(k){ await fetch('/scan/'+k, {method:'POST'}); setTimeout(refresh, 800) }
refresh(); setInterval(refresh, 8000)
</script></body></html>`

// ── http server: scanner routes + proxy ────────────────────────────
const send = (res, code, obj) => {
  res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" })
  res.end(JSON.stringify(obj))
}

const server = http.createServer((req, res) => {
  const u = req.url || "/"
  if (req.method === "GET" && (u === "/scan" || u === "/scan/")) {
    // server-render the live data INTO the page — works even if JS fetch fails
    const init = JSON.stringify({
      scanning: state.scanning,
      nowTesting: state.nowTesting,
      runTotal: state.runTotal,
      runDone: state.runDone,
      updatedAt: state.updatedAt,
      counts: counts(),
      log: state.log.slice(0, 40),
      detail: detailList(),
    }).replace(/</g, "\\u003c")
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" })
    return res.end(GUI_HTML.replace("/*__SRV_DATA__*/", "window.__INIT__=" + init + ";"))
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
    return send(res, 200, zyvoConfigPayload(host))
  }
  if (req.method === "POST" && u.startsWith("/scan/full")) { if (!state.scanning) fullScan(); return send(res, 202, { started: !state.scanning ? "full" : "busy" }) }
  if (req.method === "POST" && u.startsWith("/scan/new")) { if (!state.scanning) newScan(); return send(res, 202, { started: !state.scanning ? "new" : "busy" }) }
  if (req.method === "POST" && u.startsWith("/scan/stop")) { stopScan(); return send(res, 200, { stopping: true }) }
  // SOURCE OF TRUTH: /v1/models এ শুধু বেঁচে থাকা model — যেকোনো client
  // (opencode auto-fetch, curl, যা খুশি) সরাসরি এটাই দেখবে
  if (req.method === "GET" && u.startsWith("/v1/models"))
    return send(res, 200, { object: "list", data: activeOnlyModels() })
  return proxy(req, res)
})

const activeOnlyModels = () => {
  const ok = new Set(activeList())
  const lim = new Set(limitList())
  return Object.entries(state.models)
    .filter(([id, v]) => ok.has(id) || lim.has(id))
    .map(([id, v]) => ({
      id: v.status === "daily-limit" ? id + "  ⏳ daily limit" : id,
      object: "model",
      created: Math.floor((v.lastChecked || Date.now()) / 1000),
      owned_by: "zyvo",
    }))
}

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
