// OmniRoute gateway + model health scanner (zero dependencies, Node 20+)
//
// Runs in front of OmniRoute on Render's single public port:
//   - proxies EVERYTHING to the OmniRoute child process (127.0.0.1:20128)
//   - intercepts scanner routes:
//       GET  /active-models  -> fresh active model list (JSON) for zyvo
//       GET  /scan/status    -> scanner state summary (debug)
//       POST /scan/full      -> kick a full rescan now
//   - probes models through the LOCAL OmniRoute (P<=2, max_tokens=5)
//   - buckets: active | daily-limit (429) | no-access (401) | paid (402) |
//     hanging (timeout) — no-access/paid/hanging are excluded from
//     /active-models; daily-limit entries stay, labelled, and are rechecked
//     daily until they come back.
//
// Timers: active rescan every 3h, daily-limit recheck every 24h,
// full rescan every 7d (+ once at boot). State persists to ./data/state.json
// (Render free disk is ephemeral — on boot the first scan rebuilds it).

import http from "node:http"
import { spawn } from "node:child_process"
import fs from "node:fs"
import path from "node:path"

const PORT = Number(process.env.PORT || 10000)
const UPSTREAM_HOST = "127.0.0.1"
const UPSTREAM_PORT = Number(process.env.UPSTREAM_PORT || 20128)
const DATA_DIR = path.join(process.cwd(), "data")
const STATE_FILE = path.join(DATA_DIR, "state.json")
const SCAN_KEY = process.env.OMNIROUTE_API_KEY || "" // key for probing local OmniRoute
const PROBE_TOKENS = 5
const PROBE_TIMEOUT_MS = 45_000
const CONCURRENCY = 1 // keeps heap/CPU low on small containers
const ACTIVE_EVERY_MS = 3 * 60 * 60 * 1000
const DAILY_EVERY_MS = 24 * 60 * 60 * 1000
const FULL_EVERY_MS = 7 * 24 * 60 * 60 * 1000
const SKIP_PATTERNS = /(content-safety|safety-guard|nemoguard|riva-translate|embedding|rerank|whisper|tts|guard)/i

// ── state ──────────────────────────────────────────────────────────
let state = {
  updatedAt: 0,   // last active-list scan
  fullAt: 0,      // last full rescan
  dailyAt: 0,     // last daily-limit recheck
  models: {},     // id -> { status, label, lastChecked }
  scanning: null, // "active" | "full" | "daily" | null
  log: [],
}
const load = () => {
  try {
    state = { ...state, ...JSON.parse(fs.readFileSync(STATE_FILE, "utf8")) }
  } catch {}
}
const save = () => {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true })
    fs.writeFileSync(STATE_FILE + ".tmp", JSON.stringify(state))
    fs.renameSync(STATE_FILE + ".tmp", STATE_FILE)
  } catch {}
}
const note = (m) => {
  state.log = [`${new Date().toISOString()} ${m}`, ...state.log].slice(0, 50)
  console.log(`[scanner] ${m}`)
}

// ── start OmniRoute child ──────────────────────────────────────────
// Env mirrors the official Docker image (DATA_DIR/HOSTNAME matter for a
// fresh boot — without DATA_DIR the child can hang before listening).
const CHILD_DATA = path.join(process.cwd(), "data", "omniroute")
fs.mkdirSync(CHILD_DATA, { recursive: true })
const child = spawn(process.execPath, ["node_modules/omniroute/dist/server-ws.mjs"], {
  env: {
    ...process.env,
    PORT: String(UPSTREAM_PORT),
    HOSTNAME: "0.0.0.0",
    NODE_ENV: "production",
    DATA_DIR: CHILD_DATA,
    // official image default — without a raised ceiling omniroute's own
    // pressure guard trips at ~416MB and 503s every request
    NODE_OPTIONS: [process.env.NODE_OPTIONS, "--max-old-space-size=1024"]
      .filter(Boolean)
      .join(" "),
  },
  stdio: ["ignore", "pipe", "pipe"],
})
child.stdout.on("data", (d) => process.stdout.write(`[omniroute] ${d}`))
child.stderr.on("data", (d) => process.stderr.write(`[omniroute] ${d}`))
child.on("exit", (c) => {
  console.error(`[gateway] omniroute exited (${c}) — restarting in 5s`)
  setTimeout(() => process.exit(1), 5000) // Render restarts the service
})

// ── probing ────────────────────────────────────────────────────────
const probe = (id) =>
  new Promise((resolve) => {
    const body = JSON.stringify({
      model: id,
      messages: [{ role: "user", content: "hi" }],
      max_tokens: PROBE_TOKENS,
    })
    const req = http.request(
      {
        host: UPSTREAM_HOST,
        port: UPSTREAM_PORT,
        path: "/v1/chat/completions",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(body),
          ...(SCAN_KEY ? { Authorization: `Bearer ${SCAN_KEY}` } : {}),
        },
        timeout: PROBE_TIMEOUT_MS,
      },
      (res) => {
        res.resume()
        res.on("end", () => resolve(res.statusCode))
      }
    )
    req.on("timeout", () => req.destroy())
    req.on("error", () => resolve(0))
    req.end(body)
  })

const classify = (code) => {
  if (code === 200) return { status: "active", label: "" }
  if (code === 429) return { status: "daily-limit", label: "Daily limit reached" }
  if (code === 401 || code === 403) return { status: "no-access", label: "No access (401)" }
  if (code === 402) return { status: "paid", label: "Paid only (402)" }
  if (code === 404) return { status: "gone", label: "Not found (404)" }
  return { status: "hanging", label: "No response" }
}

async function scanIds(ids, kind) {
  if (state.scanning) return
  state.scanning = kind
  if (kind === "full") state.totalIds = ids.length
  note(`${kind} scan started (${ids.length} models)`)
  save()
  let done = 0
  const queue = [...ids]
  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (queue.length) {
      const id = queue.shift()
      const code = await probe(id)
      const { status, label } = classify(code)
      const prev = state.models[id]
      // hanging needs 2 strikes before it loses active status
      if (status === "hanging" && prev?.status === "active") {
        state.models[id] = { ...prev, strikes: (prev.strikes || 0) + 1, lastChecked: Date.now() }
        if (state.models[id].strikes < 2) continue
      }
      state.models[id] = { status, label, lastChecked: Date.now(), strikes: 0 }
      if (prev?.status !== status) note(`${id}: ${prev?.status || "new"} -> ${status} (${code})`)
      done++
      if (done % 25 === 0) save()
    }
  })
  await Promise.all(workers)
  state.updatedAt = kind === "active" || kind === "full" ? Date.now() : state.updatedAt
  if (kind === "full") state.fullAt = Date.now()
  if (kind === "daily") state.dailyAt = Date.now()
  state.scanning = null
  note(`${kind} scan done`)
  save()
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
              const ids = JSON.parse(raw).data.map((m) => m.id).filter((id) => !SKIP_PATTERNS.test(id))
              resolve(ids)
            } catch {
              resolve([])
            }
          })
        }
      )
      .on("error", () => resolve([]))
  })

const activeIds = () => Object.entries(state.models).filter(([, v]) => v.status === "active").map(([id]) => id)
const dailyIds = () => Object.entries(state.models).filter(([, v]) => v.status === "daily-limit").map(([id]) => id)

const kick = (kind, fn) => {
  if (state.scanning) return
  fn().catch((e) => {
    state.scanning = null
    note(`${kind} scan failed: ${e?.message || e}`)
    save()
  })
}
const fullScan = () => kick("full", async () => scanIds(await listIds(), "full"))
const activeScan = () => kick("active", () => scanIds(activeIds(), "active"))
const dailyScan = () => kick("daily", () => scanIds(dailyIds(), "daily"))

// ── /active-models response ────────────────────────────────────────
const prettyName = (id) => {
  const tail = id.includes("/") ? id.split("/").slice(1).join("/") : id
  return tail
    .replace(/[:_]/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .replace(/\s+Free\b/i, " (Free)")
}

const activeModelsPayload = () => {
  const now = Date.now()
  // stale active list -> refresh in background, still answer instantly
  if (now - state.updatedAt > ACTIVE_EVERY_MS) activeScan()
  const entries = Object.entries(state.models)
  const active = entries
    .filter(([, v]) => v.status === "active")
    .map(([id]) => ({ id, name: prettyName(id), status: "active" }))
  const limited = entries
    .filter(([, v]) => v.status === "daily-limit")
    .map(([id, v]) => ({ id, name: `${prettyName(id)} · ⏳ ${v.label}`, status: "daily-limit" }))
  const counts = {}
  for (const [, v] of entries) counts[v.status] = (counts[v.status] || 0) + 1
  return {
    updatedAt: state.updatedAt,
    fullAt: state.fullAt,
    scanning: state.scanning,
    counts,
    models: [...active, ...limited],
  }
}

// ── GUI (for humans — no JSON reading needed) ──────────────────────
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
refresh(); setInterval(refresh, 8000)
</script></body></html>`

// ── proxy + router ─────────────────────────────────────────────────
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

const proxy = (req, res) => {
  const isBodyful = req.method === "POST" || req.method === "PUT" || req.method === "PATCH"
  if (!isBodyful) {
    const up = http.request(
      { host: UPSTREAM_HOST, port: UPSTREAM_PORT, path: req.url, method: req.method, headers: { ...req.headers, host: `${UPSTREAM_HOST}:${UPSTREAM_PORT}` } },
      (ur) => {
        res.writeHead(ur.statusCode, ur.headers)
        ur.pipe(res)
      }
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
      (ur) => {
        res.writeHead(ur.statusCode, ur.headers)
        ur.pipe(res)
      }
    )
    up.on("error", () => {
      if (!res.headersSent) res.writeHead(502, { "Content-Type": "application/json" })
      res.end(JSON.stringify({ error: "omniroute upstream not ready" }))
    })
    up.end(body)
  })
}

const server = http.createServer((req, res) => {
  const send = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" })
    res.end(JSON.stringify(obj))
  }
  if (req.method === "GET" && req.url.startsWith("/active-models")) return send(200, activeModelsPayload())
  if (req.method === "GET" && req.url.startsWith("/scan/status")) {
    const detail = Object.entries(state.models).map(([id, v]) => ({ id, status: v.status, label: v.label || "" }))
    const counts = {}
    for (const [, v] of Object.entries(state.models)) counts[v.status] = (counts[v.status] || 0) + 1
    return send(200, {
      ...state,
      models: detail.length,
      counts,
      total: detail.length,
      totalIds: state.totalIds || detail.length,
      detail,
    })
  }
  if (req.method === "GET" && (req.url === "/scan" || req.url === "/scan/")) {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" })
    return res.end(GUI_HTML)
  }
  if (req.method === "POST" && req.url.startsWith("/scan/full")) {
    if (!state.scanning) setTimeout(fullScan, 10)
    return send(202, { started: true })
  }
  return proxy(req, res)
})

load()
server.listen(PORT, () => console.log(`[gateway] listening on :${PORT}, omniroute on :${UPSTREAM_PORT}`))

// boot + periodic scans
// The OmniRoute child takes 1-2 min to boot (Next.js + sqlite). Retry the
// first scan until the child actually serves models (max ~15 min of tries).
let bootTries = 0
const bootScan = () => {
  bootTries++
  if (bootTries > 20) return note("boot scan gave up — /scan/full to retry")
  kick("full", async () => {
    const ids = await listIds()
    if (!ids.length) {
      note(`boot try ${bootTries}: child not ready, retrying in 45s`)
      save()
      setTimeout(bootScan, 45_000)
      return
    }
    await scanIds(ids, "full")
  })
}
setTimeout(bootScan, 60_000)
setInterval(activeScan, ACTIVE_EVERY_MS)
setInterval(dailyScan, DAILY_EVERY_MS)
setInterval(fullScan, FULL_EVERY_MS)
