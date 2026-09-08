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
const CONCURRENCY = 2
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
      .get({ host: UPSTREAM_HOST, port: UPSTREAM_PORT, path: "/v1/models" }, (res) => {
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
      })
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

// ── proxy + router ─────────────────────────────────────────────────
const proxy = (req, res) => {
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
  req.pipe(up)
}

const server = http.createServer((req, res) => {
  const send = (code, obj) => {
    res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" })
    res.end(JSON.stringify(obj))
  }
  if (req.method === "GET" && req.url.startsWith("/active-models")) return send(200, activeModelsPayload())
  if (req.method === "GET" && req.url.startsWith("/scan/status"))
    return send(200, { ...state, models: Object.keys(state.models).length })
  if (req.method === "POST" && req.url.startsWith("/scan/full")) {
    if (!state.scanning) setTimeout(fullScan, 10)
    return send(202, { started: true })
  }
  return proxy(req, res)
})

load()
server.listen(PORT, () => console.log(`[gateway] listening on :${PORT}, omniroute on :${UPSTREAM_PORT}`))

// boot + periodic scans
setTimeout(() => {
  if (!Object.keys(state.models).length) fullScan()
  else activeScan()
}, 60_000) // give omniroute a minute to boot first
setInterval(activeScan, ACTIVE_EVERY_MS)
setInterval(dailyScan, DAILY_EVERY_MS)
setInterval(fullScan, FULL_EVERY_MS)
