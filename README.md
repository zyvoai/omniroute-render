# OmniRoute on Render — with live model health scanner

Fresh public OmniRoute (v3.8.50) + a zero-dependency gateway that keeps a
**live list of working models**: probes every model, buckets them
(active / daily-limit / no-access / paid / hanging), rescans the active
list every 3h, rechecks daily-limit models daily, full-rescans weekly —
and serves the fresh list at `/active-models` for zyvo to show.

Your PC install and its data stay untouched — this is a separate instance.

## Deploy (free)

1. Sign up at https://dashboard.render.com (GitHub/Google login)
2. New + → **Web Service** → connect GitHub → **zyvoai/omniroute-render**
3. Settings:
   - Runtime **Node** · Build `npm install` · Start `npm start` · **Free**
4. Environment variables:
   - `JWT_SECRET` — any long random string
   - `API_KEY_SECRET` — any long random string
   - `INITIAL_PASSWORD` — your dashboard password
   - `OMNIROUTE_API_KEY` — (optional, later) an OmniRoute API key so the
     scanner probes with auth; set after you create a key in the dashboard
5. Create — URL becomes `https://omniroute-render.onrender.com`

## First run (5 minutes)

1. Open the URL → OmniRoute dashboard → log in with `INITIAL_PASSWORD`
2. Add your upstream providers (the same ones your PC copy uses)
3. Dashboard → create an **API key** → put it in the Render env
   `OMNIROUTE_API_KEY` (Save & Deploy) — now the scanner probes with auth
4. Within ~1-2 min of OmniRoute booting, the first **full scan** starts
   (3.5k+ models, ~1-2h at safe concurrency); check progress:
   `https://<url>/scan/status`
5. Active list for zyvo: `https://<url>/active-models`

## Endpoints

| Route | What |
|---|---|
| `GET /active-models` | fresh list: active first, daily-limit labelled `· ⏳ Daily limit reached` |
| `GET /scan/status` | scanner state, counts per bucket, last log lines |
| `POST /scan/full` | kick a full rescan now |
| everything else | proxied to OmniRoute (`/v1/...`, dashboard) |

## Free tier truth

- Sleeps after ~15 min idle; any request (each zyvo launch) wakes it
- 512 MB RAM shared by gateway + OmniRoute — if it OOMs, prune providers
- Disk is ephemeral: state.json is rebuilt by the boot scan after redeploys
