# OmniRoute on Render (fresh instance)

Deploy [omniroute](https://www.npmjs.com/package/omniroute) (v3.8.50) as a
free public web service. Your PC install and its data stay untouched — this
is a separate, clean instance.

## Deploy on Render (free)

1. Sign up at https://dashboard.render.com (GitHub or Google login)
2. New + → **Web Service** → "Build and deploy from a Git repository" →
   connect GitHub → pick **zyvoai/omniroute-render**
3. Settings:
   - Runtime: **Node**
   - Build Command: `npm install`
   - Start Command: `npm start`
   - Instance Type: **Free**
4. Add Environment variables:
   - `JWT_SECRET` = PbEy+csAGLGu/Jey2ZnUb6ZDPezZoTK8KlhqV0FyqK9z3JVHcABjxkWHpCDPQsx/
   - `API_KEY_SECRET` = 590d2150ee90fc3868440f8c4f76a2829502bfd553133039f350eed18c341c8d
   - `INITIAL_PASSWORD` = (choose your own strong dashboard password)
5. Create Web Service → done. URL: https://omniroute-render.onrender.com

## First run

- Open the URL → log in to the dashboard with `INITIAL_PASSWORD`
- Add your upstream providers → create an API key
- Put that key in zyvo's `config/zyvo.json` (baseURL:
  https://omniroute-render.onrender.com/v1)

## Free tier notes

- Sleeps after ~15 min idle (first request then takes ~1 min to wake)
- RAM 512 MB; disk resets on redeploys (re-add providers if a deploy wipes them)
