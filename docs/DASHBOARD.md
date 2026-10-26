# Owner dashboard

Static page (`dashboard/index.html`, Vercel) + Worker API (`/dash/*`, `worker/src/dashboard.ts`). Not an MCP tool, so the
LLM never sees session names or the snapshot repository.

## Status (round 3)
Works: sessions (own names, start / resume / save / deep save / stop), **command history** (VM history merged with the Worker's saved
commands, keyset paging, filters, detail with request and response, paged full output), **the dashboard's own command lane**
(`POST /dash/bash`, up to 4 at once, independent of MCP commands, cancel only its own), **VM activity phases** (paused, capturing,
chunk n of N, committing...), and a **live view** over one WebSocket (LiveHub Durable Object, hibernation, 3 viewers, 30 minute cap,
signed 30 s ticket, the agent dials out and sends about one frame a second only while someone watches). UI: Tailwind v4 single page
(`dashboard/`, tabs Live, Commands, Terminal, Sessions). Verified with a mock Worker in headless Chromium under the real CSP: lazy loading
requests the next page by cursor, live frames cause zero HTTP calls to the Worker, axe reports no violations (light and dark, phone and desktop).
Not built: `.swarm` browser, snapshot picker, Cloudflare Access (see decisions below). **Never run against a deployed Worker or a real VM.**

## Setup (owner actions; cannot be done from the VM)
1. `wrangler d1 create` (prod + staging); put `database_id` in `wrangler.jsonc` and `wrangler.local.jsonc`.
2. `wrangler d1 migrations apply remote-linux-vm-db --remote` (applies 0001 failed commands, 0002 session names, 0003 descriptions).
3. `wrangler secret put DASHBOARD_TOKEN` (random, 32+ chars; shorter keeps `/dash` switched OFF, answers 404).
4. Set var `DASHBOARD_ORIGIN` to the exact dashboard origin (`https://dash.example.com`, no path, no trailing slash).
5. Create a Vercel project from `dashboard/` (Framework: Other, no build) and attach the domain.

## Security model (what is and is not protected)
- Every `/dash` request needs BOTH `Origin == DASHBOARD_ORIGIN` and `Authorization: Bearer <token>` (SHA-256 compared).
- The Origin check only stops BROWSERS on other sites (CORS). Any non-browser client can send any Origin header. The real
  boundary is the token. Do not describe the Origin lock as authentication.
- The token is typed into the page and kept in `sessionStorage` (cleared when the tab closes). The page has a strict CSP and
  renders all data with `textContent`.
- There is no rate limit on failed tokens in the Worker. With 32+ random characters this is not practical to guess; add a
  Cloudflare rate-limiting rule on `/dash/*` if you want logs/noise control.

## Design notes and open decisions
1. **DECIDED: Cloudflare Access before P2.** P2 turns the token into a shell credential, so a token alone must not be enough.
   Consequence: Access works best SAME-ORIGIN. A Vercel page calling the Worker is cross-site, so the Access cookie is a
   third-party cookie (blocked by many browsers) and CORS must also allow credentials. Plan: serve `dashboard/index.html`
   from the Worker (static assets) on the Worker's own hostname, protect `/dash/*` and the page with an Access
   self-hosted application (allow only the owner's identity), and keep the token as a second factor. Access must NOT cover
   `/mcp`, `/runner`, `/authorize`, `/token`, `/register`, `/.well-known/*` or `/health` (the connector and runners use
   those). The Worker should also verify the `Cf-Access-Jwt-Assertion` JWT (team domain + AUD) so it still refuses requests
   that reach it some other way (workers.dev). The Vercel deployment then becomes optional. P1 as committed works
   standalone (token + Origin) until P2.
2. **SUPERSEDED by the LiveHub WebSocket (round 3).** The earlier decision was a Worker streaming proxy: Browser -> Worker `/dash/live/:job` (same
   Origin+token check) -> Worker streams from the agent through the tunnel it already uses. The runner is never exposed to
   browsers and no ticket scheme is needed. Use `fetch()` streaming (not `EventSource`, which cannot send an Authorization
   header). It is read-only by construction; SSE-style one-way streaming is simpler and safer than a WebSocket for this.
   UNVERIFIED: long-lived streaming through a Worker subrequest must be checked on staging (P5) before relying on it.
3. **Operator lane (P2):** tag operator processes with their own env marker / process group, so the LLM's `bash_cancel`
   (`kill_background`) and the existing kill switch cannot hit them; record a `lane` column in history; the two lanes share
   the filesystem and CPU, so say so in the UI. Capture pauses the container: owner commands during capture are refused and
   saved as `ext-` ids, same as the LLM's.
4. **Binding name (done):** the D1 binding is `DB` (database `remote-linux-vm-db`, staging `remote-linux-vm-db-staging`); it holds
   `failed_commands` and `session_names`.
5. **CSP:** `connect-src https:` is broad because the Worker URL is typed in. Once the Worker URL is fixed, pin it in
   `dashboard/vercel.json`.
6. **`.swarm` browser (P3)** only works while a VM is up (the files live in the container). Optional: also keep a copy of
   `.swarm` in the saved snapshot metadata for a "last saved" view.
7. No audit log of dashboard actions yet (who started/stopped/saved and when). Cheap to add as one D1 table.
