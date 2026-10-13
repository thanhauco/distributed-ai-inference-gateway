# Architecture walkthrough video

`docs/media/architecture-walkthrough.mp4` (1920×1080, 30 fps, ~2 min) is
rendered from code: every frame is a pure function of time drawn on a
`<canvas>` with [rough.js](https://roughjs.com) in the hand-drawn style of the
original system-design sketch, captured by headless Chromium (Playwright) and
encoded with ffmpeg.

| Time | Scene |
|---|---|
| 0:00 | Title |
| 0:06 | The system diagram builds up, then one request travels API → rate limiter → cache → batcher → router → resilience → vLLM worker, and tokens stream back over SSE |
| 0:44 | KV-cache locality: random placement thrashes every worker's prefix cache; prefix-affinity keeps it warm |
| 1:02 | Token-level sliding-window rate limiting: 429 + Retry-After, the window slides, usage is reconciled |
| 1:13 | Semantic cache hit/miss in embedding space, the power-of-two batcher, singleflight |
| 1:25 | Failures: retries with jitter, breaker closed → open → half-open → closed, first-token timeout failover |
| 1:37 | Observability: trace waterfall, dashboards, correlated JSON logs |
| 1:47 | Scaling: stateless gateway replicas, a worker joining, rendezvous hashing moving ~1/n of keys |
| 1:57 | Outro |

## Render

```bash
cd video
npm install                     # roughjs + playwright (uses an installed Chromium)
node render.mjs                 # → ../docs/media/architecture-walkthrough.mp4
node render.mjs --stills 20,50  # PNG stills in video/frames/
open index.html?play            # live preview in a browser (serve the folder over HTTP)
```

Set `CHROMIUM_PATH` to use a system Chromium instead of Playwright's.

## Fonts

`assets/Virgil.woff2` (Virgil, by Ellinor Rapp for Excalidraw) and
`assets/Cascadia.woff2` (Cascadia Code, by Microsoft) are redistributed under
the SIL Open Font License 1.1; see [`assets/OFL.txt`](assets/OFL.txt).
