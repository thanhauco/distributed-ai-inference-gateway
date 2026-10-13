// Scenes of the architecture walkthrough. Each scene draws itself from its
// local time L (seconds since the scene started).
/* global W, H, C, ctx, P, E, clamp, lerp, win, rng, withAlpha, pop, slide, box, circle, roughLine,
   highlight, text, textWidth, bullets, typed, paragraph, tag, curve, pathLength, pointAt, arrow,
   packet, stepBubble, chapter, caption, ring, checkmark, cross, meter */

// Fit text into maxW by shrinking the font.
function fit(str, maxW, size, mono) {
  let s = size;
  while (s > 10 && textWidth(str, s, mono) > maxW) s -= 1;
  return s;
}

// ======================================================================
// Scene 0: title
// ======================================================================
function sceneTitle(L) {
  // drifting packets in the background
  const r = rng(7);
  for (let i = 0; i < 26; i++) {
    const y = 120 + r() * 840;
    const speed = 80 + r() * 160;
    const x = ((r() * W + L * speed) % (W + 200)) - 100;
    withAlpha(0.18 + 0.2 * r(), () => packet(x, y, [C.req, C.stream, C.hit][i % 3], 5 + r() * 4, { blur: 12 }));
  }
  const pt = P(L, 0.2, 1.2);
  slide(pt, 0, 30, () => {
    text('Distributed AI Inference Gateway', W / 2, 420, { size: 96, align: 'center' });
  });
  const tw = textWidth('Distributed AI Inference Gateway', 96);
  highlight(W / 2 - tw / 2 + 20, 482, tw - 40, P(L, 1.0, 1.0));
  withAlpha(P(L, 1.4, 0.8), () => {
    text('request routing  ×  KV-cache locality', W / 2, 560, { size: 46, align: 'center', color: C.soft });
  });
  const chips = [['Go · Gin', C.sky], ['vLLM · Ollama workers', C.purple], ['OpenAI-compatible · SSE', C.orange], ['Redis · Prometheus · OTel', C.green]];
  const total = chips.reduce((acc, [s]) => acc + textWidth(s, 26) + 28 + 26, -26);
  let x = W / 2 - total / 2;
  chips.forEach(([s, pal], i) => {
    const w = textWidth(s, 26) + 28;
    pop(P(L, 2.0 + i * 0.25, 0.5), x + w / 2, 660, () => tag(s, x, 660, pal, { size: 26 }));
    x += w + 26;
  });
  withAlpha(P(L, 3.0, 0.8), () => text('Thanh & Claude · 2026', W / 2, 980, { size: 30, align: 'center', color: C.faint }));
}

// ======================================================================
// Scene 1: the system diagram, then one request's journey through it
// ======================================================================
const D = {
  stageY: 455,
  stageH: 215,
  stageW: 172,
  lane: 525,
  stages: [
    { x: 222, title: 'API Layer', pal: C.green, items: ['Gin · OpenAI', 'SSE streaming', 'Auth (API key)', 'Admin API'] },
    { x: 412, title: 'Rate Limiter', pal: C.yellow, items: ['Per API key', 'Tokens / min', 'Sliding window', '429 on limit'] },
    { x: 602, title: 'Cache', pal: C.pink, items: ['Exact match', 'Semantic sim.', 'Redis-backed', 'SSE replay'] },
    { x: 792, title: 'Batcher', pal: C.purple, items: ['2ⁿ batch sizes', '5–20 ms wait', 'Coalesce dupes', 'singleflight'] },
    { x: 982, title: 'Router', pal: C.blue, items: ['Model-aware', 'KV affinity', 'Bounded load', 'Power of 2'] },
    { x: 1172, title: 'Resilience', pal: C.green, items: ['TTFT timeout', 'Retry + jitter', 'Breakers', 'Failover'] },
  ],
  workers: [
    { y: 430, title: 'vLLM Worker 1', sub: '(models: Llama 3, Mistral)', api: '/v1/chat/completions', pal: C.blue },
    { y: 590, title: 'vLLM Worker 2', sub: '(models: Llama 3)', api: '/v1/chat/completions', pal: C.blue },
    { y: 750, title: 'Ollama Worker', sub: '(models: Phi, Gemma)', api: '/api/chat (NDJSON)', pal: C.purple },
  ],
};
const callouts = ['auth ✓ · schema ✓', '−1,642 tok · 198k left', 'miss · sim 0.71', 'batch ×4 · 8 ms', 'affinity → vLLM 1', 'TTFT timer · breaker ✓'];
const J0 = 14.4; // journey start (scene-local)
const STAGE_DT = 2.25;
const stageArrive = (i) => J0 + 0.6 + i * STAGE_DT;
const WORKER_ARRIVE = stageArrive(5) + 2.4;
const STREAM_START = WORKER_ARRIVE + 0.8;

function forwardPath(j) {
  const wy = D.workers[j].y + 67;
  return [[1344, D.lane], [1440, D.lane], [1440, wy], [1553, wy]];
}
const STREAM_PATH = [[1553, 497], [1440, 497], [1440, D.lane], [672, D.lane], [672, 284]];

function sceneDiagram(L) {
  const journey = L >= J0;
  const dimTitle = journey ? 1 - 0.75 * P(L, J0, 0.6) : 1;

  // --- header + description
  withAlpha(P(L, 0, 0.8), () => {
    text('Distributed AI Inference Gateway', 40, 58, { size: 50 });
    text('System Design', 40, 116, { size: 50 });
  });
  withAlpha(P(L, 0.6, 0.8), () => {
    paragraph('OpenAI-compatible gateway that routes requests across multiple vLLM/Ollama workers with load balancing, rate limiting, caching, batching, retries, circuit breakers and observability.',
      40, 178, 560, { size: 23 });
  });

  // --- clients
  const clientsP = P(L, 1.0, 0.6);
  const streaming = L > STREAM_START + 1.2;
  pop(clientsP, 805, 200, () => {
    box(640, 120, 330, 162, C.orange, { glow: journey ? 0.6 * win(L, STREAM_START + 1.0, STREAM_START + 7.5) : 0 });
    if (!streaming) {
      text('Clients', 805, 152, { size: 32, align: 'center' });
      text('(apps, SDKs)', 805, 186, { size: 25, align: 'center' });
      text('OpenAI-compatible API', 805, 218, { size: 20, align: 'center' });
      text('/v1/chat/completions', 805, 243, { size: 18, align: 'center', mono: true });
      text('(streaming SSE)', 805, 266, { size: 20, align: 'center' });
    } else {
      const reply = 'The gateway routed this to the worker that already held the prompt prefix in its KV cache, so prefill was skipped and tokens streamed sooner.';
      const shown = typed(reply, P(L, STREAM_START + 1.2, 5.2));
      text('assistant ▸', 660, 145, { size: 20, color: C.orange[1] });
      paragraph(shown + (Math.floor(L * 3) % 2 ? '▍' : ''), 660, 175, 295, { size: 19, lineHeight: 24 });
    }
  });
  // HTTPS arrows
  arrow([[672, 284], [672, 370]], P(L, 1.5, 0.4), { both: true, color: journey ? C.stream : C.ink });
  arrow([[938, 284], [938, 370]], P(L, 1.6, 0.4));
  withAlpha(P(L, 1.8, 0.5), () => text('HTTPS (SSE streaming)', 805, 326, { size: 20, align: 'center' }));

  // --- gateway container
  pop(P(L, 1.8, 0.7), 780, 597, () => {
    box(205, 372, 1150, 450, C.sky, { dashed: true, r: 22, roughness: 0.8 });
    withAlpha(dimTitle, () => text('Go Inference Gateway (Stateless)', 780, 410, { size: 32, align: 'center' }));
  });

  // --- stages
  D.stages.forEach((s, i) => {
    const cx = s.x + D.stageW / 2;
    const active = journey ? win(L, stageArrive(i) - 0.25, stageArrive(i) + STAGE_DT - 0.2, 0.25) : 0;
    pop(P(L, 2.3 + i * 0.45, 0.6), cx, D.stageY + D.stageH / 2, () => {
      const scale = 1 + 0.035 * active;
      ctx.save();
      ctx.translate(cx, D.stageY + D.stageH / 2);
      ctx.scale(scale, scale);
      ctx.translate(-cx, -(D.stageY + D.stageH / 2));
      box(s.x, D.stageY, D.stageW, D.stageH, s.pal, { glow: active, r: 16 });
      text(s.title, cx, 490, { size: fit(s.title, D.stageW - 16, 26), align: 'center' });
      s.items.forEach((it, k) => {
        text('- ' + it, s.x + 12, 566 + k * 26, { size: fit('- ' + it, D.stageW - 18, 18) });
      });
      ctx.restore();
    });
    if (i < 5) arrow([[s.x + D.stageW + 2, D.lane], [s.x + 188, D.lane]], P(L, 4.9 + i * 0.12, 0.2), { headSize: 7, width: 2.2 });
    // registry links
    arrow([[cx, 673], [cx, 693]], P(L, 6.0 + i * 0.08, 0.25), { both: true, headSize: 7, width: 2 });
  });

  // --- registry
  pop(P(L, 5.6, 0.6), 780, 742, () => {
    box(225, 695, 1110, 95, C.red, { r: 14 });
    text('Worker Registry (In-memory)', 780, 724, { size: 28, align: 'center' });
    const live = journey && L > J0 + 0.5;
    if (!live) {
      text('Live view of workers: health, load, loaded models, KV usage  |  Updated via health probes', 780, 764, { size: 19, align: 'center' });
    } else {
      const k = Math.floor((L - J0) * 1.2);
      const load1 = 3 + (k % 3);
      const kv1 = 41 + (k * 7) % 19;
      const load2 = 5 + ((k + 1) % 2);
      withAlpha(1, () => text(`vllm-1 ✓ load ${load1} · KV ${kv1}%      vllm-2 ✓ load ${load2} · KV 63%      ollama-1 ✓ phi3 warm      ↻ probe 2 s`,
        780, 764, { size: 19, align: 'center', mono: true, color: C.red[1] }));
    }
  });

  // --- workers
  pop(P(L, 6.6, 0.6), 1705, 625, () => {
    box(1530, 330, 350, 590, C.mint, { dashed: true, r: 22, roughness: 0.8 });
    text('Model Workers', 1705, 368, { size: 30, align: 'center' });
    text('(Stateful, GPU)', 1705, 401, { size: 24, align: 'center' });
  });
  D.workers.forEach((w, j) => {
    const glow = journey && j === 0 ? win(L, WORKER_ARRIVE - 0.2, STREAM_START + 5.5, 0.3) : 0;
    pop(P(L, 7.0 + j * 0.4, 0.6), 1705, w.y + 67, () => {
      box(1555, w.y, 300, 135, w.pal, { glow, r: 16 });
      text(w.title, 1705, w.y + 32, { size: 27, align: 'center' });
      text(w.sub, 1705, w.y + 68, { size: 19, align: 'center' });
      text(w.api, 1705, w.y + 103, { size: 17, align: 'center', mono: true });
    });
    arrow(forwardPath(j), P(L, 8.4 + j * 0.2, 0.5), { width: 2.4 });
  });
  stepBubble(6, 1442, 392, P(L, 12.8, 0.5) + (journey ? 0.0 : 0));
  withAlpha(P(L, 13.0, 0.5), () => {
    ['Forward request', 'to chosen worker', '(TTFT timeout)'].forEach((s, k) => text(s, 1442, 432 + k * 22, { size: 19, align: 'center' }));
  });

  // --- redis
  pop(P(L, 9.2, 0.6), 465, 955, () => {
    box(330, 870, 270, 170, C.red, { r: 16 });
    text('Redis', 465, 900, { size: 30, align: 'center' });
    bullets(['Response cache', 'Rate limits (Lua)', 'Shared by replicas'], 352, 945, { size: 20, lineHeight: 30 });
  });
  arrow([[555, 823], [555, 846], [465, 846], [465, 868]], P(L, 9.4, 0.4), { both: true, width: 2.2, headSize: 9 });

  // --- observability
  pop(P(L, 9.9, 0.6), 920, 955, () => {
    box(660, 860, 520, 190, C.lavender, { dashed: true, r: 18, roughness: 0.8 });
    text('Observability Stack', 920, 886, { size: 26, align: 'center', color: C.lavender[1] });
  });
  const obs = [
    [680, 908, 'Prometheus (metrics)', C.green], [930, 908, 'OpenTelemetry (traces)', C.blue],
    [680, 985, 'Grafana (dashboards)', C.red], [930, 985, 'Jaeger / Tempo', C.purple],
  ];
  obs.forEach(([x, y, s, pal], k) => {
    pop(P(L, 10.1 + k * 0.18, 0.5), x + 115, y + 25, () => {
      box(x, y, 230, 50, pal, { r: 12 });
      text(s, x + 115, y + 26, { size: fit(s, 214, 20), align: 'center' });
    });
  });
  arrow([[795, 959], [795, 983]], P(L, 10.8, 0.2), { headSize: 7, width: 2 });
  arrow([[1045, 959], [1045, 983]], P(L, 10.8, 0.2), { headSize: 7, width: 2 });
  arrow([[880, 823], [880, 857]], P(L, 11.0, 0.3), { dashed: true, color: C.lavender[1], headSize: 9 });
  withAlpha(P(L, 11.1, 0.5), () => text('metrics · traces · JSON logs (one request id)', 896, 841, { size: 19, color: C.lavender[1] }));

  // --- admin
  pop(P(L, 11.5, 0.6), 1190, 200, () => {
    box(1050, 150, 280, 100, C.red, { r: 16 });
    text('Admin / Control Plane', 1190, 186, { size: fit('Admin / Control Plane', 260, 26), align: 'center' });
    text('(optional)', 1190, 222, { size: 22, align: 'center' });
  });
  pop(P(L, 11.8, 0.6), 1620, 190, () => {
    box(1420, 105, 400, 170, C.lavender, { dashed: true, r: 16, roughness: 0.8 });
    bullets(['Dynamic config (models, limits)', 'Worker registration', 'API key management', 'Workers pull config'], 1445, 140, { size: 22, lineHeight: 33 });
  });
  arrow([[1332, 200], [1416, 192]], P(L, 12.0, 0.3), { dashed: true, color: C.lavender[1] });
  arrow([[1250, 370], [1250, 253]], P(L, 12.1, 0.5), { dashed: true, color: C.lavender[1], both: true });
  withAlpha(P(L, 12.3, 0.5), () => text('Admin API (HTTP)', 1264, 312, { size: 20, color: C.lavender[1] }));

  // --- health probes
  arrow([[1760, 922], [1760, 1030], [1210, 1030], [1210, 965], [1184, 965]], P(L, 12.3, 0.7), { dashed: true, color: C.faint, width: 2.2 });
  withAlpha(P(L, 12.6, 0.5), () => text('health probes  (/health, /metrics, /api/ps)', 1485, 1052, { size: 19, align: 'center', color: C.soft }));

  // --- step bubbles
  stepBubble(1, 105, 400, P(L, 12.8, 0.5));
  withAlpha(P(L, 12.9, 0.5), () => {
    ['Chat request', '(API key, prompt,', 'model, params)'].forEach((s, k) => text(s, 105, 446 + k * 25, { size: 20, align: 'center' }));
  });
  arrow([[30, D.lane], [218, D.lane]], P(L, 13.0, 0.4));
  stepBubble(7, 622, 326, P(L, 13.1, 0.5));
  withAlpha(P(L, 13.2, 0.5), () => text('stream tokens', 592, 326, { size: 20, align: 'right', color: C.stream }));

  if (!journey) return;

  // ===== journey =====
  // request packet
  let px = null;
  let py = null;
  if (L >= J0 && L < stageArrive(0)) {
    const u = E.inOut(P(L, J0, 0.6));
    px = lerp(30, D.stages[0].x + 86, u);
    py = D.lane;
  }
  for (let i = 0; i < 6; i++) {
    const t0 = stageArrive(i);
    if (L >= t0 && L < t0 + STAGE_DT) {
      const leave = t0 + STAGE_DT - 0.5;
      const cx = D.stages[i].x + 86;
      if (L < leave || i === 5) {
        px = cx;
        py = D.lane + Math.sin((L - t0) * 6) * 2;
      } else {
        px = lerp(cx, D.stages[i + 1].x + 86, E.inOut(P(L, leave, 0.5)));
        py = D.lane;
      }
    }
    // callout tag
    const ca = win(L, t0 + 0.15, t0 + STAGE_DT + 0.05, 0.25);
    if (ca > 0) {
      const cx = D.stages[i].x + 86;
      pop(Math.min(1, P(L, t0 + 0.15, 0.4)), cx, 432, () => withAlpha(ca, () => tag(callouts[i], cx, 432, D.stages[i].pal, { size: 20, align: 'center' })));
    }
  }
  const leaveRes = stageArrive(5) + 1.6;
  if (L >= leaveRes && L < WORKER_ARRIVE + 0.1) {
    const p = pointAt([[D.stages[5].x + 86, D.lane], ...forwardPath(0)], E.inOut(P(L, leaveRes, WORKER_ARRIVE - leaveRes)));
    px = p[0];
    py = p[1];
  }
  if (px !== null) packet(px, py, C.req, 11, { label: L < stageArrive(0) ? 'POST /v1/chat/completions' : '' });

  // worker: prefix cache hit
  const wa = win(L, WORKER_ARRIVE, STREAM_START + 4.5, 0.3);
  if (wa > 0) {
    pop(Math.min(1, P(L, WORKER_ARRIVE, 0.4)), 1705, 565, () => withAlpha(wa, () => tag('KV prefix 94% cached → prefill skipped', 1705, 567, ['#ebfbee', C.hit], { size: 18, align: 'center' })));
  }
  // token stream back to the client
  if (L > STREAM_START) {
    const r = rng(11);
    for (let k = 0; k < 90; k++) {
      const born = STREAM_START + k * 0.075;
      const u = (L - born) / 1.7;
      const jitter = r() * 6 - 3;
      if (u > 0 && u < 1) {
        const [x, y] = pointAt(STREAM_PATH, u);
        packet(x + jitter, y + jitter, C.stream, 5, { blur: 10 });
      }
    }
  }
  // telemetry dots to observability + registry probes
  const r2 = rng(5);
  for (let k = 0; k < 40; k++) {
    const born = J0 + 0.5 + k * 0.6 + r2() * 0.3;
    const u = (L - born) / 0.9;
    if (u > 0 && u < 1) packet(880, lerp(823, 857, u), C.lavender[1], 4, { blur: 8 });
    const u2 = (L - born - 0.3) / 2.2;
    if (k % 3 === 0 && u2 > 0 && u2 < 1) {
      const [x, y] = pointAt([[1760, 922], [1760, 1030], [1210, 1030], [1210, 965], [1184, 965]], u2);
      packet(x, y, C.faint, 4, { blur: 6 });
    }
  }
}

// ======================================================================
// Scene 2: KV cache locality
// ======================================================================
const KV = (() => {
  const colors = ['#e03131', '#1971c2', '#2f9e44', '#f08c00', '#7048e8', '#0c8599', '#d6336c', '#5c940d', '#495057'];
  const r = rng(42);
  const N = 40;
  const reqs = [];
  // Conversations repeat: draw prefixes with locality so later turns reuse earlier prefixes.
  for (let i = 0; i < N; i++) reqs.push({ prefix: Math.floor(r() * 9), t: 2.0 + i * 0.3, rand: Math.floor(r() * 3) });
  function simulate(choose) {
    const lru = [[], [], []]; // most recent first, capacity 3 prefixes
    return reqs.map((q) => {
      const w = choose(q);
      const cache = lru[w];
      const idx = cache.indexOf(q.prefix);
      const hit = idx >= 0;
      let evicted = null;
      if (hit) cache.splice(idx, 1);
      else if (cache.length >= 3) evicted = cache.pop();
      cache.unshift(q.prefix);
      return { ...q, w, hit, evicted, snapshot: lru.map((c) => c.slice()) };
    });
  }
  return {
    colors,
    left: simulate((q) => q.rand),
    right: simulate((q) => q.prefix % 3),
  };
})();
const KV_TRAVEL = 0.7;

function kvPanel(L, x0, title, pal, events, sub) {
  const pw = 840;
  const cx = x0 + pw / 2;
  pop(P(L, 0.3, 0.6), cx, 520, () => {
    box(x0, 150, pw, 770, pal, { dashed: true, r: 22, roughness: 0.7, fillColor: pal[0] });
    text(title, cx, 192, { size: 34, align: 'center', color: pal[1] });
    text(sub, cx, 232, { size: 21, align: 'center', color: C.soft });
  });
  // router
  pop(P(L, 0.6, 0.5), cx, 300, () => {
    box(cx - 110, 268, 220, 66, C.blue, { r: 14 });
    text('Router', cx, 302, { size: 28, align: 'center' });
  });
  const wx = (j) => x0 + 35 + j * 270;
  // which events have arrived by now
  const arrived = events.filter((e) => L >= e.t + KV_TRAVEL);
  const last = arrived[arrived.length - 1];
  const state = last ? last.snapshot : [[], [], []];
  for (let j = 0; j < 3; j++) {
    const x = wx(j);
    const recent = arrived.filter((e) => e.w === j).pop();
    const flash = recent ? 1 - P(L, recent.t + KV_TRAVEL, 0.6) : 0;
    pop(P(L, 0.8 + j * 0.15, 0.5), x + 115, 640, () => {
      box(x, 470, 230, 330, C.gray, { r: 16, glow: flash, glowColor: recent && recent.hit ? C.hit : C.miss });
      text(`GPU ${j + 1}`, x + 115, 500, { size: 26, align: 'center' });
      text('KV cache', x + 115, 780, { size: 18, align: 'center', color: C.faint });
      // 3 slots × 2 rows × 5 blocks
      for (let s = 0; s < 3; s++) {
        const pfx = state[j][s];
        for (let row = 0; row < 2; row++) {
          for (let b = 0; b < 5; b++) {
            const bx = x + 22 + b * 38;
            const by = 530 + s * 78 + row * 33;
            let fill = '#e9ecef';
            if (pfx !== undefined) {
              fill = KV.colors[pfx];
              // fresh prefill: blocks fill in left-to-right
              if (recent && !recent.hit && recent.prefix === pfx && s === 0) {
                const k = P(L, recent.t + KV_TRAVEL, 0.5) * 10;
                if (row * 5 + b >= k) fill = '#e9ecef';
              }
            }
            ctx.save();
            ctx.fillStyle = fill;
            ctx.globalAlpha *= 0.85;
            ctx.fill(new Path2D(`M${bx + 4},${by} h26 q4,0 4,4 v20 q0,4 -4,4 h-26 q-4,0 -4,-4 v-20 q0,-4 4,-4z`));
            ctx.restore();
          }
        }
      }
    });
    if (recent && L < recent.t + KV_TRAVEL + 0.9) {
      const a = 1 - P(L, recent.t + KV_TRAVEL + 0.5, 0.4);
      withAlpha(a, () => tag(recent.hit ? 'HIT · reuse KV' : 'MISS · prefill', x + 115, 448, recent.hit ? ['#ebfbee', C.hit] : ['#fff5f5', C.miss], { size: 19, align: 'center' }));
    }
  }
  // requests in flight
  for (const e of events) {
    const u = (L - e.t) / KV_TRAVEL;
    if (u > 0 && u < 1) {
      const tx = wx(e.w) + 115;
      const pts = curve([cx, 336], [lerp(cx, tx, 0.5), 360], [tx, 465]);
      const [x, y] = pointAt(pts, E.inOut(u));
      packet(x, y, KV.colors[e.prefix], 10, { blur: 14 });
    }
  }
  // counters
  const hits = arrived.filter((e) => e.hit).length;
  const rate = arrived.length ? hits / arrived.length : 0;
  withAlpha(P(L, 1.2, 0.5), () => {
    text('prefix-cache hit rate', x0 + 60, 850, { size: 24, color: C.soft });
    meter(x0 + 330, 838, 330, 24, rate, rate > 0.6 ? C.hit : C.miss);
    text(`${Math.round(rate * 100)}%`, x0 + 690, 850, { size: 36, color: rate > 0.6 ? C.hit : C.miss });
    const recompute = arrived.filter((e) => !e.hit).length * 1600;
    text(`prefill tokens recomputed: ${recompute.toLocaleString('en-US')}`, x0 + 60, 890, { size: 21, color: C.faint });
  });
}

function sceneKV(L) {
  chapter('01', 'KV-cache locality: send repeat prefixes to the same worker', P(L, 0, 0.6));
  kvPanel(L, 60, 'Any available GPU', C.red, KV.left, 'least-loaded / random: every prefix lands everywhere');
  kvPanel(L, 1020, 'Prefix-affinity routing', C.green, KV.right, 'rendezvous hash + sticky conversations');
  const a = P(L, 14.6, 0.6);
  withAlpha(a, () => {
    ctx.save();
    ctx.fillStyle = 'rgba(255,253,248,0.93)';
    ctx.fillRect(0, 930, W, 150);
    ctx.restore();
    text('KV-cache reuse on the same worker is the difference between ~50 and ~500 tok/s per GPU', W / 2, 968, { size: 32, align: 'center' });
    text('measured · scripts/bench-affinity.sh: TTFT p50 1,034 ms → 48 ms · prefix hits 21% → 69% · +39% tok/s', W / 2, 1022, { size: 24, align: 'center', color: C.hit, mono: true });
  });
}

// ======================================================================
// Scene 3: token-level sliding window rate limiting
// ======================================================================
function sceneRateLimit(L) {
  chapter('02', 'Token-level rate limiting · sliding window', P(L, 0, 0.6));
  // Simulated clock: 1 scene second = 10 s of traffic.
  const sim = Math.max(0, (L - 0.8) * 10);
  const axisY = 760;
  const nowX = 1260;
  const pxPerSec = 18; // 60 s window = 1080 px
  const winLeft = nowX - 60 * pxPerSec;
  const tokPx = 5; // px per k tokens
  const LIMIT = 200;
  const reqs = [[0, 48], [6, 36], [14, 30], [22, 24], [31, 28], [39, 20]];
  const DENY_AT = 45;
  const RETRY_AT = 61;
  const BIG = 40;
  const ACTUAL = 22;

  pop(P(L, 0.2, 0.5), 700, axisY, () => {
    roughLine(120, axisY, nowX + 40, axisY, C.ink, { width: 2.4 });
    text('time →', nowX + 60, axisY, { size: 22, color: C.faint });
  });
  withAlpha(P(L, 0.5, 0.5), () => {
    ctx.save();
    ctx.fillStyle = 'rgba(25,113,194,0.06)';
    ctx.fillRect(winLeft, 330, nowX - winLeft, axisY - 330);
    ctx.restore();
    roughLine(winLeft, 330, winLeft, axisY + 20, C.blue[1], { dashed: true });
    roughLine(nowX, 330, nowX, axisY + 20, C.blue[1], { width: 3 });
    text('now', nowX, axisY + 42, { size: 22, align: 'center', color: C.blue[1] });
    text('← window = last 60 s, slides with time →', (winLeft + nowX) / 2, axisY + 42, { size: 22, align: 'center', color: C.blue[1] });
    for (let b = 1; b < 12; b++) {
      const x = winLeft + b * 90;
      roughLine(x, axisY - 6, x, axisY + 6, C.faint, { width: 1.5 });
    }
    text('12 × 5 s buckets', winLeft + 10, 350, { size: 20, color: C.blue[1] });
  });

  let inWindow = 0;
  const bar = (t, k, pal, label) => {
    const x = nowX - (sim - t) * pxPerSec;
    if (x < 100) return;
    const inside = x >= winLeft;
    const born = P(sim, t, 2);
    const h = k * tokPx;
    withAlpha((inside ? 1 : 0.22) * born, () => {
      box(x - 32, axisY - h, 64, h, pal, { r: 8, seed: 31 + t });
      if (label) text(label, x, axisY - h - 18, { size: 20, align: 'center', color: pal[1] });
    });
    if (inside) inWindow += k * born;
  };
  for (const [t, k] of reqs) if (sim >= t) bar(t, k, C.yellow, `${k}k`);

  // The big request: denied at DENY_AT, retried and admitted at RETRY_AT.
  const denied = sim >= DENY_AT && sim < RETRY_AT;
  if (denied) {
    const u = clamp((sim - DENY_AT) / 3);
    const bx = nowX + 30 + 140 * E.out(u);
    const by = axisY - BIG * tokPx - 60 * Math.sin(Math.PI * Math.min(1, u * 1.2));
    box(bx - 32, by, 64, BIG * tokPx, C.red, { r: 8, seed: 77 });
    text(`${BIG}k`, bx, by - 18, { size: 20, align: 'center', color: C.miss });
    ring(bx, by - 52, 14, clamp((sim - DENY_AT) / (RETRY_AT - DENY_AT)), C.miss, 4);
  }
  if (sim >= RETRY_AT) {
    const shrink = clamp((sim - RETRY_AT - 10) / 6);
    const k = lerp(BIG, ACTUAL, E.inOut(shrink));
    bar(RETRY_AT, k, C.green, sim > RETRY_AT + 10 ? `${Math.round(k)}k` : `${BIG}k`);
  }

  // gauge
  withAlpha(P(L, 0.8, 0.5), () => {
    const gx = 1560;
    const top = 330;
    const hMax = 410;
    const scale = hMax / 240;
    text('tokens in window', gx + 60, 300, { size: 24, align: 'center' });
    box(gx, top, 120, hMax + 20, C.gray, { r: 14 });
    const pending = denied ? BIG : 0;
    const h = Math.min(hMax, inWindow * scale);
    ctx.save();
    ctx.globalAlpha *= 0.75;
    ctx.fillStyle = C.yellow[1];
    ctx.fill(new Path2D(`M${gx + 12},${top + 10 + hMax - h} h96 v${h} h-96z`));
    if (pending) {
      const ph = Math.min(hMax - h, pending * scale);
      ctx.fillStyle = C.miss;
      ctx.globalAlpha *= 0.5 + 0.3 * Math.sin(L * 10);
      ctx.fill(new Path2D(`M${gx + 12},${top + 10 + hMax - h - ph} h96 v${ph} h-96z`));
    }
    ctx.restore();
    const ly = top + 10 + hMax - LIMIT * scale;
    roughLine(gx - 14, ly, gx + 134, ly, C.miss, { width: 2.6 });
    text('200k limit', gx + 145, ly, { size: 20, color: C.miss });
    text(`${Math.round(inWindow + pending)}k`, gx + 60, 790, { size: 36, align: 'center', color: inWindow + pending > LIMIT ? C.miss : C.ink });
    text('/ 200k tokens', gx + 60, 828, { size: 21, align: 'center', color: C.faint });
  });

  // annotations
  const dA = win(sim, DENY_AT + 0.5, RETRY_AT + 4, 3);
  withAlpha(dA, () => {
    tag('429 · Retry-After: 15 s', 980, 250, ['#fff5f5', C.miss], { size: 26 });
    tag('x-ratelimit-remaining-tokens: 14k', 980, 306, ['#fff5f5', C.miss], { size: 18, mono: true });
    text('requests / min:    7 of 600   ✓', 150, 236, { size: 25, color: C.hit });
    text('tokens / min:   226k of 200k   ✗', 150, 278, { size: 25, color: C.miss });
  });
  withAlpha(win(sim, RETRY_AT, RETRY_AT + 9, 2), () => tag('✓ oldest 48k slid out → 178k ≤ 200k → admitted', 150, 260, ['#ebfbee', C.hit], { size: 23 }));
  withAlpha(win(sim, RETRY_AT + 10, 200, 2), () => tag('stream done: actual 22k → 18k refunded to the window', 150, 260, ['#e7f5ff', C.blue[1]], { size: 23 }));
  caption('budget = prompt + max_tokens · reconciled to actual usage · Redis + Lua across replicas', P(L, 1.0, 0.5) * (1 - P(L, 10.4, 0.4)));
}

// ======================================================================
// Scene 4: semantic cache, batcher, singleflight
// ======================================================================
function sceneCacheBatch(L) {
  chapter('03', 'Semantic cache · dynamic batching · singleflight', P(L, 0, 0.6));
  // ---- semantic cache plane
  pop(P(L, 0.3, 0.6), 490, 520, () => {
    box(90, 160, 800, 740, C.pink, { dashed: true, r: 22, roughness: 0.7 });
    text('embedding space (final user turn)', 490, 196, { size: 26, align: 'center', color: C.pink[1] });
    ctx.save();
    ctx.strokeStyle = 'rgba(194,37,92,0.12)';
    ctx.lineWidth = 1.5;
    for (let i = 1; i < 6; i++) {
      ctx.beginPath(); ctx.moveTo(90 + i * 133, 220); ctx.lineTo(90 + i * 133, 880); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(90, 220 + i * 110); ctx.lineTo(890, 220 + i * 110); ctx.stroke();
    }
    ctx.restore();
    text('LSH buckets', 860, 870, { size: 18, align: 'right', color: C.pink[1] });
  });
  const pts = [
    [330, 360, 'How do I reverse a list in Python?'],
    [520, 600, 'Explain KV cache reuse in vLLM'],
    [250, 730, 'What is the capital of France?'],
    [560, 270, 'Write a haiku about GPUs'],
  ];
  pts.forEach(([x, y, s], i) => pop(P(L, 0.7 + i * 0.2, 0.4), x, y, () => {
    packet(x, y, C.pink[1], 9, { blur: 10 });
    text(s, x + 18, y + 26, { size: 19, color: C.soft });
  }));
  // query 1: hit
  const q1 = P(L, 1.8, 0.8);
  if (q1 > 0) {
    const x = lerp(520, 356, E.out(q1));
    const y = lerp(140, 326, E.out(q1));
    withAlpha(P(L, 2.6, 0.4), () => {
      ctx.save();
      ctx.setLineDash([8, 6]);
      ctx.strokeStyle = C.hit;
      ctx.lineWidth = 2.2;
      ctx.beginPath();
      ctx.arc(330, 360, 80, 0, Math.PI * 2);
      ctx.stroke();
      ctx.restore();
    });
    packet(x, y, C.req, 10, { label: q1 < 1 ? 'how do i reverse a list in python' : '' });
    pop(P(L, 2.8, 0.4), 330, 255, () => tag('cos 0.97 ≥ 0.95 → HIT · replay SSE · 0 GPU', 125, 255, ['#ebfbee', C.hit], { size: 20 }));
  }
  // query 2: miss
  const q2 = P(L, 4.4, 0.8);
  if (q2 > 0) {
    const x = lerp(600, 470, E.out(q2));
    const y = lerp(140, 470, E.out(q2));
    packet(x, y, C.req, 10, { label: q2 < 1 ? 'How do I sort a list in Python?' : '' });
    withAlpha(P(L, 5.2, 0.3), () => {
      roughLine(470, 470, 340, 370, C.miss, { dashed: true });
      tag('cos 0.81 → miss → generate, then cache', 480, 500, ['#fff5f5', C.miss], { size: 20 });
    });
  }
  withAlpha(P(L, 6.0, 0.5), () => paragraph('Only within an identical model + params + conversation namespace; exact tier first.', 120, 820, 740, { size: 21, color: C.soft }));

  // ---- batcher
  const bx = 990;
  pop(P(L, 0.5, 0.6), 1415, 340, () => {
    box(bx, 160, 850, 360, C.purple, { dashed: true, r: 22, roughness: 0.7 });
    text('Dynamic batcher (per model)', 1415, 196, { size: 26, align: 'center', color: C.purple[1] });
  });
  const sizes = [1, 2, 4, 8, 16];
  const phaseBurst = L > 3.0;
  const target = phaseBurst ? 4 : 1;
  withAlpha(P(L, 0.9, 0.4), () => {
    text('2ⁿ target from arrival rate:', bx + 30, 250, { size: 21, color: C.soft });
    let tx = bx + 330;
    sizes.forEach((s) => {
      const on = s === target;
      tag(String(s), tx, 250, on ? C.purple : ['#ffffff', '#adb5bd'], { size: 20, shadow: false });
      tx += textWidth(String(s), 20) + 44;
    });
  });
  // window box with timer
  const wx0 = 1240;
  withAlpha(P(L, 1.0, 0.4), () => {
    box(wx0, 300, 300, 120, C.gray, { r: 14 });
    for (let k = 0; k < 4; k++) {
      ctx.save();
      ctx.strokeStyle = '#adb5bd';
      ctx.setLineDash([5, 5]);
      ctx.strokeRect(wx0 + 22 + k * 66, 335, 52, 52);
      ctx.restore();
    }
    text('window ≤ 20 ms', wx0 + 150, 445, { size: 19, align: 'center', color: C.faint });
  });
  // phase A: single request passes straight through (t 1.4 - 2.6)
  const a = P(L, 1.4, 1.2);
  if (a > 0 && a < 1) packet(lerp(bx + 40, 1780, a), 360, C.purple[1], 10, { blur: 14 });
  withAlpha(win(L, 1.5, 3.0, 0.3), () => text('low traffic → target 1 → no wait', wx0 + 150, 490, { size: 21, align: 'center', color: C.hit }));
  // phase B: four requests fill slots, timer, flush together
  const timer = P(L, 3.4, 1.2);
  for (let k = 0; k < 4; k++) {
    const arr = 3.4 + k * 0.28;
    const u = P(L, arr, 0.4);
    const flush = P(L, 4.8, 0.7);
    if (u > 0 && flush < 1) {
      const sx = wx0 + 48 + k * 66;
      const x = flush > 0 ? lerp(sx, 1780, E.inOut(flush)) : lerp(bx + 40, sx, E.out(u));
      const y = flush > 0 ? lerp(361, 360 + (k - 1.5) * 16, flush) : 361;
      packet(x, y, KV.colors[1], 10, { blur: 14 });
    }
  }
  if (L > 3.3 && L < 5.6) {
    ring(wx0 + 345, 352, 20, timer, C.purple[1], 5);
    withAlpha(win(L, 3.4, 5.6, 0.3), () => text(`${Math.round(timer * 8)} ms`, wx0 + 345, 395, { size: 18, align: 'center', color: C.purple[1] }));
  }
  withAlpha(win(L, 4.8, 7.0, 0.3), () => text('flush ×4 → router → same worker (shared prefix)', wx0 + 150, 490, { size: 21, align: 'center', color: C.purple[1] }));
  pop(P(L, 1.2, 0.4), 1790, 360, () => {
    box(1760, 320, 66, 80, C.blue, { r: 12 });
    text('GPU', 1793, 361, { size: 18, align: 'center' });
  });

  // ---- singleflight
  pop(P(L, 6.2, 0.6), 1415, 735, () => {
    box(bx, 560, 850, 340, C.lavender, { dashed: true, r: 22, roughness: 0.7 });
    text('singleflight: identical in-flight requests share one generation', 1415, 596, { size: 23, align: 'center', color: C.lavender[1] });
  });
  const gx = 1340;
  const gy = 760;
  pop(P(L, 6.5, 0.4), gx, gy, () => {
    box(gx - 80, gy - 45, 160, 90, C.sky, { r: 14 });
    text('gateway', gx, gy + 1, { size: 22, align: 'center' });
  });
  pop(P(L, 6.6, 0.4), 1730, gy, () => {
    box(1660, gy - 45, 140, 90, C.blue, { r: 14 });
    text('worker', 1730, gy + 1, { size: 22, align: 'center' });
  });
  const clients = [[1060, 670], [1060, 760], [1060, 850]];
  clients.forEach(([x, y], k) => {
    pop(P(L, 6.6 + k * 0.1, 0.4), x, y, () => {
      circle(x, y, 44, C.orange);
      text('C' + (k + 1), x, y + 1, { size: 18, align: 'center' });
    });
    const u = P(L, 7.0 + k * 0.15, 0.6);
    if (u > 0 && u < 1) {
      const [px, py] = pointAt([[x + 22, y], [gx - 80, gy]], E.inOut(u));
      packet(px, py, C.req, 8, { blur: 10 });
    }
  });
  const up = P(L, 7.9, 0.6);
  if (up > 0 && up < 1) packet(lerp(gx + 80, 1660, up), gy, C.req, 10, { label: '1 request' });
  arrow([[gx + 82, gy - 12], [1656, gy - 12]], P(L, 7.9, 0.5), { width: 2.2, headSize: 9 });
  if (L > 8.6) {
    for (let k = 0; k < 30; k++) {
      const born = 8.6 + k * 0.09;
      const u = (L - born) / 0.9;
      if (u > 0 && u < 1) {
        const target = clients[k % 3];
        const pts = u < 0.5 ? [[1660, gy + 12], [gx + 80, gy + 12]] : [[gx - 80, gy], [target[0] + 22, target[1]]];
        const [px, py] = pointAt(pts, u < 0.5 ? u * 2 : (u - 0.5) * 2);
        packet(px, py, C.stream, 4.5, { blur: 8 });
      }
    }
  }
  pop(P(L, 9.2, 0.4), 1500, 860, () => tag('1 generation · 3 identical streams', 1500, 862, ['#ebfbee', C.hit], { size: 21, align: 'center' }));
}

// ======================================================================
// Scene 5: resilience
// ======================================================================
function sceneResilience(L) {
  chapter('04', 'Partial failures should not cascade', P(L, 0, 0.6));
  const G = [180, 410, 280, 240];
  const workers = [[1180, 200], [1180, 450], [1180, 700]];
  const failStart = 3.0;
  const failCount = (t) => [3.5, 4.0, 4.5, 5.0, 5.5].filter((x) => t >= x + 0.6).length;
  const opened = L >= 5.5 + 0.6;
  const recovered = L >= 8.6;
  const halfOpen = L >= 8.6 && L < 9.6;
  const closedAgain = L >= 9.6;
  pop(P(L, 0.3, 0.5), G[0] + G[2] / 2, G[1] + G[3] / 2, () => {
    box(G[0], G[1], G[2], G[3], C.green, { r: 18 });
    text('Gateway', G[0] + G[2] / 2, G[1] + 50, { size: 32, align: 'center' });
    bullets(['TTFT timeout 15 s', 'retries: full jitter', 'breaker per worker', 'failover'], G[0] + 25, G[1] + 105, { size: 21, lineHeight: 30 });
  });
  workers.forEach(([x, y], j) => {
    const sick = j === 1 && L >= failStart && !recovered;
    const pal = sick ? C.red : C.blue;
    const dim = j === 1 && opened && !recovered ? 0.45 : 1;
    pop(P(L, 0.5 + j * 0.15, 0.5), x + 160, y + 75, () => withAlpha(dim, () => {
      box(x, y, 320, 150, pal, { r: 16, glow: sick ? 0.5 + 0.3 * Math.sin(L * 8) : 0, glowColor: C.miss });
      text(`vLLM worker ${j + 1}`, x + 160, y + 50, { size: 28, align: 'center' });
      text(sick ? '5xx · CUDA error' : j === 2 && L > 6.4 && L < 8.2 ? 'slow: no first token' : 'healthy', x + 160, y + 98, { size: 22, align: 'center', color: sick ? C.miss : C.soft });
    }));
    // breaker pill
    let state = 'CLOSED';
    let pal2 = ['#ebfbee', C.hit];
    if (j === 1 && opened && !recovered) { state = 'OPEN'; pal2 = ['#fff5f5', C.miss]; }
    if (j === 1 && halfOpen) { state = 'HALF-OPEN'; pal2 = ['#fff9db', C.yellow[1]]; }
    withAlpha(P(L, 1.0, 0.4), () => {
      tag('breaker ' + state, x + 350, y + 75, pal2, { size: 20 });
      if (j === 1 && L > 3.4 && !closedAgain) text(`failures ${Math.min(5, failCount(L))}/5`, x + 360, y + 118, { size: 18, color: C.miss });
    });
  });
  const gx = G[0] + G[2];
  const gy = G[1] + G[3] / 2;
  const to = (j) => [workers[j][0], workers[j][1] + 75];
  const trip = (from, toPt, u, color, label) => {
    const pts = curve(from, [lerp(from[0], toPt[0], 0.5), lerp(from[1], toPt[1], 0.5) - 60], toPt);
    const [x, y] = pointAt(pts, E.inOut(u));
    packet(x, y, color, 9, { blur: 12, label });
  };
  // routes
  withAlpha(P(L, 0.8, 0.6) * 0.5, () => {
    for (let j = 0; j < 3; j++) {
      const toPt = to(j);
      const pts = curve([gx, gy], [lerp(gx, toPt[0], 0.5), lerp(gy, toPt[1], 0.5) - 60], toPt);
      arrow(pts, 1, { dashed: true, color: '#adb5bd', width: 2, headSize: 10 });
    }
  });
  // traffic schedule: [start, worker, outcome]
  const sched = [];
  for (let k = 0; k < 6; k++) sched.push([0.8 + k * 0.33, k % 3, 'ok']);
  [3.5, 4.0, 4.5, 5.0, 5.5].forEach((t, k) => sched.push([t, 1, 'fail', k % 2 === 0 ? 0 : 2]));
  for (let k = 0; k < 6; k++) {
    const t = 6.2 + k * 0.33;
    sched.push([t, k % 2 === 0 || (t > 6.3 && t < 8.3) ? 0 : 2, 'ok']);
  }
  sched.push([6.5, 2, 'slow', 0]);
  sched.push([8.9, 1, 'probe']);
  for (let k = 0; k < 6; k++) sched.push([9.9 + k * 0.3, k % 3, 'ok']);
  for (const [t0, j, kind, alt] of sched) {
    const u = P(L, t0, 0.6);
    if (kind === 'slow') {
      if (u > 0 && L < 8.2) {
        if (u < 1) trip([gx, gy], to(j), u, C.req);
        else {
          const [wx, wy] = to(j);
          packet(wx - 14, wy, C.req, 9);
          ring(wx - 14, wy, 22, P(L, t0 + 0.6, 1.0), C.miss, 4);
          if (L > t0 + 1.6) withAlpha(1 - P(L, 7.9, 0.3), () => tag('first-token timeout → cancel', wx - 330, wy + 34, ['#fff5f5', C.miss], { size: 18 }));
        }
      }
      const u2 = P(L, t0 + 1.7, 0.7);
      if (u2 > 0 && u2 < 1) trip(to(j), to(alt), u2, C.req, 'failover');
      if (u2 >= 1 && L < t0 + 3.2) checkmark(to(alt)[0] - 26, to(alt)[1], 26, C.hit, P(L, t0 + 2.4, 0.3));
      continue;
    }
    if (u > 0 && u < 1) trip([gx, gy], to(j), u, kind === 'probe' ? C.yellow[1] : C.req, kind === 'probe' ? 'probe' : '');
    if (u >= 1) {
      const [wx, wy] = to(j);
      const age = L - t0 - 0.6;
      if (kind === 'ok' || kind === 'probe') {
        if (age < 0.7) checkmark(wx - 26, wy, 26, C.hit, P(age, 0, 0.3));
      } else if (kind === 'fail') {
        if (age < 0.5) cross(wx - 26, wy, 24, C.miss, P(age, 0, 0.25));
        const u3 = P(L, t0 + 0.9, 0.7);
        if (u3 > 0 && u3 < 1) trip(to(j), to(alt), u3, C.req, '');
        if (t0 === 3.5) withAlpha(win(L, t0 + 0.8, t0 + 2.2, 0.2), () => tag('retry on another worker · +23 ms jitter', 640, 330, ['#fff4e6', C.req], { size: 20 }));
        if (u3 >= 1 && L < t0 + 2.2) checkmark(to(alt)[0] - 26, to(alt)[1], 26, C.hit, P(L, t0 + 1.6, 0.3));
      }
    }
  }
  // state machine
  const sm = [[760, 935, 'CLOSED', ['#ebfbee', C.hit]], [1140, 935, 'OPEN', ['#fff5f5', C.miss]], [1520, 935, 'HALF-OPEN', ['#fff9db', C.yellow[1]]]];
  const activeIdx = !opened || closedAgain ? 0 : halfOpen ? 2 : 1;
  withAlpha(P(L, 1.4, 0.5), () => {
    sm.forEach(([x, y, s, pal], k) => {
      const on = k === activeIdx;
      box(x - 105, y - 34, 210, 68, on ? pal : ['#ffffff', '#ced4da'], { r: 34, glow: on ? 0.6 : 0, glowColor: pal[1] });
      text(s, x, y + 1, { size: 24, align: 'center', color: on ? pal[1] : C.faint });
    });
    arrow([[868, 925], [1032, 925]], 1, { width: 2, headSize: 9, color: C.soft });
    text('5× 5xx or slow TTFT', 950, 895, { size: 18, align: 'center', color: C.soft });
    arrow([[1248, 925], [1412, 925]], 1, { width: 2, headSize: 9, color: C.soft });
    text('15 s cool-down', 1330, 895, { size: 18, align: 'center', color: C.soft });
    arrow(curve([1520, 972], [1140, 1050], [760, 972]), 1, { width: 2, headSize: 9, color: C.soft });
    text('probe succeeds', 1140, 1022, { size: 18, align: 'center', color: C.soft });
  });
}

// ======================================================================
// Scene 6: observability
// ======================================================================
function sceneObservability(L) {
  chapter('05', 'Observability that actually helps debugging', P(L, 0, 0.6));
  // trace waterfall
  pop(P(L, 0.3, 0.5), 570, 410, () => {
    box(80, 160, 980, 520, C.gray, { r: 18, fillColor: '#ffffff' });
    text('trace 4bf92f35… · POST /v1/chat/completions', 110, 200, { size: 24, mono: true, color: C.purple[1] });
  });
  const rows = [
    ['POST /v1/chat/completions', 0, 1150, '#adb5bd'],
    ['gateway.chat', 4, 1146, C.blue[1]],
    ['ratelimit.reserve', 6, 9, C.yellow[1]],
    ['cache.lookup', 9, 12, C.pink[1]],
    ['batcher.wait', 12, 20, C.purple[1]],
    ['worker.attempt vllm-1', 21, 1142, C.green[1]],
  ];
  const sx = 470;
  const scale = 500 / 1150;
  rows.forEach(([name, a, b, color], k) => {
    const y = 260 + k * 62;
    const p = P(L, 0.8 + k * 0.35, 0.8);
    withAlpha(P(L, 0.6 + k * 0.35, 0.3), () => text(name, 110 + (k > 0 ? 18 : 0) + (k > 1 ? 18 : 0), y, { size: 21, mono: true }));
    if (p > 0) {
      const w = Math.max(6, (b - a) * scale * E.out(p));
      ctx.save();
      ctx.fillStyle = color;
      ctx.globalAlpha *= 0.85;
      ctx.fill(new Path2D(`M${sx + a * scale},${y - 14} h${w} v28 h-${w}z`));
      ctx.restore();
      if (p > 0.9) text(`${b - a} ms`, sx + a * scale + w + 10, y, { size: 17, color: C.faint });
    }
  });
  // events
  const ev = [[70, 'first_token · 70 ms'], [1142, 'stream_complete']];
  ev.forEach(([ms, label], k) => {
    const p = P(L, 3.2 + k * 0.5, 0.4);
    pop(p, sx + ms * scale, 572, () => {
      ctx.save();
      ctx.fillStyle = C.req;
      ctx.translate(sx + ms * scale, 572);
      ctx.rotate(Math.PI / 4);
      ctx.fillRect(-8, -8, 16, 16);
      ctx.restore();
      text(label, sx + ms * scale + (k ? -10 : 14), 614, { size: 18, align: k ? 'right' : 'left', color: C.req });
    });
  });
  // charts
  const charts = [
    ['completion tokens / s', [C.blue[1]], (x, s, n) => 520 + 260 * x + 90 * Math.sin(x * 11) + 60 * n],
    ['TTFT p50 · p99 (affinity on at ½)', [C.hit, C.miss], (x, s, n) => (s ? (x < 0.5 ? 1050 : 160) + 120 * n : (x < 0.5 ? 620 : 50) + 40 * n)],
    ['queue depth by worker', [C.purple[1], C.yellow[1]], (x, s, n) => (s ? 6 + 4 * Math.sin(x * 6 + 1) : 3 + 2 * Math.sin(x * 8)) + 1.5 * n],
  ];
  charts.forEach(([title, colors, fn], k) => {
    const x0 = 1110;
    const y0 = 160 + k * 177;
    pop(P(L, 0.6 + k * 0.25, 0.5), x0 + 365, y0 + 77, () => {
      box(x0, y0, 730, 155, C.gray, { r: 14, fillColor: '#ffffff' });
      text(title, x0 + 20, y0 + 26, { size: 21, color: C.soft });
    });
    const p = P(L, 1.2 + k * 0.3, 4.5);
    colors.forEach((color, s) => {
      const vals = [];
      const noise = rng(17 + k * 7 + s);
      for (let i = 0; i <= 60; i++) vals.push(fn(i / 60, s, noise() - 0.5));
      const lo = Math.min(...vals);
      const hi = Math.max(...vals);
      ctx.save();
      ctx.strokeStyle = color;
      ctx.lineWidth = 3;
      ctx.lineJoin = 'round';
      ctx.beginPath();
      const n = Math.floor(60 * p);
      for (let i = 0; i <= n; i++) {
        const x = x0 + 25 + i * (680 / 60);
        const y = y0 + 140 - ((vals[i] - lo) / (hi - lo || 1)) * 90;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }
      ctx.stroke();
      ctx.restore();
    });
  });
  // log line
  const log = '{"level":"INFO","msg":"request","request_id":"9f2c41e0…","trace_id":"4bf92f35…","model":"llama3-8b","worker":"vllm-1","route":"affinity_sticky","status":200,"ttft_ms":70}';
  pop(P(L, 4.2, 0.5), 960, 800, () => {
    ctx.save();
    ctx.fillStyle = '#1e1e1e';
    ctx.fill(new Path2D(`M100,720 h1720 q20,0 20,20 v120 q0,20 -20,20 h-1720 q-20,0 -20,-20 v-120 q0,-20 20,-20z`));
    ctx.restore();
    text('$ gateway logs | jq -c', 110, 750, { size: 20, mono: true, color: '#69db7c' });
    const shown = typed(log, P(L, 4.6, 2.2));
    const cut = log.indexOf(',"model"') + 1;
    text(shown.slice(0, cut), 110, 795, { size: 20, mono: true, color: '#f8f9fa' });
    text(shown.slice(cut), 110, 832, { size: 20, mono: true, color: '#f8f9fa' });
  });
  withAlpha(P(L, 7.0, 0.5), () => {
    tag('same request_id + trace_id in every hop: gateway → router → batcher → worker', 960, 925, ['#fff9db', C.yellow[1]], { size: 22, align: 'center' });
  });
  caption('Prometheus + Grafana · OpenTelemetry → Jaeger/Tempo · structured JSON logs', P(L, 1.0, 0.5) * (1 - P(L, 9.6, 0.4)));
}

// ======================================================================
// Scene 7: scaling
// ======================================================================
function sceneScale(L) {
  chapter('06', 'Why this scales', P(L, 0, 0.6));
  // LB and gateway replicas
  pop(P(L, 0.3, 0.5), 220, 540, () => {
    box(120, 470, 200, 140, C.orange, { r: 16 });
    text('Load', 220, 525, { size: 28, align: 'center' });
    text('balancer', 220, 560, { size: 28, align: 'center' });
  });
  const reps = [[420, 300], [420, 480], [420, 660]];
  reps.forEach(([x, y], k) => {
    const p = P(L, 0.6 + k * 0.9, 0.6);
    pop(p, x + 120, y + 60, () => {
      box(x, y, 240, 120, C.sky, { r: 16 });
      text(`gateway ${k + 1}`, x + 120, y + 45, { size: 26, align: 'center' });
      text('stateless', x + 120, y + 82, { size: 20, align: 'center', color: C.soft });
    });
    arrow([[322, 540], [418, y + 60]], P(L, 0.8 + k * 0.9, 0.4), { width: 2.2, headSize: 9 });
    if (k > 0) withAlpha(win(L, 0.8 + k * 0.9, 1.9 + k * 0.9, 0.3), () => tag('HPA +1 replica', x + 250, y + 20, ['#ebfbee', C.hit], { size: 18 }));
  });
  // rendezvous ring
  const cx = 1010;
  const cy = 560;
  const R = 200;
  const ownersBefore = (k) => {
    const r = rng(1000 + k);
    const scores = [0, 1, 2].map((w) => r() * [1, 1, 1][w]);
    return scores.indexOf(Math.max(...scores));
  };
  const ownersAfter = (k) => {
    const r = rng(1000 + k);
    const scores = [0, 1, 2].map(() => r());
    const extra = rng(5000 + k)();
    scores.push(extra);
    return scores.indexOf(Math.max(...scores));
  };
  const palW = [C.blue[1], C.green[1], C.purple[1], C.req];
  const added = P(L, 7.2, 1.0);
  withAlpha(P(L, 3.4, 0.5), () => {
    ctx.save();
    ctx.strokeStyle = '#ced4da';
    ctx.lineWidth = 3;
    ctx.beginPath();
    ctx.arc(cx, cy, R, 0, Math.PI * 2);
    ctx.stroke();
    ctx.restore();
    text('prefix keys → owner', cx, cy - 12, { size: 24, align: 'center', color: C.soft });
    text('(rendezvous hash)', cx, cy + 22, { size: 20, align: 'center', color: C.faint });
    let moved = 0;
    for (let k = 0; k < 48; k++) {
      const a = (k / 48) * Math.PI * 2 - Math.PI / 2;
      const before = ownersBefore(k);
      const after = ownersAfter(k);
      const changes = after === 3;
      if (changes) moved++;
      const color = changes && added > (k % 12) / 12 ? palW[3] : palW[before];
      packet(cx + Math.cos(a) * R, cy + Math.sin(a) * R, color, 8, { blur: 6 });
    }
    if (added > 0) withAlpha(added, () => tag(`only ${moved}/48 keys move to the new worker (~1/n)`, cx, cy - R - 50, ['#fff4e6', C.req], { size: 21, align: 'center' }));
  });
  // worker pools
  const wk = [[1440, 230, 'vLLM 1 · Llama 3', C.blue], [1440, 390, 'vLLM 2 · Llama 3', C.green], [1440, 550, 'vLLM 3 · Llama 3', C.purple]];
  withAlpha(P(L, 3.0, 0.5), () => text('Llama 3 worker pool', 1630, 190, { size: 24, align: 'center', color: C.soft }));
  wk.forEach(([x, y, s, pal], k) => pop(P(L, 3.0 + k * 0.2, 0.5), x + 190, y + 55, () => {
    box(x, y, 380, 110, pal, { r: 16 });
    text(s, x + 190, y + 56, { size: 25, align: 'center' });
  }));
  const nw = P(L, 4.5, 0.8);
  slide(nw, 300, 0, () => {
    box(1440, 760, 380, 110, C.orange, { r: 16, glow: win(L, 4.5, 7.5, 0.4) });
    text('vLLM 4 · joins', 1630, 816, { size: 25, align: 'center' });
  });
  const steps = ['① GET /internal/v1/worker-config', '② POST /admin/v1/workers', '③ heartbeat every 10 s'];
  steps.forEach((s, k) => {
    const p = P(L, 5.2 + k * 0.6, 0.5);
    arrow([[1436, 800 + k * 14], [664, 712 + k * 14]], p, { dashed: k === 2, color: C.req, width: 2, headSize: 9 });
    withAlpha(p, () => text(s, 1235, 880 + k * 34, { size: 20, align: 'right', color: C.req, mono: true }));
  });
  caption('gateways scale horizontally · workers scale per model · zero client changes', P(L, 1.0, 0.5) * (1 - P(L, 9.6, 0.4)));
}

// ======================================================================
// Scene 8: outro
// ======================================================================
function sceneOutro(L) {
  slide(P(L, 0.2, 0.8), 0, 20, () => text('Distributed AI Inference Gateway', W / 2, 220, { size: 74, align: 'center' }));
  const ow = textWidth('Distributed AI Inference Gateway', 74);
  highlight(W / 2 - ow / 2 + 20, 268, ow - 40, P(L, 0.8, 0.8));
  withAlpha(P(L, 0.8, 0.6), () => text('github.com/thanhauco/distributed-ai-inference-gateway', W / 2, 330, { size: 30, align: 'center', mono: true, color: C.blue[1] }));
  const items = [
    'OpenAI-compatible API + SSE', 'token-level sliding-window limits',
    'exact + semantic response cache', '2ⁿ dynamic batcher + singleflight',
    'KV-affinity routing, bounded load', 'timeouts · retries · breakers · failover',
    'Prometheus · OpenTelemetry · JSON logs', 'admin plane · self-registering workers',
  ];
  items.forEach((s, k) => {
    const col = k % 2;
    const row = Math.floor(k / 2);
    const x = col === 0 ? 225 : 1015;
    const y = 450 + row * 82;
    const p = P(L, 1.2 + k * 0.18, 0.5);
    withAlpha(p, () => text(s, x + 50, y, { size: 30 }));
    checkmark(x + 12, y, 30, C.hit, p);
  });
  withAlpha(P(L, 3.0, 0.6), () => text('Thanh & Claude · 2026', W / 2, 860, { size: 32, align: 'center', color: C.faint }));
}

// ======================================================================
// Timeline
// ======================================================================
const SCENES = [
  [0, 6, sceneTitle],
  [6, 44, sceneDiagram],
  [44, 62, sceneKV],
  [62, 73, sceneRateLimit],
  [73, 85, sceneCacheBatch],
  [85, 97, sceneResilience],
  [97, 107, sceneObservability],
  [107, 117, sceneScale],
  [117, 123, sceneOutro],
];
const DURATION = 123;

function renderFrame(t) {
  background();
  for (const [a, b, fn] of SCENES) {
    if (t < a || t >= b) continue;
    const alpha = Math.min(P(t, a, 0.45), 1 - P(t, b - 0.45, 0.45));
    withAlpha(alpha, () => fn(t - a));
  }
}
