// Drawing helpers for the motion-graphics video. Everything is a pure
// function of time so any frame can be rendered independently.
/* global rough */

const W = 1920;
const H = 1080;

const C = {
  ink: '#1e1e1e',
  soft: '#495057',
  faint: '#868e96',
  paper: '#fffdf8',
  dot: '#ebe5d6',
  green: ['#d3f9d8', '#2b8a3e'],
  yellow: ['#fff3bf', '#e67700'],
  pink: ['#ffdeeb', '#c2255c'],
  purple: ['#e5dbff', '#5f3dc4'],
  blue: ['#d0ebff', '#1971c2'],
  red: ['#ffc9c9', '#c92a2a'],
  orange: ['#ffe8cc', '#d9480f'],
  lavender: ['#f3f0ff', '#7048e8'],
  mint: ['#ebfbee', '#2f9e44'],
  sky: ['#e7f5ff', '#1c7ed6'],
  gray: ['#f1f3f5', '#495057'],
  hit: '#2f9e44',
  miss: '#e03131',
  stream: '#1c7ed6',
  req: '#f76707',
};

const FONT = 'Inter, "DejaVu Sans", sans-serif';
const MONO = 'Cascadia, "DejaVu Sans Mono", monospace';

let ctx;
let rc;

function init(canvas) {
  ctx = canvas.getContext('2d');
  rc = rough.canvas(canvas);
}

// ---- time ------------------------------------------------------------
const clamp = (v, a = 0, b = 1) => Math.min(b, Math.max(a, v));
const lerp = (a, b, t) => a + (b - a) * t;
const P = (t, start, dur) => clamp((t - start) / dur);
const E = {
  out: (t) => 1 - Math.pow(1 - t, 3),
  in: (t) => t * t * t,
  inOut: (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2),
  back: (t) => {
    const c1 = 1.70158;
    const c3 = c1 + 1;
    return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
  },
};
// window(t, a, b, fade): 0→1 at a, 1→0 at b
const win = (t, a, b, f = 0.4) => Math.min(P(t, a, f), 1 - P(t, b - f, f));

function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---- transforms ------------------------------------------------------
function withAlpha(a, fn) {
  if (a <= 0) return;
  ctx.save();
  ctx.globalAlpha *= clamp(a);
  fn();
  ctx.restore();
}

// Pop in with a slight overshoot around (cx, cy).
function pop(p, cx, cy, fn) {
  if (p <= 0) return;
  const s = p >= 1 ? 1 : 0.55 + 0.45 * E.back(p);
  ctx.save();
  ctx.globalAlpha *= clamp(p * 2.2);
  ctx.translate(cx, cy);
  ctx.scale(s, s);
  ctx.translate(-cx, -cy);
  fn();
  ctx.restore();
}

function slide(p, dx, dy, fn) {
  if (p <= 0) return;
  const e = E.out(p);
  ctx.save();
  ctx.globalAlpha *= clamp(p * 1.6);
  ctx.translate(dx * (1 - e), dy * (1 - e));
  fn();
  ctx.restore();
}

// ---- background ------------------------------------------------------
function background() {
  ctx.fillStyle = C.paper;
  ctx.fillRect(0, 0, W, H);
  ctx.fillStyle = C.dot;
  for (let x = 20; x < W; x += 40) {
    for (let y = 20; y < H; y += 40) {
      ctx.fillRect(x - 1, y - 1, 2.4, 2.4);
    }
  }
}

// ---- shapes ----------------------------------------------------------
function roundPath(x, y, w, h, r) {
  r = Math.min(r, w / 2, h / 2);
  return `M${x + r},${y} L${x + w - r},${y} Q${x + w},${y} ${x + w},${y + r} L${x + w},${y + h - r} Q${x + w},${y + h} ${x + w - r},${y + h} L${x + r},${y + h} Q${x},${y + h} ${x},${y + h - r} L${x},${y + r} Q${x},${y} ${x + r},${y} Z`;
}

function seedOf(x, y, w, h) {
  return (Math.abs(Math.round(x * 7 + y * 13 + w * 3 + h * 5)) % 9973) + 1;
}

// Hand-drawn rounded box. pal = [fill, stroke].
function box(x, y, w, h, pal, o = {}) {
  const opts = {
    fill: o.fill === false ? undefined : (o.fillColor || pal[0]),
    fillStyle: o.fillStyle || 'solid',
    stroke: o.strokeColor || pal[1],
    strokeWidth: o.strokeWidth || 2.2,
    roughness: o.roughness ?? 1.1,
    bowing: 1,
    seed: o.seed || seedOf(x, y, w, h),
  };
  if (o.dashed) opts.strokeLineDash = [12, 9];
  if (o.glow) {
    ctx.save();
    ctx.shadowColor = o.glowColor || pal[1];
    ctx.shadowBlur = 28 * o.glow;
    ctx.strokeStyle = o.glowColor || pal[1];
    ctx.globalAlpha *= 0.85 * o.glow;
    ctx.lineWidth = 5;
    ctx.stroke(new Path2D(roundPath(x - 3, y - 3, w + 6, h + 6, o.r ?? 18)));
    ctx.restore();
  }
  rc.path(roundPath(x, y, w, h, o.r ?? 18), opts);
}

function circle(cx, cy, d, pal, o = {}) {
  rc.circle(cx, cy, d, {
    fill: pal[0], fillStyle: 'solid', stroke: pal[1], strokeWidth: o.strokeWidth || 2.2,
    roughness: o.roughness ?? 1, seed: o.seed || seedOf(cx, cy, d, d),
  });
}

function roughLine(x1, y1, x2, y2, color, o = {}) {
  rc.line(x1, y1, x2, y2, {
    stroke: color, strokeWidth: o.width || 2.4, roughness: o.roughness ?? 1.2,
    seed: o.seed || seedOf(x1, y1, x2, y2),
    strokeLineDash: o.dashed ? [10, 8] : undefined,
  });
}

// Highlighter stroke under text.
function highlight(x, y, w, p, color = 'rgba(255, 212, 59, 0.55)') {
  if (p <= 0) return;
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineCap = 'round';
  ctx.lineWidth = 18;
  ctx.beginPath();
  ctx.moveTo(x, y);
  ctx.lineTo(x + w * E.out(p), y - 3);
  ctx.stroke();
  ctx.restore();
}

// ---- text ------------------------------------------------------------
// Weight follows size unless given: headings bold, box titles semibold,
// labels medium. Mono text stays regular.
function weightFor(size, o = {}) {
  if (o.weight) return o.weight;
  if (o.mono) return 400;
  if (size >= 40) return 700;
  if (size >= 24) return 600;
  return 500;
}

function fontSpec(size, o = {}) {
  return `${weightFor(size, o)} ${size}px ${o.mono ? MONO : FONT}`;
}

function text(str, x, y, o = {}) {
  ctx.save();
  ctx.font = fontSpec(o.size || 28, o);
  ctx.fillStyle = o.color || C.ink;
  ctx.textAlign = o.align || 'left';
  ctx.textBaseline = o.baseline || 'middle';
  if (o.alpha !== undefined) ctx.globalAlpha *= clamp(o.alpha);
  if (o.maxChars !== undefined) str = str.slice(0, Math.max(0, Math.floor(o.maxChars)));
  ctx.fillText(str, x, y);
  ctx.restore();
}

function textWidth(str, size, mono, weight) {
  ctx.save();
  ctx.font = fontSpec(size, { mono, weight });
  const w = ctx.measureText(str).width;
  ctx.restore();
  return w;
}

// Bulleted list; p reveals lines one by one.
function bullets(lines, x, y, o = {}) {
  const lh = o.lineHeight || (o.size || 22) * 1.35;
  lines.forEach((ln, i) => {
    const a = o.p === undefined ? 1 : clamp(o.p * lines.length - i);
    text('- ' + ln, x, y + i * lh, { ...o, alpha: a });
  });
}

// Typewriter.
function typed(str, p) {
  return str.slice(0, Math.floor(str.length * clamp(p)));
}

// Word-wrapped paragraph; returns height used.
function paragraph(str, x, y, maxW, o = {}) {
  const size = o.size || 26;
  const lh = o.lineHeight || size * 1.35;
  const words = str.split(' ');
  let line = '';
  let yy = y;
  for (const w of words) {
    const test = line ? line + ' ' + w : w;
    if (textWidth(test, size, o.mono) > maxW && line) {
      text(line, x, yy, o);
      line = w;
      yy += lh;
    } else {
      line = test;
    }
  }
  if (line) text(line, x, yy, o);
  return yy - y + lh;
}

// Pill-shaped tag.
function tag(str, x, y, pal, o = {}) {
  const size = o.size || 22;
  const w = textWidth(str, size, o.mono) + 28;
  const h = size + 18;
  const left = o.align === 'center' ? x - w / 2 : x;
  ctx.save();
  if (o.shadow !== false) {
    ctx.shadowColor = 'rgba(0,0,0,0.12)';
    ctx.shadowBlur = 10;
    ctx.shadowOffsetY = 3;
  }
  ctx.fillStyle = pal[0];
  ctx.strokeStyle = pal[1];
  ctx.lineWidth = 2;
  const path = new Path2D(roundPath(left, y - h / 2, w, h, h / 2));
  ctx.fill(path);
  ctx.shadowColor = 'transparent';
  ctx.stroke(path);
  ctx.restore();
  text(str, left + w / 2, y + 1, { size, align: 'center', color: o.color || pal[1], mono: o.mono });
  return w;
}

// ---- paths -----------------------------------------------------------
function curve(p0, c, p1, n = 28) {
  const pts = [];
  for (let i = 0; i <= n; i++) {
    const t = i / n;
    pts.push([
      (1 - t) * (1 - t) * p0[0] + 2 * (1 - t) * t * c[0] + t * t * p1[0],
      (1 - t) * (1 - t) * p0[1] + 2 * (1 - t) * t * c[1] + t * t * p1[1],
    ]);
  }
  return pts;
}

function pathLength(pts) {
  let L = 0;
  for (let i = 1; i < pts.length; i++) L += Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
  return L;
}

function pointAt(pts, u) {
  const L = pathLength(pts) * clamp(u);
  let acc = 0;
  for (let i = 1; i < pts.length; i++) {
    const seg = Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
    if (acc + seg >= L) {
      const k = seg === 0 ? 0 : (L - acc) / seg;
      return [lerp(pts[i - 1][0], pts[i][0], k), lerp(pts[i - 1][1], pts[i][1], k), Math.atan2(pts[i][1] - pts[i - 1][1], pts[i][0] - pts[i - 1][0])];
    }
    acc += seg;
  }
  const n = pts.length - 1;
  return [pts[n][0], pts[n][1], Math.atan2(pts[n][1] - pts[n - 1][1], pts[n][0] - pts[n - 1][0])];
}

// Arrow drawn progressively along pts. o.both draws a head at the start too.
function arrow(pts, p, o = {}) {
  if (p <= 0) return;
  const color = o.color || C.ink;
  const total = pathLength(pts);
  const target = total * clamp(p);
  ctx.save();
  ctx.strokeStyle = color;
  ctx.fillStyle = color;
  ctx.lineWidth = o.width || 2.6;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  if (o.dashed) ctx.setLineDash([11, 9]);
  ctx.beginPath();
  ctx.moveTo(pts[0][0], pts[0][1]);
  let acc = 0;
  let end = pts[0];
  let angle = 0;
  for (let i = 1; i < pts.length; i++) {
    const seg = Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
    angle = Math.atan2(pts[i][1] - pts[i - 1][1], pts[i][0] - pts[i - 1][0]);
    if (acc + seg >= target) {
      const k = seg === 0 ? 0 : (target - acc) / seg;
      end = [lerp(pts[i - 1][0], pts[i][0], k), lerp(pts[i - 1][1], pts[i][1], k)];
      ctx.lineTo(end[0], end[1]);
      break;
    }
    acc += seg;
    end = pts[i];
    ctx.lineTo(end[0], end[1]);
  }
  ctx.stroke();
  ctx.setLineDash([]);
  if (o.head !== false) head(end[0], end[1], angle, o.headSize || 13);
  if (o.both && p > 0.05) {
    const a0 = Math.atan2(pts[0][1] - pts[1][1], pts[0][0] - pts[1][0]);
    head(pts[0][0], pts[0][1], a0, o.headSize || 13);
  }
  ctx.restore();
}

function head(x, y, a, s) {
  ctx.beginPath();
  ctx.moveTo(x, y);
  ctx.lineTo(x - s * Math.cos(a - 0.45), y - s * Math.sin(a - 0.45));
  ctx.moveTo(x, y);
  ctx.lineTo(x - s * Math.cos(a + 0.45), y - s * Math.sin(a + 0.45));
  ctx.stroke();
}

// Glowing dot (a request or a token in flight).
function packet(x, y, color, r = 9, o = {}) {
  ctx.save();
  ctx.shadowColor = color;
  ctx.shadowBlur = o.blur ?? 22;
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.arc(x, y, r, 0, Math.PI * 2);
  ctx.fill();
  ctx.shadowBlur = 0;
  ctx.fillStyle = 'rgba(255,255,255,0.75)';
  ctx.beginPath();
  ctx.arc(x - r * 0.3, y - r * 0.3, r * 0.35, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
  if (o.label) text(o.label, x, y - r - 14, { size: 18, align: 'center', color });
}

// Numbered step bubble like the original diagram.
function stepBubble(n, x, y, p) {
  pop(p, x, y, () => {
    circle(x, y, 46, ['#ffffff', C.blue[1]], { strokeWidth: 2.6 });
    text(String(n), x, y + 2, { size: 28, align: 'center', color: C.blue[1] });
  });
}

// Chapter header (top-left) and caption (bottom center).
function chapter(num, title, a) {
  withAlpha(a, () => {
    text(num, 70, 72, { size: 30, color: C.faint });
    text(title, 70 + textWidth(num, 30) + 18, 72, { size: 46 });
    highlight(70 + textWidth(num, 30) + 14, 98, textWidth(title, 46) + 8, a);
  });
}

function caption(str, a, y = 1020) {
  withAlpha(a, () => {
    const w = textWidth(str, 32) + 60;
    ctx.save();
    ctx.fillStyle = 'rgba(30,30,30,0.86)';
    ctx.fill(new Path2D(roundPath(W / 2 - w / 2, y - 30, w, 60, 30)));
    ctx.restore();
    text(str, W / 2, y + 1, { size: 32, align: 'center', color: '#fff' });
  });
}

// Simple progress ring (timer).
function ring(cx, cy, r, p, color, width = 6) {
  ctx.save();
  ctx.lineWidth = width;
  ctx.strokeStyle = 'rgba(0,0,0,0.08)';
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.stroke();
  ctx.strokeStyle = color;
  ctx.lineCap = 'round';
  ctx.beginPath();
  ctx.arc(cx, cy, r, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * clamp(p));
  ctx.stroke();
  ctx.restore();
}

function checkmark(x, y, s, color, p = 1) {
  if (p <= 0) return;
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = s / 6;
  ctx.lineCap = 'round';
  ctx.lineJoin = 'round';
  const pts = [[x - s / 2, y], [x - s / 8, y + s / 2.6], [x + s / 2, y - s / 2.4]];
  const L1 = Math.hypot(pts[1][0] - pts[0][0], pts[1][1] - pts[0][1]);
  const L2 = Math.hypot(pts[2][0] - pts[1][0], pts[2][1] - pts[1][1]);
  const L = (L1 + L2) * clamp(p);
  ctx.beginPath();
  ctx.moveTo(pts[0][0], pts[0][1]);
  if (L <= L1) {
    ctx.lineTo(lerp(pts[0][0], pts[1][0], L / L1), lerp(pts[0][1], pts[1][1], L / L1));
  } else {
    ctx.lineTo(pts[1][0], pts[1][1]);
    const k = (L - L1) / L2;
    ctx.lineTo(lerp(pts[1][0], pts[2][0], k), lerp(pts[1][1], pts[2][1], k));
  }
  ctx.stroke();
  ctx.restore();
}

function cross(x, y, s, color, p = 1) {
  if (p <= 0) return;
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = s / 6;
  ctx.lineCap = 'round';
  const k = clamp(p * 2);
  const k2 = clamp(p * 2 - 1);
  ctx.beginPath();
  ctx.moveTo(x - s / 2, y - s / 2);
  ctx.lineTo(x - s / 2 + s * k, y - s / 2 + s * k);
  if (k2 > 0) {
    ctx.moveTo(x + s / 2, y - s / 2);
    ctx.lineTo(x + s / 2 - s * k2, y - s / 2 + s * k2);
  }
  ctx.stroke();
  ctx.restore();
}

// Bar meter.
function meter(x, y, w, h, v, color, o = {}) {
  ctx.save();
  ctx.fillStyle = o.track || 'rgba(0,0,0,0.07)';
  ctx.fill(new Path2D(roundPath(x, y, w, h, h / 2)));
  if (v > 0) {
    ctx.fillStyle = color;
    ctx.fill(new Path2D(roundPath(x, y, Math.max(h, w * clamp(v)), h, h / 2)));
  }
  ctx.restore();
}
