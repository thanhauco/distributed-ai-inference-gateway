// Renders index.html frame by frame with headless Chromium and pipes the
// frames into ffmpeg.
//
//   node render.mjs                         # full video → ../docs/media/architecture-walkthrough.mp4
//   node render.mjs --stills 10,20,30       # PNG stills → frames/still-<t>.png
//   node render.mjs --from 44 --to 62       # render a time range only
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
import { extname, join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { existsSync } from 'node:fs';
import { chromium } from 'playwright';

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : def;
};
const FPS = Number(opt('--fps', 30));
const out = resolve(here, opt('--out', '../docs/media/architecture-walkthrough.mp4'));
const stills = opt('--stills', null);

// Tiny static server: fonts and scripts load the same way as in a browser.
const types = { '.html': 'text/html', '.js': 'text/javascript', '.woff2': 'font/woff2' };
const server = createServer(async (req, res) => {
  try {
    const path = join(here, decodeURIComponent(new URL(req.url, 'http://x').pathname));
    const body = await readFile(path);
    res.writeHead(200, { 'Content-Type': types[extname(path)] || 'application/octet-stream' });
    res.end(body);
  } catch {
    res.writeHead(404);
    res.end();
  }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const url = `http://127.0.0.1:${server.address().port}/index.html`;

// CHROMIUM_PATH lets you use a system Chromium when Playwright's own build is absent.
const executablePath = process.env.CHROMIUM_PATH && existsSync(process.env.CHROMIUM_PATH) ? process.env.CHROMIUM_PATH : undefined;
const browser = await chromium.launch({ executablePath });
const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 });
page.on('pageerror', (e) => { console.error('page error:', e.message); process.exitCode = 1; });
await page.goto(url);
await page.evaluate(() => window.ready);
const duration = await page.evaluate(() => window.DURATION);

if (stills) {
  await mkdir(join(here, 'frames'), { recursive: true });
  for (const t of stills.split(',').map(Number)) {
    await page.evaluate((t) => window.renderFrame(t), t);
    const file = join(here, 'frames', `still-${String(t).padStart(6, '0')}.png`);
    await page.screenshot({ path: file });
    console.log(file);
  }
  await browser.close();
  server.close();
  process.exit(process.exitCode || 0);
}

const from = Number(opt('--from', 0));
const to = Number(opt('--to', duration));
await mkdir(dirname(out), { recursive: true });
const ffmpeg = spawn('ffmpeg', [
  '-y', '-loglevel', 'error',
  '-f', 'image2pipe', '-framerate', String(FPS), '-c:v', 'mjpeg', '-i', '-',
  '-c:v', 'libx264', '-preset', 'slow', '-crf', '22', '-pix_fmt', 'yuv420p',
  '-movflags', '+faststart', out,
], { stdio: ['pipe', 'inherit', 'inherit'] });

const frames = Math.round((to - from) * FPS);
const started = Date.now();
for (let i = 0; i < frames; i++) {
  const t = from + i / FPS;
  await page.evaluate((t) => window.renderFrame(t), t);
  const jpg = await page.screenshot({ type: 'jpeg', quality: 92 });
  if (!ffmpeg.stdin.write(jpg)) await new Promise((r) => ffmpeg.stdin.once('drain', r));
  if (i % (FPS * 5) === 0) {
    const el = (Date.now() - started) / 1000;
    process.stdout.write(`\r${t.toFixed(1)}s / ${to}s  (${(i / Math.max(el, 0.001)).toFixed(1)} fps)   `);
  }
}
ffmpeg.stdin.end();
await new Promise((r) => ffmpeg.on('close', r));
await browser.close();
server.close();
console.log(`\nwrote ${out}`);
