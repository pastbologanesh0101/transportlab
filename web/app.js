"use strict";
/* TransportLab dashboard.  Vanilla JS + <canvas>, no dependencies. */

const $  = (s) => document.querySelector(s);
const CSS = getComputedStyle(document.documentElement);
const C = (n) => CSS.getPropertyValue(n).trim();
const COL = {
  data: C("--data"), ack: C("--ack"), retx: C("--retx"), drop: C("--drop"),
  ctrl: C("--ctrl"), ink: C("--ink"), muted: C("--muted"), line: C("--line"),
};
const FLOW_COL = [C("--data"), C("--retx"), C("--ctrl"), C("--ack")];
const now = () => performance.now();

/* ------------------------------------------------------------------ */
const SLIDERS = [
  {k:"loss",        label:"loss",         min:0, max:40,  step:0.5, unit:"%",  factor:0.01},
  {k:"latency_ms",  label:"one-way delay",min:0, max:400, step:1,   unit:"ms", factor:1},
  {k:"jitter_ms",   label:"jitter",       min:0, max:100, step:1,   unit:"ms", factor:1},
  {k:"reorder",     label:"reordering",   min:0, max:30,  step:0.5, unit:"%",  factor:0.01},
  {k:"dup",         label:"duplication",  min:0, max:15,  step:0.5, unit:"%",  factor:0.01},
  {k:"corrupt",     label:"corruption",   min:0, max:10,  step:0.25,unit:"%",  factor:0.01},
  {k:"rate_kbps",   label:"bandwidth cap",min:0, max:50000,step:500, unit:"kbps",factor:1, zero:"off"},
  {k:"buffer_bytes",label:"router buffer",min:0, max:1048576,step:16384,unit:"KB",factor:1, div:1024, zero:"∞"},
];
function buildSliders() {
  const box = $("#sliders");
  for (const s of SLIDERS) {
    const el = document.createElement("div");
    el.className = "slider";
    el.innerHTML = `<div class="lab"><span>${s.label}</span><b id="lab-${s.k}"></b></div>
       <input type="range" id="sl-${s.k}" min="${s.min}" max="${s.max}" step="${s.step}">`;
    box.appendChild(el);
    el.querySelector("input").addEventListener("input", () => { showSlider(s); scheduleLink(); });
  }
}
function showSlider(s) {
  const v = +$("#sl-" + s.k).value;
  $("#lab-" + s.k).textContent = (s.zero && v === 0) ? s.zero
    : (s.div ? Math.round(v / s.div) : v) + (s.unit === "%" ? "" : " ") + s.unit;
}
const readSliders = () => Object.fromEntries(SLIDERS.map(s => [s.k, +$("#sl-" + s.k).value * s.factor]));
function setSliders(cfg) {
  for (const s of SLIDERS) {
    if (cfg[s.k] === undefined) continue;
    $("#sl-" + s.k).value = cfg[s.k] / s.factor; showSlider(s);
  }
}
let linkTimer = null;
const scheduleLink = () => { clearTimeout(linkTimer); linkTimer = setTimeout(() => post("/link", readSliders()), 120); };

function buildPresets(presets) {
  const box = $("#presets"); box.innerHTML = "";
  for (const [name, desc] of Object.entries(presets)) {
    const b = document.createElement("button");
    b.textContent = name.replace(/_/g, " "); b.title = desc; b.dataset.name = name;
    b.onclick = async () => { setSliders(await post("/preset", {name})); markPreset(name); };
    box.appendChild(b);
  }
}
const markPreset = (name) => document.querySelectorAll("#presets button")
  .forEach(b => b.classList.toggle("active", b.dataset.name === name));

async function post(url, body) {
  const r = await fetch(url, {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify(body || {})});
  return r.json().catch(() => ({}));
}

/* ------------------------------------------------------------------ *
 *  state
 * ------------------------------------------------------------------ */
const S = {
  running:false, nFlows:1, mux:1, hol:false, t0:0, tEnd:0,
  segs:[], pending:{up:[], down:[]},
  flows:{},                       // id -> {cc, cwnd, sst, gp, srtt, rto, delivered, lastProg, cnt, final, cur}
  streams:{},                     // stream id -> [{t, v}]  (QUIC mux delivery)
  sweep:{active:false, cc:"", pts:[]},
  verifyAll:null,
};
const streamCol = (i) => `hsl(${(i * 47 + 165) % 360} 62% 62%)`;
function flow(id) {
  if (!S.flows[id]) S.flows[id] = {
    cc:"", cwnd:[], sst:[], gp:[], srtt:[], rto:[],
    delivered:0, lastProg:null, final:null,
    cnt:{retx:0, timeout:0, dupack:0, drop:0},
    cur:{cwnd:0, sst:0, state:"-", srtt:0, gp:0, inflight:0},
  };
  return S.flows[id];
}
function resetRun() {
  S.segs = []; S.pending = {up:[], down:[]};
  S.flows = {}; S.streams = {}; S.t0 = now(); S.tEnd = 0; S.verifyAll = null;
  $("#verify").className = "pill hidden";
}

/* ------------------------------------------------------------------ *
 *  event stream
 * ------------------------------------------------------------------ */
function connect() {
  const es = new EventSource("/events");
  es.onmessage = (m) => { try { onEvent(JSON.parse(m.data)); } catch (e) {} };
}
function kindOf(ev) {
  if (ev.retransmit) return "retx";
  const p = (ev.ptype || "").toUpperCase();
  if (p.startsWith("SYN") || p.startsWith("FIN")) return "ctrl";
  if (p === "ACK") return "ack";
  return "data";
}
function onEvent(ev) {
  const f = ev.flow ?? 0;
  switch (ev.kind) {
    case "run_begin":
      resetRun();
      S.running = true; S.nFlows = ev.flows || 1;
      S.mux = ev.mux || 1; S.hol = !!ev.hol;
      $("#fig-streams").classList.toggle("hidden", S.mux < 2);
      (ev.flow_cc || []).forEach((cc, i) => flow(i).cc = cc);
      setStatus("running"); break;
    case "flow_begin": flow(f).cc = ev.cc || flow(f).cc; break;
    case "run_end":
      S.running = false; S.tEnd = now() - S.t0; setStatus("done");
      showVerify(ev.all_verified); break;

    case "tx": {
      const dir = ev.who === "client" ? "up" : "down";
      const seg = {t0:now(), t1:null, dir, kind:kindOf(ev), flow:f, done:false, dropped:false};
      S.segs.push(seg); S.pending[dir].push(seg);
      if (ev.retransmit) flow(f).cnt.retx++;
      if (S.segs.length > 1400) S.segs = S.segs.filter(s => (s.t1 || s.t0) > now() - LADDER_MS - 500);
      break;
    }
    case "rx": {
      const dir = ev.who === "server" ? "up" : "down";
      const seg = S.pending[dir].find(s => !s.done);
      if (seg) { seg.t1 = now(); seg.done = true;
        S.pending[dir] = S.pending[dir].filter(s => s !== seg); }
      break;
    }
    case "link_drop": case "rx_drop": {
      const dir = ev.dir || (ev.who === "server" ? "up" : "down");
      const seg = (S.pending[dir] || []).find(s => !s.done);
      if (seg) { seg.done = true; seg.dropped = true; seg.t1 = now();
        S.pending[dir] = S.pending[dir].filter(s => s !== seg); }
      flow(f).cnt.drop++; break;
    }

    case "cwnd": {
      const g = flow(f), t = now() - S.t0;
      g.cwnd.push({t, v:ev.cwnd}); g.sst.push({t, v:Math.min(ev.ssthresh, 1e6)});
      g.cur.cwnd = ev.cwnd; g.cur.sst = ev.ssthresh;
      g.cur.state = ev.state; g.cur.inflight = ev.inflight;
      cap(g.cwnd); cap(g.sst); break;
    }
    case "rtt": {
      const g = flow(f), t = now() - S.t0;
      g.srtt.push({t, v:ev.srtt_ms}); g.rto.push({t, v:ev.rto_ms});
      g.cur.srtt = ev.srtt_ms; cap(g.srtt); cap(g.rto); break;
    }
    case "progress": {
      if (ev.side !== "server") break;
      const g = flow(f), t = now() - S.t0;
      g.delivered = ev.bytes;
      if (g.lastProg) {
        const dt = (t - g.lastProg.t) / 1000;
        if (dt >= 0.1) {
          const inst = (ev.bytes - g.lastProg.bytes) * 8 / dt / 1e6;
          g.cur.gp = g.cur.gp ? g.cur.gp * 0.5 + inst * 0.5 : inst;
          g.gp.push({t, v:g.cur.gp}); cap(g.gp);
          g.lastProg = {t, bytes:ev.bytes};
        }
      } else g.lastProg = {t, bytes:ev.bytes};
      break;
    }
    case "stream_progress": {
      (S.streams[ev.stream] || (S.streams[ev.stream] = [])).push({t: now() - S.t0, v: ev.segments});
      cap(S.streams[ev.stream]); break;
    }
    case "dupack": flow(f).cnt.dupack++; break;
    case "rto":    flow(f).cnt.timeout++; break;
    case "preset": markPreset(ev.name); break;
    case "verify": flow(f).final = Object.assign(flow(f).final || {}, ev); break;
    case "stats":  flow(f).final = Object.assign(flow(f).final || {}, ev); break;

    case "sweep_begin":
      S.sweep = {active:true, cc:ev.cc, pts:[]};
      $("#fig-sweep").classList.remove("hidden");
      $("#sweep-note").textContent = `running ${ev.cc}…`; break;
    case "sweep_point":
      S.sweep.pts.push(ev);
      $("#sweep-note").textContent = `${S.sweep.cc}: ${S.sweep.pts.length} pts`; break;
    case "sweep_done":
      S.sweep.active = false;
      $("#sweep-note").textContent = `${S.sweep.cc}: done`; break;
  }
}
const cap = (a) => { if (a.length > 6000) a.splice(0, a.length - 6000); };

function setStatus(s) { const e = $("#status"); e.className = "pill " + s;
  e.textContent = s === "running" ? "transferring" : s; }
function showVerify(ok) { const e = $("#verify"); e.className = "pill " + (ok ? "ok" : "fail");
  e.textContent = ok ? "✓ all sha-256 match" : "✗ verify failed"; }

/* ================================================================== *
 *  canvas helpers
 * ================================================================== */
function fit(cv) {
  const r = cv.getBoundingClientRect(), dpr = window.devicePixelRatio || 1;
  cv.width = r.width * dpr; cv.height = r.height * dpr;
  cv.getContext("2d").setTransform(dpr, 0, 0, dpr, 0, 0);
  return {w:r.width, h:r.height};
}
function timeDomain(all, windowMs) {
  if (S.running || !S.tEnd) { const tMax = now() - S.t0; return [Math.max(0, tMax - windowMs), tMax]; }
  const tMax = S.tEnd + 200;
  return [Math.min(...all.map(p => p.t), tMax) - 100, tMax];
}
function lineChart(cv, series, opts = {}) {
  const {w, h} = fit(cv), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  const padL = 36, padR = 8, padT = 8, padB = 14;
  const all = series.flatMap(s => s.pts); if (!all.length) return;
  const [tMin, tMax] = timeDomain(all, opts.windowMs || 22000);
  const scale = series.filter(s => !s.noscale).flatMap(s => s.pts);
  const vMax = (opts.vMax || Math.max(...(scale.length ? scale : all).map(p => p.v), 1)) * 1.12;
  const vMin = opts.vMin ?? 0;
  const X = (t) => padL + (t - tMin) / (tMax - tMin || 1) * (w - padL - padR);
  const Y = (v) => h - padB - (v - vMin) / (vMax - vMin || 1) * (h - padT - padB);
  ctx.font = "9px ui-monospace, monospace";
  for (let i = 0; i <= 2; i++) {
    const v = vMin + (vMax - vMin) * i / 2, y = Y(v);
    ctx.strokeStyle = COL.line; ctx.globalAlpha = 0.5;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.globalAlpha = 1; ctx.fillStyle = COL.muted; ctx.fillText(fmt(v), 2, y + 3);
  }
  for (const s of series) {
    const pts = s.pts.filter(p => p.t >= tMin - 500); if (!pts.length) continue;
    ctx.strokeStyle = s.color; ctx.lineWidth = s.width || 1.5;
    ctx.setLineDash(s.dash || []); ctx.beginPath();
    pts.forEach((p, i) => {
      const x = X(p.t), y = Y(p.v);
      (s.step && i) ? (ctx.lineTo(x, Y(pts[i-1].v)), ctx.lineTo(x, y))
                    : (i ? ctx.lineTo(x, y) : ctx.moveTo(x, y));
    });
    ctx.stroke(); ctx.setLineDash([]);
    if (s.fill) {
      const last = pts[pts.length-1];
      ctx.lineTo(X(last.t), Y(vMin)); ctx.lineTo(X(pts[0].t), Y(vMin));
      ctx.globalAlpha = 0.12; ctx.fillStyle = s.color; ctx.fill(); ctx.globalAlpha = 1;
    }
  }
}
function fmt(v) {
  if (v >= 1000) return (v / 1000).toFixed(1) + "k";
  if (v >= 10) return v.toFixed(0);
  return v.toFixed(1);
}

/* ================================================================== *
 *  flow ladder
 * ================================================================== */
const LADDER_MS = 1800;
const ladder = $("#cv-ladder"), lx = ladder.getContext("2d");
function drawLadder() {
  const {w, h} = fit(ladder); lx.clearRect(0, 0, w, h);
  const padT = 26, padB = 12, xS = w * 0.16, xR = w * 0.84;
  const yOf = (t) => padT + (now() - t) / LADDER_MS * (h - padT - padB);
  lx.strokeStyle = COL.line; lx.lineWidth = 1; lx.globalAlpha = 0.35;
  lx.font = "9px ui-monospace, monospace"; lx.textAlign = "left";
  for (let ms = 0; ms <= LADDER_MS; ms += 300) {
    const y = padT + ms / LADDER_MS * (h - padT - padB);
    lx.beginPath(); lx.moveTo(xS, y); lx.lineTo(xR, y); lx.stroke();
    lx.fillStyle = COL.line; lx.fillText(ms ? `-${ms}` : "now", 4, y + 3);
  }
  lx.globalAlpha = 1;
  lx.strokeStyle = COL.muted;
  lx.beginPath(); lx.moveTo(xS, padT); lx.lineTo(xS, h - padB);
  lx.moveTo(xR, padT); lx.lineTo(xR, h - padB); lx.stroke();
  lx.fillStyle = COL.muted; lx.font = "11px ui-monospace, monospace";
  lx.textAlign = "center";
  lx.fillText("SENDER", xS, 14); lx.fillText("RECEIVER", xR, 14);
  if (!S.running && !S.segs.length) { lx.fillStyle = COL.muted;
    lx.fillText("press Start", w / 2, h / 2); return; }

  for (const s of S.segs) {
    const goRight = s.dir === "up";
    const x0 = goRight ? xS : xR, x1 = goRight ? xR : xS;
    const dur = s.t1 ? (s.t1 - s.t0) : Math.max(120, estOneWay());
    const frac = s.done ? 1 : Math.min(1, (now() - s.t0) / dur);
    const yStart = yOf(s.t0), yEnd = yOf(s.t0 + dur);
    if (yStart < padT - 20 && yEnd < padT - 20) continue;
    if (yStart > h) continue;
    const hx = x0 + (x1 - x0) * frac, hy = yStart + (yEnd - yStart) * frac;
    let c = s.kind === "data" ? (FLOW_COL[s.flow] || COL.data)
          : s.kind === "ack" ? COL.ack : s.kind === "retx" ? COL.retx : COL.ctrl;
    if (s.dropped) c = COL.drop;
    lx.strokeStyle = c;
    lx.globalAlpha = s.dropped ? 0.95 : (s.kind === "ack" ? 0.28 : 0.8);
    lx.lineWidth = (s.kind === "data" || s.kind === "retx") ? 1.5 : 0.8;
    lx.setLineDash(s.kind === "retx" ? [3, 2] : []);
    lx.beginPath(); lx.moveTo(x0, yStart); lx.lineTo(hx, hy); lx.stroke();
    lx.setLineDash([]);
    if (!s.done) { lx.globalAlpha = 1; lx.fillStyle = c;
      lx.beginPath(); lx.arc(hx, hy, s.kind === "ack" ? 1.6 : 2.4, 0, 7); lx.fill(); }
    else if (s.dropped) { lx.globalAlpha = 1; lx.strokeStyle = COL.drop; lx.lineWidth = 1.4;
      lx.beginPath(); lx.moveTo(hx-3, hy-3); lx.lineTo(hx+3, hy+3);
      lx.moveTo(hx+3, hy-3); lx.lineTo(hx-3, hy+3); lx.stroke(); }
    lx.globalAlpha = 1;
  }
}
const estOneWay = () => (+$("#sl-latency_ms").value || 5) + 60;

/* ================================================================== *
 *  scope charts
 * ================================================================== */
function ids() { return Object.keys(S.flows).map(Number).sort((a,b) => a-b); }

function drawCwnd() {
  const fl = ids();
  const series = fl.map(i => ({pts: S.flows[i].cwnd, color: FLOW_COL[i] || COL.data,
                               width: 1.7, step: true}));
  if (fl.length === 1 && S.flows[fl[0]])
    series.unshift({pts: S.flows[fl[0]].sst, color: COL.retx, dash:[2,3], width:1, noscale:true});
  lineChart($("#cv-cwnd"), series, {windowMs: 22000});
}
function drawTput() {
  lineChart($("#cv-tput"),
    ids().map(i => ({pts: S.flows[i].gp, color: FLOW_COL[i] || COL.ack, width: 1.5,
                     fill: ids().length === 1})),
    {windowMs: 22000});
}
function drawRtt() {
  const fl = ids(), series = [];
  for (const i of fl) {
    series.push({pts: S.flows[i].srtt, color: FLOW_COL[i] || COL.ctrl, width: 1.6});
    if (fl.length === 1) series.push({pts: S.flows[i].rto, color: COL.drop, dash:[2,3], width:1});
  }
  lineChart($("#cv-rtt"), series, {windowMs: 22000});
}

function drawShare() {
  const cv = $("#cv-share"), {w, h} = fit(cv), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  const fl = ids(); if (fl.length < 2) return;
  const padL = 36, padR = 8, padT = 8, padB = 14;
  const all = fl.flatMap(i => S.flows[i].gp); if (!all.length) return;
  const [tMin, tMax] = timeDomain(all, 22000);
  const N = 120;
  const grid = Array.from({length: N}, (_, k) => tMin + (tMax - tMin) * k / (N - 1));
  const at = (pts, t) => {
    if (!pts.length) return 0;
    let lo = pts[0]; for (const p of pts) { if (p.t <= t) lo = p; else break; }
    return lo.v;
  };
  const stacks = grid.map(t => fl.map(i => Math.max(0, at(S.flows[i].gp, t))));
  const vMax = Math.max(...stacks.map(s => s.reduce((a, b) => a + b, 0)), 0.1) * 1.1;
  const X = (t) => padL + (t - tMin) / (tMax - tMin || 1) * (w - padL - padR);
  const Y = (v) => h - padB - v / vMax * (h - padT - padB);
  ctx.font = "9px ui-monospace, monospace"; ctx.fillStyle = COL.muted;
  ctx.fillText(fmt(vMax), 2, Y(vMax) + 8); ctx.fillText("0", 2, Y(0));
  let base = grid.map(() => 0);
  fl.forEach((i, fi) => {
    ctx.beginPath();
    grid.forEach((t, k) => { const y = Y(base[k] + stacks[k][fi]);
      k ? ctx.lineTo(X(t), y) : ctx.moveTo(X(t), y); });
    for (let k = N - 1; k >= 0; k--) ctx.lineTo(X(grid[k]), Y(base[k]));
    ctx.closePath();
    ctx.fillStyle = FLOW_COL[i] || COL.data; ctx.globalAlpha = 0.55; ctx.fill();
    ctx.globalAlpha = 1;
    base = base.map((b, k) => b + stacks[k][fi]);
  });
}

function drawFair() {
  const cv = $("#cv-fair"), {w, h} = fit(cv), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  const fl = ids();
  if (fl.length === 2) {
    const a = S.flows[fl[0]].cwnd, b = S.flows[fl[1]].cwnd;
    if (!a.length || !b.length) return;
    const m = Math.max(...a.map(p => p.v), ...b.map(p => p.v), 4) * 1.1;
    const pad = 22;
    const X = (v) => pad + v / m * (w - pad * 2);
    const Y = (v) => h - pad - v / m * (h - pad * 2);
    ctx.strokeStyle = COL.line; ctx.lineWidth = 1;
    ctx.strokeRect(pad, pad, w - pad * 2, h - pad * 2);
    ctx.strokeStyle = COL.muted; ctx.setLineDash([4, 3]);           // y = x fair line
    ctx.beginPath(); ctx.moveTo(X(0), Y(0)); ctx.lineTo(X(m), Y(m)); ctx.stroke();
    ctx.setLineDash([]);
    const n = Math.min(a.length, b.length), from = Math.max(0, n - 400);
    ctx.strokeStyle = COL.data; ctx.lineWidth = 1.4; ctx.beginPath();
    for (let k = from; k < n; k++) {
      const x = X(a[k].v), y = Y(b[k].v);
      k === from ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    }
    ctx.stroke();
    ctx.fillStyle = COL.retx;
    ctx.beginPath(); ctx.arc(X(a[n-1].v), Y(b[n-1].v), 3, 0, 7); ctx.fill();
    ctx.fillStyle = COL.muted; ctx.font = "9px ui-monospace, monospace";
    ctx.fillText(`${S.flows[fl[0]].cc} cwnd →`, pad + 2, h - 6);
    ctx.save(); ctx.translate(10, h - pad); ctx.rotate(-Math.PI/2);
    ctx.fillText(`${S.flows[fl[1]].cc} cwnd →`, 0, 0); ctx.restore();
  } else {
    // 3-4 flows: current goodput share bars + Jain index
    const gp = fl.map(i => Math.max(0, S.flows[i].cur.gp));
    const tot = gp.reduce((a, b) => a + b, 0) || 1;
    const sq = gp.reduce((a, b) => a + b * b, 0) || 1;
    const jain = tot * tot / (fl.length * sq);
    const bh = (h - 26) / fl.length;
    fl.forEach((i, k) => {
      const y = 4 + k * bh;
      ctx.fillStyle = FLOW_COL[i] || COL.data;
      ctx.fillRect(60, y + 3, (gp[k] / tot) * (w - 70), bh - 8);
      ctx.fillStyle = COL.ink; ctx.font = "10px ui-monospace, monospace";
      ctx.fillText(S.flows[i].cc, 4, y + bh / 2 + 3);
    });
    ctx.fillStyle = COL.muted; ctx.font = "10px ui-monospace, monospace";
    ctx.fillText(`Jain fairness index  ${jain.toFixed(3)}`, 4, h - 8);
  }
}

function drawSweep() {
  const cv = $("#cv-sweep"), {w, h} = fit(cv), ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  const pts = S.sweep.pts; if (!pts.length) return;
  const padL = 40, padR = 10, padT = 10, padB = 20;
  const xs = pts.map(p => p.loss * 100);
  const ys = pts.flatMap(p => [p.goodput_kbps, p.mathis_kbps].filter(v => v != null));
  const xMax = Math.max(...xs, 1), yMax = Math.max(...ys, 1) * 1.1;
  const X = (x) => padL + x / xMax * (w - padL - padR);
  const Y = (y) => h - padB - y / yMax * (h - padT - padB);
  ctx.font = "9px ui-monospace, monospace"; ctx.fillStyle = COL.muted;
  ctx.fillText(fmt(yMax), 2, Y(yMax) + 8); ctx.fillText("0", 2, Y(0));
  ctx.fillText("loss %", w - 40, h - 6);
  const draw = (key, color, dash) => {
    ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.setLineDash(dash || []);
    ctx.beginPath(); let started = false;
    pts.forEach(p => { if (p[key] == null) return;
      const x = X(p.loss * 100), y = Y(p[key]);
      started ? ctx.lineTo(x, y) : (ctx.moveTo(x, y), started = true);
      ctx.fillStyle = color; });
    ctx.stroke(); ctx.setLineDash([]);
    pts.forEach(p => { if (p[key] == null) return;
      ctx.beginPath(); ctx.arc(X(p.loss*100), Y(p[key]), 2.5, 0, 7); ctx.fill(); });
  };
  draw("mathis_kbps", COL.muted, [3, 3]);
  draw("goodput_kbps", COL.data);
  ctx.fillStyle = COL.data; ctx.fillText("measured", w - 130, 12);
  ctx.fillStyle = COL.muted; ctx.fillText("Mathis √p", w - 70, 12);
}

/* ------------------------------------------------------------------ */
function drawStats() {
  const fl = ids();
  const arena = S.nFlows > 1;
  let tiles;
  if (arena) {
    tiles = fl.map(i => {
      const g = S.flows[i], f = g.final || {};
      const gp = (f.goodput_kbps ? f.goodput_kbps / 1000 : g.cur.gp).toFixed(2);
      return [`${g.cc}`, `${gp} Mb/s`,
              `cwnd ${g.cur.cwnd ? g.cur.cwnd.toFixed(0) : "–"} · to ${f.timeouts ?? g.cnt.timeout}`,
              FLOW_COL[i]];
    });
  } else {
    const g = flow(0), f = g.final || {};
    tiles = [
      ["goodput", (f.goodput_kbps ? (f.goodput_kbps/1000).toFixed(2) : g.cur.gp.toFixed(2)) + " Mb/s"],
      ["cwnd", g.cur.cwnd ? g.cur.cwnd.toFixed(1) : "–"],
      ["in flight", g.cur.inflight || "–"],
      ["srtt", (g.cur.srtt || f.srtt_ms || 0).toFixed(1) + " ms"],
      ["cc state", (g.cur.state || "–").replace(/_/g, " ")],
      ["retransmits", f.retransmits ?? g.cnt.retx],
      ["fast rtx", f.fast_retx ?? "–"],
      ["timeouts", f.timeouts ?? g.cnt.timeout],
      ["dup acks", f.dupacks ?? g.cnt.dupack],
    ];
  }
  $("#stats").className = "stat-grid" + (arena ? " arena" : "");
  $("#stats").innerHTML = tiles.map(([l, v, sub, col]) =>
    `<div class="stat"${col ? ` style="border-left:3px solid ${col}"` : ""}>
       <div class="v">${v}</div><div class="l">${l}</div>
       ${sub ? `<div class="sub">${sub}</div>` : ""}</div>`).join("");
}

/* ------------------------------------------------------------------ */
function drawStreams() {
  const ks = Object.keys(S.streams).map(Number).sort((a, b) => a - b);
  lineChart($("#cv-streams"),
    ks.map(k => ({pts: S.streams[k], color: streamCol(k), width: 1.6, step: true})),
    {windowMs: 22000});
}
function frame() {
  drawLadder();
  drawCwnd(); drawTput(); drawRtt();
  if (S.nFlows > 1) { drawShare(); drawFair(); }
  if (S.mux > 1) drawStreams();
  if (S.sweep.pts.length) drawSweep();
  drawStats();
  requestAnimationFrame(frame);
}

/* ------------------------------------------------------------------ *
 *  PNG export
 * ------------------------------------------------------------------ */
function exportPNG() {
  const cvs = ["#cv-cwnd", "#cv-share", "#cv-tput", "#cv-rtt", "#cv-sweep"]
    .map(s => $(s)).filter(c => !c.closest("figure").classList.contains("hidden"));
  const pad = 16, W = cvs[0].width, gap = 10;
  const out = document.createElement("canvas");
  out.width = W + pad * 2;
  out.height = cvs.reduce((a, c) => a + c.height, 0) + pad * 2 + gap * (cvs.length - 1);
  const g = out.getContext("2d");
  g.fillStyle = "#0a0d13"; g.fillRect(0, 0, out.width, out.height);
  let y = pad; for (const c of cvs) { g.drawImage(c, pad, y); y += c.height + gap; }
  const a = document.createElement("a");
  a.download = "transportlab-charts.png"; a.href = out.toDataURL("image/png"); a.click();
}

/* ------------------------------------------------------------------ *
 *  Arena controls
 * ------------------------------------------------------------------ */
const CC_OPTS = [["reno","Reno"],["cubic","CUBIC"],["bbr","BBR"],["tahoe","Tahoe"],["none","none"]];
function buildFlowCC(n, current) {
  const box = $("#flow-cc"); box.innerHTML = "";
  for (let i = 0; i < n; i++) {
    const sel = document.createElement("select");
    sel.dataset.flow = i;
    sel.innerHTML = CC_OPTS.map(([v, t]) =>
      `<option value="${v}"${(current[i] === v) ? " selected" : ""}>flow ${i}: ${t}</option>`).join("");
    sel.style.borderLeft = `3px solid ${FLOW_COL[i]}`;
    sel.onchange = pushParams;
    box.appendChild(sel);
  }
}
function onFlowsChange(currentCC) {
  const n = +$("#p-flows").value;
  const arena = n > 1;
  $("#flow-cc").classList.toggle("hidden", !arena);
  $("#stagger-wrap").classList.toggle("hidden", !arena);
  $("#cc-single").classList.toggle("hidden", arena);
  $("#fig-share").classList.toggle("hidden", !arena);
  $("#fairness-wrap").classList.toggle("hidden", !arena);
  $("#fairness-cap").textContent = arena
    ? (n === 2 ? "cwnd of flow 0 vs flow 1 — the dashed line is the fair share"
               : "current goodput share") : "";
  if (arena) buildFlowCC(n, currentCC || []);
}

/* ------------------------------------------------------------------ */
function pushParams() {
  const flowCC = [...document.querySelectorAll("#flow-cc select")].map(s => s.value);
  post("/params", {
    arq: $("#p-arq").value, cc: $("#p-cc").value,
    rwnd: +$("#p-rwnd").value, mss: +$("#p-mss").value,
    size_bytes: Math.round(+$("#p-size").value * 1e6),
    flows: +$("#p-flows").value, stagger_s: +$("#p-stagger").value,
    flow_cc: flowCC.length ? flowCC : null,
    mux: +$("#p-mux").value, hol: $("#p-hol").checked ? 1 : 0,
  });
}

async function init() {
  buildSliders();
  const st = await fetch("/state").then(r => r.json());
  buildPresets(st.presets || {});
  setSliders(st.link || {});
  const p = st.params || {};
  if (p.arq) $("#p-arq").value = p.arq;
  if (p.cc) $("#p-cc").value = p.cc;
  if (p.rwnd) $("#p-rwnd").value = p.rwnd;
  if (p.mss) $("#p-mss").value = p.mss;
  if (p.size_bytes) $("#p-size").value = p.size_bytes / 1e6;
  if (p.flows) $("#p-flows").value = p.flows;
  if (p.stagger_s != null) $("#p-stagger").value = p.stagger_s;
  if (p.mux) $("#p-mux").value = p.mux;
  if (p.hol) $("#p-hol").checked = true;
  onFlowsChange(st.flow_cc || ["reno", "cubic", "bbr", "tahoe"]);

  ["p-arq", "p-cc", "p-size", "p-rwnd", "p-mss", "p-stagger", "p-mux", "p-hol"]
    .forEach(id => $("#" + id).addEventListener("change", pushParams));
  $("#p-flows").addEventListener("change", () => {
    onFlowsChange(st.flow_cc || ["reno", "cubic", "bbr", "tahoe"]); pushParams();
  });
  $("#btn-start").onclick = () => post("/run", {action: "start"});
  $("#btn-stop").onclick = () => post("/run", {action: "stop"});
  $("#btn-png").onclick = exportPNG;
  $("#btn-sweep").onclick = () => {
    $("#fig-sweep").classList.remove("hidden");
    post("/sweep", {cc: $("#sw-cc").value});
  };
  if (st.running) setStatus("running");
  connect();
  requestAnimationFrame(frame);
}
window.addEventListener("resize", () => { drawLadder(); });
init();
