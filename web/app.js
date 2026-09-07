"use strict";
/* TransportLab dashboard front-end.  Vanilla JS + <canvas>, no dependencies. */

const $  = (s) => document.querySelector(s);
const CSS = getComputedStyle(document.documentElement);
const COL = {
  data: CSS.getPropertyValue("--data").trim(),
  ack:  CSS.getPropertyValue("--ack").trim(),
  retx: CSS.getPropertyValue("--retx").trim(),
  drop: CSS.getPropertyValue("--drop").trim(),
  ctrl: CSS.getPropertyValue("--ctrl").trim(),
  ink:  CSS.getPropertyValue("--ink").trim(),
  muted:CSS.getPropertyValue("--muted").trim(),
  line: CSS.getPropertyValue("--line").trim(),
};

/* ------------------------------------------------------------------ *
 *  live link-impairment sliders
 * ------------------------------------------------------------------ */
const SLIDERS = [
  {k:"loss",        label:"loss",         min:0, max:40,  step:0.5, unit:"%",  factor:0.01},
  {k:"latency_ms",  label:"one-way delay",min:0, max:400, step:1,   unit:"ms", factor:1},
  {k:"jitter_ms",   label:"jitter",       min:0, max:100, step:1,   unit:"ms", factor:1},
  {k:"reorder",     label:"reordering",   min:0, max:30,  step:0.5, unit:"%",  factor:0.01},
  {k:"dup",         label:"duplication",  min:0, max:15,  step:0.5, unit:"%",  factor:0.01},
  {k:"corrupt",     label:"corruption",   min:0, max:10,  step:0.25,unit:"%",  factor:0.01},
  {k:"rate_kbps",   label:"bandwidth cap",min:0, max:50000,step:500, unit:"kbps", factor:1, zero:"off"},
  {k:"buffer_bytes",label:"router buffer",min:0, max:1048576,step:16384,unit:"KB", factor:1, div:1024, zero:"∞"},
];

function buildSliders() {
  const box = $("#sliders");
  for (const s of SLIDERS) {
    const el = document.createElement("div");
    el.className = "slider";
    el.innerHTML =
      `<div class="lab"><span>${s.label}</span><b id="lab-${s.k}"></b></div>
       <input type="range" id="sl-${s.k}" min="${s.min}" max="${s.max}" step="${s.step}">`;
    box.appendChild(el);
    const input = el.querySelector("input");
    input.addEventListener("input", () => { showSlider(s); scheduleLink(); });
  }
}
function showSlider(s) {
  const v = +$("#sl-" + s.k).value;
  const disp = (s.zero && v === 0) ? s.zero
             : (s.div ? Math.round(v / s.div) : v) + (s.unit === "%" ? "" : " ") + s.unit;
  $("#lab-" + s.k).textContent = disp;
}
function readSliders() {
  const out = {};
  for (const s of SLIDERS) out[s.k] = +$("#sl-" + s.k).value * s.factor;
  return out;
}
function setSliders(cfg) {
  for (const s of SLIDERS) {
    if (cfg[s.k] === undefined) continue;
    $("#sl-" + s.k).value = cfg[s.k] / s.factor;
    showSlider(s);
  }
}
let linkTimer = null;
function scheduleLink() {
  clearTimeout(linkTimer);
  linkTimer = setTimeout(() => post("/link", readSliders()), 120);
}

/* ------------------------------------------------------------------ *
 *  presets
 * ------------------------------------------------------------------ */
function buildPresets(presets) {
  const box = $("#presets");
  box.innerHTML = "";
  for (const [name, desc] of Object.entries(presets)) {
    const b = document.createElement("button");
    b.textContent = name.replace(/_/g, " ");
    b.title = desc;
    b.dataset.name = name;
    b.onclick = async () => {
      const cfg = await post("/preset", {name});
      setSliders(cfg);
      markPreset(name);
    };
    box.appendChild(b);
  }
}
function markPreset(name) {
  document.querySelectorAll("#presets button")
    .forEach(b => b.classList.toggle("active", b.dataset.name === name));
}

/* ------------------------------------------------------------------ *
 *  helpers
 * ------------------------------------------------------------------ */
async function post(url, body) {
  const r = await fetch(url, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body || {}),
  });
  return r.json().catch(() => ({}));
}
const now = () => performance.now();

/* ------------------------------------------------------------------ *
 *  per-run state
 * ------------------------------------------------------------------ */
const S = {
  running: false,
  segs: [],                       // ladder segments
  pending: {up: [], down: []},    // unmatched segments, FIFO
  cwnd: [], sst: [], rwnd: [],    // {t,v}
  tput: [],                       // {t,v} Mbps
  rtt: [], srtt: [], rto: [],
  t0: 0,
  tEnd: 0,
  totalSegs: 0,
  lastProg: null,
  count: {retx: 0, timeout: 0, dupack: 0, drop: 0, corrupt: 0, delivered: 0},
  cur: {cwnd: 0, sst: 0, rwnd: 0, inflight: 0, state: "-", srtt: 0, goodput: 0},
  final: null,
};

function resetRun() {
  S.segs = []; S.pending = {up: [], down: []};
  S.cwnd = []; S.sst = []; S.rwnd = []; S.tput = [];
  S.rtt = []; S.srtt = []; S.rto = [];
  S.t0 = now(); S.tEnd = 0; S.lastProg = null; S.final = null;
  S.count = {retx: 0, timeout: 0, dupack: 0, drop: 0, corrupt: 0, delivered: 0};
  S.cur = {cwnd: 0, sst: 0, rwnd: 0, inflight: 0, state: "-", srtt: 0, goodput: 0};
  $("#verify").className = "pill hidden";
}

/* ------------------------------------------------------------------ *
 *  event stream
 * ------------------------------------------------------------------ */
function connect() {
  const es = new EventSource("/events");
  es.onmessage = (m) => { try { onEvent(JSON.parse(m.data)); } catch (e) {} };
  es.onerror = () => {};
}

function kindOf(ev) {
  if (ev.retransmit) return "retx";
  const p = (ev.ptype || "").toUpperCase();
  if (p.startsWith("SYN") || p.startsWith("FIN")) return "ctrl";
  if (p === "ACK") return "ack";
  return "data";
}

function onEvent(ev) {
  switch (ev.kind) {
    case "run_begin":
      resetRun(); S.running = true; S.totalSegs = ev.segments || 0;
      setStatus("running"); break;
    case "hello":
      if (ev.role === "client") S.totalSegs = ev.segments || S.totalSegs;
      break;
    case "run_end":
      S.running = false;
      S.tEnd = now() - S.t0;
      setStatus("done");
      showVerify(ev.verified);
      break;

    case "tx": {
      const dir = ev.who === "client" ? "up" : "down";
      const seg = {t0: now(), t1: null, dir, kind: kindOf(ev), seq: ev.seq,
                   done: false, dropped: false, reason: ""};
      S.segs.push(seg);
      S.pending[dir].push(seg);
      if (ev.retransmit) S.count.retx++;
      trimSegs();
      break;
    }
    case "rx": {
      const dir = ev.who === "server" ? "up" : "down";
      const seg = S.pending[dir].find(s => !s.done);
      if (seg) { seg.t1 = now(); seg.done = true;
                 S.pending[dir] = S.pending[dir].filter(s => s !== seg); }
      break;
    }
    case "link_drop": {
      const seg = S.pending[ev.dir] && S.pending[ev.dir].find(s => !s.done);
      if (seg) { seg.done = true; seg.dropped = true; seg.t1 = now();
                 seg.reason = ev.reason || "lost";
                 S.pending[ev.dir] = S.pending[ev.dir].filter(s => s !== seg); }
      S.count.drop++;
      break;
    }
    case "rx_drop": {
      const dir = ev.who === "server" ? "up" : "down";
      const seg = S.pending[dir] && S.pending[dir].find(s => !s.done);
      if (seg) { seg.done = true; seg.dropped = true; seg.t1 = now();
                 seg.reason = ev.reason || "checksum";
                 S.pending[dir] = S.pending[dir].filter(s => s !== seg); }
      if (ev.reason === "checksum") S.count.corrupt++;
      break;
    }

    case "cwnd": {
      const t = now() - S.t0;
      S.cwnd.push({t, v: ev.cwnd});
      S.sst.push({t, v: Math.min(ev.ssthresh, 1e6)});
      S.rwnd.push({t, v: ev.rwnd});
      S.cur.cwnd = ev.cwnd; S.cur.sst = ev.ssthresh;
      S.cur.rwnd = ev.rwnd; S.cur.inflight = ev.inflight; S.cur.state = ev.state;
      capArrays(); break;
    }
    case "rtt": {
      const t = now() - S.t0;
      S.rtt.push({t, v: ev.sample_ms});
      S.srtt.push({t, v: ev.srtt_ms});
      S.rto.push({t, v: ev.rto_ms});
      S.cur.srtt = ev.srtt_ms; capArrays(); break;
    }
    case "progress": {
      if (ev.side !== "server") break;
      const t = now() - S.t0;
      S.count.delivered = ev.bytes;
      if (S.lastProg) {
        const dt = (t - S.lastProg.t) / 1000;
        if (dt >= 0.1) {                       // integrate over >=100 ms
          const inst = (ev.bytes - S.lastProg.bytes) * 8 / dt / 1e6;
          S.cur.goodput = S.cur.goodput ? S.cur.goodput * 0.5 + inst * 0.5 : inst;
          S.tput.push({t, v: S.cur.goodput});
          S.lastProg = {t, bytes: ev.bytes};
        }
      } else {
        S.lastProg = {t, bytes: ev.bytes};
      }
      capArrays(); break;
    }
    case "dupack":  S.count.dupack++; break;
    case "rto":     S.count.timeout++; break;
    case "preset":  markPreset(ev.name); break;
    case "verify":
      S.final = S.final || {};
      Object.assign(S.final, {ok: ev.ok, sha: (ev.recv_sha256 || "").slice(0, 12)});
      showVerify(ev.ok);
      break;
    case "stats":
      S.final = Object.assign(S.final || {}, ev);
      break;
  }
}

function trimSegs() {
  const cutoff = now() - LADDER_MS - 400;
  if (S.segs.length > 1200) S.segs = S.segs.filter(s => (s.t1 || s.t0) > cutoff);
}
function capArrays() {
  for (const a of [S.cwnd, S.sst, S.rwnd, S.tput, S.rtt, S.srtt, S.rto])
    if (a.length > 6000) a.splice(0, a.length - 6000);
}

/* ------------------------------------------------------------------ *
 *  status + verify pills
 * ------------------------------------------------------------------ */
function setStatus(s) {
  const el = $("#status");
  el.className = "pill " + s;
  el.textContent = s === "running" ? "transferring" : s;
}
function showVerify(ok) {
  const el = $("#verify");
  el.className = "pill " + (ok ? "ok" : "fail");
  el.textContent = ok ? "✓ sha-256 match" : "✗ verify failed";
}

/* ================================================================== *
 *  FLOW LADDER
 * ================================================================== */
const LADDER_MS = 1800;      // visible time window
const ladder = $("#cv-ladder");
const lx = ladder.getContext("2d");

function fit(cv) {
  const r = cv.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  cv.width = r.width * dpr; cv.height = r.height * dpr;
  cv.getContext("2d").setTransform(dpr, 0, 0, dpr, 0, 0);
  return {w: r.width, h: r.height};
}

function drawLadder() {
  const {w, h} = fit(ladder);
  lx.clearRect(0, 0, w, h);
  const padT = 26, padB = 12;
  const xS = w * 0.16, xR = w * 0.84;
  const yOf = (t) => padT + (now() - t) / LADDER_MS * (h - padT - padB);

  // horizontal time ticks every 250 ms (so the ladder reads as a timeline)
  lx.strokeStyle = COL.line; lx.lineWidth = 1; lx.globalAlpha = 0.4;
  lx.font = "9px ui-monospace, monospace"; lx.textAlign = "left";
  for (let ms = 0; ms <= LADDER_MS; ms += 250) {
    const y = padT + ms / LADDER_MS * (h - padT - padB);
    lx.beginPath(); lx.moveTo(xS, y); lx.lineTo(xR, y); lx.stroke();
    lx.fillStyle = COL.line;
    lx.fillText(ms === 0 ? "now" : `-${ms}ms`, 4, y + 3);
  }
  lx.globalAlpha = 1;

  // rails
  lx.strokeStyle = COL.muted; lx.lineWidth = 1;
  lx.beginPath(); lx.moveTo(xS, padT); lx.lineTo(xS, h - padB);
  lx.moveTo(xR, padT); lx.lineTo(xR, h - padB); lx.stroke();
  lx.fillStyle = COL.muted; lx.font = "11px ui-monospace, monospace";
  lx.textAlign = "center";
  lx.fillText("SENDER", xS, 14);
  lx.fillText("RECEIVER", xR, 14);

  if (!S.running && !S.segs.length) {
    lx.fillStyle = COL.muted;
    lx.fillText("press Start transfer", w / 2, h / 2);
  }

  for (const s of S.segs) {
    const goRight = s.dir === "up";
    const x0 = goRight ? xS : xR, x1 = goRight ? xR : xS;
    const dur = s.t1 ? (s.t1 - s.t0) : Math.max(120, estOneWay());
    const frac = s.done ? 1 : Math.min(1, (now() - s.t0) / dur);
    const yStart = yOf(s.t0);
    const yEnd = yOf(s.t0 + dur);
    if (yStart < padT - 20 && yEnd < padT - 20) continue;
    if (yStart > h) continue;

    const hx = x0 + (x1 - x0) * frac;
    const hy = yStart + (yEnd - yStart) * frac;

    let c = COL[s.kind] || COL.data;
    if (s.dropped) c = COL.drop;
    lx.strokeStyle = c;
    lx.globalAlpha = s.dropped ? 0.95 : (s.kind === "ack" ? 0.28 : 0.8);
    lx.lineWidth = s.kind === "data" || s.kind === "retx" ? 1.5 : 0.8;
    if (s.kind === "retx") lx.setLineDash([3, 2]); else lx.setLineDash([]);
    lx.beginPath(); lx.moveTo(x0, yStart); lx.lineTo(hx, hy); lx.stroke();
    lx.setLineDash([]);

    if (!s.done) {                       // moving head
      lx.globalAlpha = 1; lx.fillStyle = c;
      lx.beginPath(); lx.arc(hx, hy, s.kind === "ack" ? 1.6 : 2.4, 0, 7); lx.fill();
    } else if (s.dropped) {              // ✕ where it died
      lx.globalAlpha = 1; lx.strokeStyle = COL.drop; lx.lineWidth = 1.4;
      const r = 3;
      lx.beginPath();
      lx.moveTo(hx - r, hy - r); lx.lineTo(hx + r, hy + r);
      lx.moveTo(hx + r, hy - r); lx.lineTo(hx - r, hy + r); lx.stroke();
    }
    lx.globalAlpha = 1;
  }
}
function estOneWay() {
  const lat = +$("#sl-latency_ms").value || 5;
  return lat + 60;
}

/* ================================================================== *
 *  SCOPE CHARTS
 * ================================================================== */
function lineChart(cv, series, opts) {
  const {w, h} = fit(cv);
  const ctx = cv.getContext("2d");
  ctx.clearRect(0, 0, w, h);
  const padL = 34, padR = 8, padT = 8, padB = 14;
  const all = series.flatMap(s => s.pts);
  if (!all.length) return;
  // While a run is live, follow a sliding window ending at "now".  Once it has
  // finished, freeze to the whole run so the sawtooth stays on screen.
  let tMax, tMin;
  if (S.running || !S.tEnd) {
    tMax = now() - S.t0;
    tMin = Math.max(0, tMax - (opts.windowMs || 20000));
  } else {
    tMax = S.tEnd + 200;
    tMin = Math.min(...all.map(p => p.t), tMax) - 100;
  }
  const scalePts = series.filter(s => !s.noscale).flatMap(s => s.pts);
  let vMax = opts.vMax || Math.max(...(scalePts.length ? scalePts : all).map(p => p.v), 1);
  let vMin = opts.vMin != null ? opts.vMin : 0;
  vMax *= 1.12;

  const X = (t) => padL + (t - tMin) / (tMax - tMin || 1) * (w - padL - padR);
  const Y = (v) => h - padB - (v - vMin) / (vMax - vMin || 1) * (h - padT - padB);

  // grid + y labels
  ctx.strokeStyle = COL.line; ctx.fillStyle = COL.muted;
  ctx.font = "9px ui-monospace, monospace"; ctx.lineWidth = 1;
  for (let i = 0; i <= 2; i++) {
    const v = vMin + (vMax - vMin) * i / 2;
    const y = Y(v);
    ctx.globalAlpha = 0.5;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.fillText(fmt(v), 2, y + 3);
  }
  for (const s of series) {
    const pts = s.pts.filter(p => p.t >= tMin - 500);
    if (!pts.length) continue;
    ctx.strokeStyle = s.color; ctx.lineWidth = s.width || 1.4;
    ctx.setLineDash(s.dash || []);
    ctx.beginPath();
    pts.forEach((p, i) => {
      const x = X(p.t), y = Y(p.v);
      s.step && i ? (ctx.lineTo(x, Y(pts[i - 1].v)), ctx.lineTo(x, y))
                  : (i ? ctx.lineTo(x, y) : ctx.moveTo(x, y));
    });
    ctx.stroke(); ctx.setLineDash([]);
    if (s.fill) {
      const last = pts[pts.length - 1];
      ctx.lineTo(X(last.t), Y(vMin)); ctx.lineTo(X(pts[0].t), Y(vMin));
      ctx.globalAlpha = 0.12; ctx.fillStyle = s.color; ctx.fill(); ctx.globalAlpha = 1;
    }
  }
}
function fmt(v) {
  if (v >= 1000) return (v / 1000).toFixed(1) + "k";
  if (v >= 100)  return v.toFixed(0);
  if (v >= 10)   return v.toFixed(0);
  return v.toFixed(1);
}

function drawCharts() {
  lineChart($("#cv-cwnd"), [
    {pts: S.rwnd, color: COL.muted, dash: [4, 3], width: 1, noscale: true},
    {pts: S.sst,  color: COL.retx, dash: [2, 3], width: 1},
    {pts: S.cwnd, color: COL.data, width: 1.8, step: true},
  ], {windowMs: 22000, vMin: 0});

  lineChart($("#cv-tput"), [
    {pts: S.tput, color: COL.ack, width: 1.6, fill: true},
  ], {windowMs: 22000, vMin: 0});

  lineChart($("#cv-rtt"), [
    {pts: S.rto,  color: COL.drop, dash: [2, 3], width: 1},
    {pts: S.rtt,  color: COL.muted, width: 1},
    {pts: S.srtt, color: COL.ctrl, width: 1.8},
  ], {windowMs: 22000, vMin: 0});
}

/* ------------------------------------------------------------------ *
 *  stat tiles
 * ------------------------------------------------------------------ */
function drawStats() {
  const f = S.final || {};
  const pct = S.totalSegs ? Math.min(100, S.count.delivered / (S.totalSegs * 1024) * 100) : 0;
  const tiles = [
    ["goodput", (f.goodput_kbps ? (f.goodput_kbps / 1000).toFixed(2)
                 : S.cur.goodput.toFixed(2)) + " Mb/s", ""],
    ["progress", pct.toFixed(0) + " %", ""],
    ["cwnd", S.cur.cwnd ? S.cur.cwnd.toFixed(1) : "–", ""],
    ["in flight", S.cur.inflight || "–", ""],
    ["srtt", (S.cur.srtt || f.srtt_ms || 0).toFixed(1) + " ms", ""],
    ["cc state", (S.cur.state || "–").replace(/_/g, " "), ""],
    ["retransmits", (f.retransmits ?? S.count.retx), S.count.retx ? "bad" : "good"],
    ["timeouts", (f.timeouts ?? S.count.timeout), (f.timeouts ?? S.count.timeout) ? "bad" : "good"],
    ["dup acks", (f.dupacks ?? S.count.dupack), ""],
  ];
  $("#stats").innerHTML = tiles.map(([l, v, cls]) =>
    `<div class="stat ${cls}"><div class="v">${v}</div><div class="l">${l}</div></div>`
  ).join("");
}

/* ------------------------------------------------------------------ *
 *  main render loop
 * ------------------------------------------------------------------ */
function frame() {
  drawLadder();
  drawCharts();
  drawStats();
  requestAnimationFrame(frame);
}

/* ------------------------------------------------------------------ *
 *  PNG export (stitches the three scope charts)
 * ------------------------------------------------------------------ */
function exportPNG() {
  const cvs = ["#cv-cwnd", "#cv-tput", "#cv-rtt"].map(s => $(s));
  const pad = 16, W = cvs[0].width, gap = 10;
  const out = document.createElement("canvas");
  out.width = W + pad * 2;
  out.height = cvs.reduce((a, c) => a + c.height, 0) + pad * 2 + gap * 2;
  const g = out.getContext("2d");
  g.fillStyle = "#0a0d13"; g.fillRect(0, 0, out.width, out.height);
  let y = pad;
  for (const c of cvs) { g.drawImage(c, pad, y); y += c.height + gap; }
  const a = document.createElement("a");
  a.download = "transportlab-charts.png";
  a.href = out.toDataURL("image/png");
  a.click();
}

/* ------------------------------------------------------------------ *
 *  wiring
 * ------------------------------------------------------------------ */
async function init() {
  buildSliders();
  const st = await fetch("/state").then(r => r.json());
  buildPresets(st.presets || {});
  setSliders(st.link || {});
  const p = st.params || {};
  if (p.arq)  $("#p-arq").value = p.arq;
  if (p.cc)   $("#p-cc").value = p.cc;
  if (p.rwnd) $("#p-rwnd").value = p.rwnd;
  if (p.mss)  $("#p-mss").value = p.mss;
  if (p.size_bytes) $("#p-size").value = (p.size_bytes / 1e6);

  for (const id of ["p-arq", "p-cc"])
    $("#" + id).addEventListener("change", pushParams);
  for (const id of ["p-size", "p-rwnd", "p-mss"])
    $("#" + id).addEventListener("change", pushParams);

  $("#btn-start").onclick = () => post("/run", {action: "start"});
  $("#btn-stop").onclick  = () => post("/run", {action: "stop"});
  $("#btn-png").onclick   = exportPNG;

  if (st.running) setStatus("running");
  connect();
  requestAnimationFrame(frame);
}
function pushParams() {
  post("/params", {
    arq: $("#p-arq").value,
    cc: $("#p-cc").value,
    rwnd: +$("#p-rwnd").value,
    mss: +$("#p-mss").value,
    size_bytes: Math.round(+$("#p-size").value * 1e6),
  });
}
window.addEventListener("resize", () => { drawLadder(); drawCharts(); });
init();
