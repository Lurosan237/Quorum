// The demo page: runs the Go simulator (quorum.wasm) and draws it.
"use strict";

const N = 5, CX = 300, CY = 300, R = 190, RC = 280, NODE_R = 44, TAIL = 16;
const $ = (id) => document.getElementById(id);
const svgNS = "http://www.w3.org/2000/svg";
const TERM_COLORS = ["#ffc857", "#6cc5a1", "#8fa8ff", "#f28fb1", "#c3e88d", "#7fd6e8"];
const termColor = (t) => TERM_COLORS[t % TERM_COLORS.length];
const MSG_STYLE = {
  PreVote: ["#ffc857", 4], PreVoteResp: ["#ffc857", 3], Vote: ["#ffc857", 5], VoteResp: ["#ffc857", 3.5],
  App: ["#6cc5a1", 5], AppResp: ["#6cc5a1", 3], Heartbeat: ["#5d6c99", 3], Snap: ["#edebf5", 7],
  ReadIndex: ["#8fa8ff", 3.5], ReadIndexResp: ["#8fa8ff", 3], Request: ["#edebf5", 4], Reply: ["#edebf5", 3],
};

const pos = (i) => {
  const a = (-90 + i * 72) * Math.PI / 180;
  return [CX + R * Math.cos(a), CY + R * Math.sin(a)];
};
const clientPos = (k) => {
  const a = (-90 + 36 + k * 72) * Math.PI / 180;
  return [CX + RC * Math.cos(a), CY + RC * Math.sin(a)];
};
const at = (end) => (end >= 0 ? pos(end) : clientPos(-1 - end));

let ready = false, running = true, simTime = 0, last = performance.now(), view = null;
let weather = "calm", seed = 1 + Math.floor(Math.random() * 100000);
const feed = [];

// ---- boot -----------------------------------------------------------------------------------------
const go = new Go();
WebAssembly.instantiateStreaming(fetch("quorum.wasm"), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((e) => { $("loading").textContent = "Could not start the simulator: " + e.message; });
window.addEventListener("quorum-ready", () => {
  start();
  $("loading").remove();
  ready = true;
  requestAnimationFrame(frame);
});

function start() {
  quorum.start(weather, seed, 4);
  simTime = 0;
  feed.length = 0;
  renderFeed();
  $("verdict").innerHTML = "";
  $("answer").textContent = "";
  buildStatic();
}

// ---- the ring -------------------------------------------------------------------------------------
function el(tag, attrs, parent) {
  const e = document.createElementNS(svgNS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(e);
  return e;
}

const nodeEls = [];
function buildStatic() {
  for (const g of ["links", "halos", "clients", "nodes"]) $(g).innerHTML = "";
  nodeEls.length = 0;
  for (let i = 0; i < N; i++) for (let j = i + 1; j < N; j++) {
    const [x1, y1] = pos(i), [x2, y2] = pos(j);
    el("line", { x1, y1, x2, y2, class: "link", "data-a": i, "data-b": j }, $("links"));
  }
  for (let k = 0; k < 5; k++) {
    const [x, y] = clientPos(k);
    const g = el("g", { class: "client" + (k === 4 ? " you" : ""), opacity: k === 4 ? 0.25 : 1 }, $("clients"));
    el("circle", { cx: x, cy: y, r: k === 4 ? 9 : 7 }, g);
    el("text", { x, y: y + 24 }, g).textContent = k === 4 ? "you" : "client " + (k + 1);
  }
  for (let i = 0; i < N; i++) {
    const [x, y] = pos(i);
    const halo = el("circle", { cx: x, cy: y, r: NODE_R * 1.9, fill: "url(#halo)", opacity: 0 }, $("halos"));
    const g = el("g", { class: "node", tabindex: 0, role: "button", "aria-label": "Server " + i }, $("nodes"));
    el("circle", { cx: x, cy: y, r: NODE_R + 8, class: "fence" }, g);
    el("circle", { cx: x, cy: y, r: NODE_R, class: "body" }, g);
    const name = el("text", { x, y: y + 2, class: "name" }, g);
    name.textContent = "S" + i;
    const sub = el("text", { x, y: y + 22, class: "sub" }, g);
    const outer = i === 0 ? y - NODE_R - 14 : y + NODE_R + 22;
    const role = el("text", { x, y: outer, class: "role" }, g);
    g.addEventListener("click", (e) => openPop(i, e));
    g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openPop(i); } });
    nodeEls.push({ g, sub, role, halo });
  }
}

function drawRing(v, t) {
  v.nodes.forEach((n, i) => {
    const e = nodeEls[i];
    const isolated = n.side === 0 && v.nodes.filter((m) => m.side === 0).length === 1;
    e.g.setAttribute("class", "node " + (n.up ? n.role : "down") + (isolated ? " isolated" : ""));
    e.sub.textContent = n.up ? "term " + n.term : "no power";
    e.role.textContent = !n.up ? "off" : n.role === "leader" ? "leader" : n.role === "follower" ? "" : n.role === "candidate" ? "asking for votes" : "testing the waters";
    e.halo.setAttribute("opacity", n.up && n.role === "leader" ? 1 : 0);
    e.g.setAttribute("aria-label", `Server ${i}: ${n.up ? n.role + ", term " + n.term : "no power"}`);
  });
  for (const line of $("links").children) {
    const a = v.nodes[+line.dataset.a], b = v.nodes[+line.dataset.b];
    const cut = a.side >= 0 && b.side >= 0 && a.side !== b.side;
    line.setAttribute("class", "link" + (cut ? " cut" : ""));
  }
  if (v.you) $("clients").lastChild.setAttribute("opacity", 1);
  // messages, placed by where they are between their send time and their arrival
  let html = "";
  for (const m of v.msgs || []) {
    const [x1, y1] = at(m.from), [x2, y2] = at(m.to);
    let p = (t - m.sent) / Math.max(1, m.at - m.sent);
    p = Math.max(0, Math.min(1, p));
    if (m.cut) p = Math.min(p, 0.45);
    const [c, r] = MSG_STYLE[m.kind] || ["#edebf5", 3];
    const x = x1 + (x2 - x1) * p, y = y1 + (y2 - y1) * p;
    html += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}" fill="${m.cut ? "#f2545b" : c}"${m.cut ? ` opacity="${(1 - p).toFixed(2)}"` : ""}/>`;
  }
  $("msgs").innerHTML = html;
}

// ---- the headline: how many servers have power ----------------------------------------------------
let lastUp = -1;
function drawTitle(v) {
  const up = v.nodes.filter((n) => n.up).length;
  if (up === lastUp) return;
  lastUp = up;
  const t = $("title");
  t.classList.toggle("dark", up < 3);
  const tail = up === 5 ? "Three are enough to agree." : up >= 3 ? "Still enough to agree." : "Not enough to agree on anything new.";
  t.innerHTML = `<span class="lit">${up} of 5</span> server${up === 1 ? " has" : "s have"} power.<br><span class="unlit">${tail}</span>`;
}

// ---- logs -----------------------------------------------------------------------------------------
function drawLogs(v) {
  let html = "";
  for (const n of v.nodes) {
    html += `<div class="logrow${n.up ? "" : " down"}"><div class="who">S${n.id}</div><div class="cells">`;
    if (!n.up) {
      html += `<span class="snapcell">no power: its log is on disk, waiting</span>`;
    } else {
      if (n.snap > 0) html += `<span class="snapcell">1–${n.snap} in a snapshot</span>`;
      for (const e of n.tail || []) {
        const c = termColor(e.t);
        const label = `#${e.i}, term ${e.t}${e.op ? ": " + e.op : ": new leader's no-op"}`;
        html += `<span class="cell${e.i > n.commit ? " open" : ""}" style="background:${c};border-color:${c}" title="${escapeHTML(label)}"></span>`;
      }
    }
    html += `</div><div class="data">`;
    if (n.up) for (const k of ["k0", "k1", "k2"]) {
      const val = (n.data && n.data[k]) || "";
      html += `<div>${k} = <span>${escapeHTML(val.length > 40 ? "…" + val.slice(-40) : val || "(empty)")}</span></div>`;
    }
    html += `</div></div>`;
  }
  $("logs").innerHTML = html;
}

function escapeHTML(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// ---- events ---------------------------------------------------------------------------------------
function addEvents(evs) {
  for (const e of evs) feed.unshift(e);
  if (feed.length > 14) feed.length = 14;
  if (evs.length) renderFeed();
}
function renderFeed() {
  $("feed").innerHTML = feed.map((e) =>
    `<li class="${e.kind}"><time>${(e.t / 100).toFixed(1)} s</time><span>${escapeHTML(cap(e.text))}</span></li>`).join("");
}
const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);

// ---- the loop -------------------------------------------------------------------------------------
const tps = () => 4 * Math.pow(25, $("speed").value / 100); // 4 to 100 ticks a second (100 = real time)
let logsAt = 0, youWaiting = false;

function frame(now) {
  const dt = Math.min(0.1, (now - last) / 1000);
  last = now;
  if (running) {
    simTime += dt * tps();
    const whole = Math.floor(simTime) - (view ? view.now : 0);
    if (whole > 0) quorum.step(Math.min(whole, 50));
  }
  view = JSON.parse(quorum.view(TAIL));
  if (view.now < Math.floor(simTime) - 50) simTime = view.now; // fell behind: don't try to catch up
  drawRing(view, running ? simTime : view.now);
  drawTitle(view);
  addEvents(view.events || []);
  $("clock").textContent = (view.now / 100).toFixed(1) + " s";
  $("sAnswers").textContent = view.completed.toLocaleString();
  $("sLeaders").textContent = view.elections;
  $("sCuts").textContent = view.crashes;
  $("sCommitted").textContent = view.committed.toLocaleString();
  if (now - logsAt > 150) { drawLogs(view); logsAt = now; }
  if (youWaiting && !view.waiting) {
    youWaiting = false;
    const mine = feed.find((e) => e.kind === "you");
    $("answer").className = "answer";
    $("answer").textContent = mine ? cap(mine.text) : "Answered.";
  }
  requestAnimationFrame(frame);
}

// ---- controls -------------------------------------------------------------------------------------
const pop = $("pop");
function openPop(i, ev) {
  const n = view.nodes[i];
  const [x, y] = pos(i);
  const scale = $("svg").clientWidth / 600;
  pop.style.left = Math.min(x * scale + 20, $("svg").clientWidth - 200) + "px";
  pop.style.top = (y * scale - 20) + "px";
  const cutOff = n.side === 0;
  pop.innerHTML = `<b>Server ${i}</b>
    <button data-a="${n.up ? "crash" : "restart"}">${n.up ? "Cut its power" : "Restore its power"}</button>
    ${cutOff ? `<button data-a="heal">Reconnect it</button>` : `<button data-a="isolate">Cut it off the network</button>`}`;
  pop.classList.add("open");
  pop.querySelectorAll("button").forEach((b) => b.addEventListener("click", () => {
    quorum[b.dataset.a](i);
    pop.classList.remove("open");
  }));
  pop.querySelector("button").focus();
  if (ev) ev.stopPropagation();
}
document.addEventListener("click", (e) => { if (!pop.contains(e.target)) pop.classList.remove("open"); });
document.addEventListener("keydown", (e) => { if (e.key === "Escape") pop.classList.remove("open"); });

$("weather").addEventListener("click", (e) => {
  const b = e.target.closest("button");
  if (!b || !ready) return;
  weather = b.dataset.p;
  quorum.weather(weather);
  for (const o of $("weather").children) o.setAttribute("aria-pressed", o === b);
});
$("pause").addEventListener("click", () => {
  running = !running;
  $("pause").textContent = running ? "Pause" : "Resume";
  if (running) simTime = view.now;
});
$("reset").addEventListener("click", () => {
  if (!ready) return;
  seed = 1 + Math.floor(Math.random() * 100000);
  start();
  quorum.weather(weather);
  view = null;
  lastUp = -1;
});

document.querySelectorAll("[data-k]").forEach((b) => b.addEventListener("click", () => {
  if (!ready) return;
  const kind = b.dataset.k, key = $("key").value, val = kind === "get" ? "" : $("val").value + " ";
  if (!quorum.ask(kind, key, val)) {
    $("answer").textContent = "Your last request is still on its way. Each client sends one at a time.";
    return;
  }
  view.you = true;
  youWaiting = true;
  $("answer").className = "answer wait";
  $("answer").textContent = `Sent ${kind} ${key}. Waiting for a majority to agree…`;
}));

$("check").addEventListener("click", () => {
  if (!ready) return;
  const r = JSON.parse(quorum.check());
  const v = $("verdict");
  if (r.ok && !(r.violations || []).length) {
    v.innerHTML = `<div class="verdict">All ${r.ops.toLocaleString()} answers fit one order of events. The checker
      tried ${r.explored.toLocaleString()} orderings to prove it.</div>`;
  } else if (r.gaveUp) {
    v.innerHTML = `<div class="verdict bad">The checker ran out of time on one key. That is not a failure, but it is not a pass either.</div>`;
  } else {
    v.innerHTML = `<div class="verdict bad">Something is wrong. ${r.badKey ? "The answers on " + escapeHTML(r.badKey) + " fit no order of events." : ""}
      ${(r.violations || []).map(escapeHTML).join("<br>")}<pre>${escapeHTML(r.explain || "")}</pre></div>`;
  }
});

document.querySelectorAll("[data-bug]").forEach((b) => b.addEventListener("click", () => {
  if (!ready) return;
  const out = b.closest(".bug").querySelector("[data-out]");
  out.textContent = "Running 30 simulated seconds…";
  setTimeout(() => {
    const t0 = performance.now();
    const r = JSON.parse(quorum.run(b.dataset.p, +b.dataset.seed, 3000, b.dataset.bug));
    const ms = Math.round(performance.now() - t0);
    const pl = (n, w) => `${n.toLocaleString()} ${w}${n === 1 ? "" : "s"}`;
    const what = `${pl(r.completed, "answer")}, ${pl(r.elections, "leader")}, ${pl(r.crashes, "power cut")}, in ${ms} ms.`;
    if (r.ok) {
      out.innerHTML = `<span style="color:var(--ok)">Every check passed.</span> ${what}`;
    } else if ((r.violations || []).length) {
      out.innerHTML = `<span style="color:var(--cut)">Caught:</span> ${escapeHTML(r.violations[0].replace(/^t=\d+: /, ""))}. ${what}`;
    } else {
      out.innerHTML = `<span style="color:var(--cut)">Caught:</span> the answers on ${escapeHTML(r.badKey)} fit no order of events. ${what}`;
    }
  }, 30);
}));
