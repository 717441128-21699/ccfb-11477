/* 足球数据融合控制台前端：WebSocket 实时推送，失败自动降级为轮询。 */
"use strict";

const TEAM = {
  RED_LIONS: "红狮 Red Lions", BLUE_SHARKS: "蓝鲨 Blue Sharks",
};
const PLAYER = {
  P_FOSTER: "福斯特", P_DIAZ: "迪亚斯", P_CHEN: "陈", P_OKAFOR: "奥卡福",
  P_ROSSI: "罗西", P_MWANGI: "姆万吉", P_PARK: "朴", P_NOVAK: "诺瓦克",
  P_SILVA: "席尔瓦", P_KOCH: "科赫",
};
const EVENT = {
  goal: "进球", own_goal: "乌龙球", penalty_goal: "点球进球",
  penalty_missed: "点球未进", red_card: "红牌", yellow_card: "黄牌",
  substitution: "换人", var: "VAR 复核", kickoff: "开球",
  halftime: "半场", fulltime: "终场", unknown: "未知类型",
};
const STATUS_TEXT = {
  confirmed: "多数确认", official: "官方确认", pending: "待确认", retracted: "已撤回",
};
const MATCH_STATUS = {
  scheduled: "未开始", live: "进行中", halftime: "半场", finished: "已完赛",
};
const KIND_TEXT = {
  retraction: "报道撤回", false_report: "误报",
  status_change: "状态变更", score_revision: "比分修正",
};

let DATA = null;
let MATCHES = [];
let currentMatch = null;
let ws = null;
let pollTimer = null;
let wsMode = "none";

const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const fmtTime = (ts) => new Date(ts * 1000).toLocaleTimeString("zh-CN",
  { hour12: false });

// ---------------- 实时通道 ----------------------------------------------------

async function loadMatches() {
  const res = await fetch("/api/matches");
  MATCHES = await res.json();
  const sel = $("#matchSelect");
  if (!currentMatch) currentMatch = MATCHES[0]?.match_id ?? null;
  sel.innerHTML = MATCHES.map((m) =>
    `<option value="${m.match_id}" ${m.match_id === currentMatch ? "selected" : ""}>${esc(m.label)}</option>`
  ).join("");
  renderMatches();
}

function connect(matchId) {
  if (ws) { try { ws.close(); } catch { /* ignore */ } ws = null; }
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  setWsState("连接中…", "");
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/${matchId}`);
  ws.onmessage = (ev) => {
    DATA = JSON.parse(ev.data);
    if (wsMode !== "ws") { wsMode = "ws"; setWsState("实时 WebSocket", "ok"); }
    renderAll();
  };
  ws.onclose = () => {
    if (wsMode !== "poll") { wsMode = "poll"; setWsState("已降级轮询", "poll"); }
    startPolling(matchId);
  };
  ws.onerror = () => ws.close();
}

function startPolling(matchId) {
  if (pollTimer) return;
  const tick = async () => {
    try {
      const res = await fetch(`/api/state/${matchId}`);
      DATA = await res.json();
      renderAll();
    } catch { setWsState("连接失败", "bad"); }
  };
  tick();
  pollTimer = setInterval(tick, 1500);
}

function setWsState(text, cls) {
  const el = $("#wsState");
  el.textContent = text;
  el.className = `ws-state ${cls}`;
}

// ---------------- 渲染 --------------------------------------------------------

function renderAll() {
  if (!DATA) return;
  renderScoreboard();
  renderLive();
  renderCompare();
  renderCorrections();
  renderReport();
}

function renderMatches() {
  $("#panelMatches").innerHTML = `<div class="cards">${MATCHES.map((m) => `
    <div class="match-card" data-match="${m.match_id}">
      <h3>${esc(m.label)}</h3>
      <div class="row"><span class="badge ${m.status}">${MATCH_STATUS[m.status] || m.status}</span>
      <span>${m.score[0]} : ${m.score[1]}</span></div>
      <div class="row" style="margin-top:8px"><span>ID: ${m.match_id}</span>
      <span>点击切换 →</span></div>
    </div>`).join("")}</div>`;
  document.querySelectorAll(".match-card").forEach((card) =>
    card.addEventListener("click", () => {
      currentMatch = card.dataset.match;
      $("#matchSelect").value = currentMatch;
      connect(currentMatch);
    }));
}

function renderScoreboard() {
  const { state, source_status: ss, provider_meta: meta, label } = DATA;
  const chips = Object.entries(meta).map(([p, m]) => `
    <span class="chip ${ss[p] || "disconnected"} ${p}">
      <span class="dot"></span>${p} · ${transportText(m.transport)}
      ${m.is_official ? '<span class="pill">官方</span>' : ""}
      <small>（${connText(ss[p])}）</small>
    </span>`).join("");
  const pending = state.pending_goals.length
    ? `<div class="pending-note">⚠ 待官方确认进球 ${state.pending_goals.length} 个（不计入比分）：` +
      state.pending_goals.map((g) => `${g.minute}' ${esc(TEAM[g.team] || g.team)}`).join("；") +
      "</div>" : "";
  $("#scoreboard").innerHTML = `
    <div class="team-block"><div class="team-name">${esc(TEAM[state.home] || state.home)}</div></div>
    <div class="match-meta">
      <div><span class="score-big">${state.score_home}</span>
           <span class="score-vs">:</span>
           <span class="score-big">${state.score_away}</span></div>
      <span class="badge ${state.status}">${MATCH_STATUS[state.status] || state.status}</span>
      <div class="src-chips">${chips}</div>${pending}
    </div>
    <div class="team-block away"><div class="team-name">${esc(TEAM[state.away] || state.away)}</div>
      <div style="color:var(--muted);font-size:12px;margin-top:6px">${esc(label)}</div></div>`;
}

function transportText(t) {
  return { websocket: "WebSocket", sse: "SSE", long_poll: "长轮询" }[t] || t;
}
function connText(s) {
  return { connected: "在线", disconnected: "离线", reconnecting: "重连中" }[s] || s;
}

function renderLive() {
  const incidents = [...DATA.state.incidents].sort(
    (a, b) => (a.minute - b.minute) || ((a.first_arrival || 0) - (b.first_arrival || 0)));
  if (!incidents.length) {
    $("#panelLive").innerHTML = `<div class="empty">等待数据商推送…</div>`;
    return;
  }
  const earliestByInc = new Map();
  $("#panelLive").innerHTML = `<div class="timeline">${incidents.map((inc) => {
    const arrivals = Object.values(inc.sources).map((r) => r.received_at);
    const earliest = Math.min(...arrivals);
    const title = eventTitle(inc);
    const votes = Object.entries(inc.sources).map(([p, r]) => {
      const lag = r.received_at - earliest;
      const retracted = inc.retracted_sources.includes(p);
      return `<span class="vote ${p} ${retracted ? "retracted" : ""}">
        ${p}${retracted ? "（已撤）" : ""}
        <span class="${lag < 0.05 ? "fast" : "lag"}">
        ${lag < 0.05 ? "⚡最快" : "+" + lag.toFixed(2) + "s"}</span>
        <div style="color:var(--muted);font-size:11px">到达 ${fmtTime(r.received_at)} · seq ${r.seq}</div>
      </span>`;
    }).join("");
    const detail = inc.player ? `球员：${esc(PLAYER[inc.player] || inc.player)}` : "";
    const sub = inc.event_type === "substitution"
      ? `换上 ${esc(PLAYER[inc.player_in] || inc.player_in)} / 换下 ${esc(PLAYER[inc.player_out] || inc.player_out)}`
      : detail;
    return `<div class="incident ${inc.status}">
      <div class="inc-head">
        <span class="minute">${inc.minute}'</span>
        <span class="inc-title">${esc(title)}</span>
        <span class="status-tag ${inc.status}">${STATUS_TEXT[inc.status] || inc.status}</span>
        <span style="color:var(--muted);font-size:12px">${esc(TEAM[inc.team] || inc.team)}</span>
      </div>
      ${sub ? `<div class="inc-detail">${sub}</div>` : ""}
      <div class="votes">${votes}</div>
    </div>`;
  }).join("")}</div>`;
}

function eventTitle(inc) {
  let t = EVENT[inc.event_type] || inc.event_type;
  if (inc.score_home !== null && inc.score_home !== undefined) {
    t += ` ${inc.score_home}-${inc.score_away}`;
  }
  return t;
}

function renderCompare() {
  const rows = [...DATA.state.incidents].sort(
    (a, b) => (a.minute - b.minute) || ((a.first_arrival || 0) - (b.first_arrival || 0)));
  const head = `<tr><th>事件</th><th>分钟</th><th>状态</th>
      <th class="num">alpha（官方·WS）</th><th class="num">beta（SSE）</th>
      <th class="num">gamma（长轮询）</th><th>最快</th></tr>`;
  const body = rows.map((inc) => {
    const arrivals = Object.values(inc.sources).map((r) => r.received_at);
    const earliest = Math.min(...arrivals);
    const cell = (p) => {
      const r = inc.sources[p];
      if (!r) return `<td class="num"><span style="color:var(--muted)">漏报</span></td>`;
      const lag = r.received_at - earliest;
      const retracted = inc.retracted_sources.includes(p);
      const score = r.score_home !== null && r.score_home !== undefined
        ? `<div style="font-size:11px">报分 ${r.score_home}-${r.score_away}</div>` : "";
      return `<td class="num" ${retracted ? 'style="opacity:.45"' : ""}>
        ${retracted ? "已撤回<br>" : ""}+${lag.toFixed(2)}s${score}</td>`;
    };
    const first = Object.entries(inc.sources)
      .filter(([p]) => !inc.retracted_sources.includes(p))
      .sort((a, b) => a[1].received_at - b[1].received_at)[0];
    return `<tr>
      <td>${esc(EVENT[inc.event_type] || inc.event_type)}</td>
      <td>${inc.minute}'</td>
      <td><span class="status-tag ${inc.status}">${STATUS_TEXT[inc.status] || inc.status}</span></td>
      ${cell("alpha")}${cell("beta")}${cell("gamma")}
      <td class="srcname ${first ? first[0] : ""}">${first ? first[0] : "—"}</td>
    </tr>`;
  }).join("");
  $("#panelCompare").innerHTML =
    `<p style="color:var(--muted);margin-top:0">延迟口径：相对该事件三家最早到达时间的差值（秒）；0.00 即最快。</p>
     <table>${head}${body}</table>`;
}

function renderCorrections() {
  const items = [...DATA.state.corrections].reverse();
  if (!items.length) {
    $("#panelCorrections").innerHTML = `<div class="empty">暂无纠错记录</div>`;
    return;
  }
  $("#panelCorrections").innerHTML = items.map((c) => `
    <div class="corr-item ${c.kind}">
      <div class="corr-head">
        <span class="pill">${esc(KIND_TEXT[c.kind] || c.kind)}</span>
        <span>来源：<b class="srcname ${c.provider === "fusion" ? "" : c.provider}">${esc(c.provider)}</b></span>
        ${c.minute !== null && c.minute !== undefined ? `<span>${c.minute}'</span>` : ""}
        <span>${fmtTime(c.at)}</span>
      </div>
      <div>${esc(c.detail)}</div>
    </div>`).join("");
}

function renderReport() {
  const panel = $("#panelReport");
  const report = DATA.report;
  if (!report) {
    panel.innerHTML = `<div class="empty">比赛尚未完赛，终场后自动生成赛后报告…</div>`;
    return;
  }
  const cards = report.ranking.map((s, i) => `
    <div class="summary-card">
      <h4><span class="srcname ${s.provider}">${s.provider}</span>
        <span class="rank-${i + 1}">#${i + 1}</span></h4>
      <div class="kv"><span>传输方式</span><b>${transportText(s.transport)}${s.is_official ? "（官方）" : ""}</b></div>
      <div class="kv"><span>准确率</span><b>${(s.accuracy * 100).toFixed(1)}%</b></div>
      <div class="kv"><span>平均延迟</span><b>${s.avg_latency === null ? "—" : s.avg_latency + "s"}</b></div>
      <div class="kv"><span>最大延迟</span><b>${s.max_latency === null ? "—" : s.max_latency + "s"}</b></div>
      <div class="kv"><span>最快次数</span><b>${s.first_place_count}</b></div>
      <div class="kv"><span>正确 / 错误</span><b>${s.correct} / ${s.wrong}</b></div>
      <div class="kv"><span>撤回 / 漏报</span><b>${s.retracted} / ${s.missing}</b></div>
      ${s.score_mismatches.length ? `<div class="kv"><span>比分错报</span>
        <b style="color:var(--red)">${s.score_mismatches.length} 次</b></div>` : ""}
    </div>`).join("");
  panel.innerHTML = `
    <h3 class="section-title">最终比分 ${report.final_score[0]}-${report.final_score[1]}
      · 融合事件 ${report.generated_events} / 剧本事件 ${report.script_event_count}</h3>
    <div class="summary-cards">${cards}</div>
    <h3 class="section-title">逐事件到达时间差（秒，相对最快源）</h3>
    <table><tr><th>事件</th><th>分钟</th><th>状态</th>
        <th class="num">alpha</th><th class="num">beta</th><th class="num">gamma</th><th>最快</th></tr>
      ${report.event_timeline.map((r) => `<tr>
        <td>${esc(EVENT[r.event_type] || r.event_type)}</td><td>${r.minute}'</td>
        <td>${esc(STATUS_TEXT[r.status] || r.status)}</td>
        ${["alpha", "beta", "gamma"].map((p) =>
          `<td class="num">${p in r.arrival_offsets ? r.arrival_offsets[p].toFixed(2) : "—"}</td>`).join("")}
        <td class="srcname ${r.first_provider || ""}">${r.first_provider || "—"}</td>
      </tr>`).join("")}
    </table>`;
}

// ---------------- 启动 --------------------------------------------------------

$("#matchSelect").addEventListener("change", (e) => {
  currentMatch = e.target.value;
  connect(currentMatch);
});
document.querySelectorAll(".tab").forEach((tab) =>
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    tab.classList.add("active");
    document.querySelector(`.panel[data-panel="${tab.dataset.tab}"]`).classList.add("active");
  }));

(async function init() {
  await loadMatches();
  if (currentMatch) connect(currentMatch);
  setInterval(loadMatches, 4000);
})();
