/* 足球多源数据融合台前端：WS 增量推送 + 3s 轮询兜底 */
"use strict";

const state = {
  matchId: null,
  detail: null,          // /api/matches/{id} 全量
  summary: null,
  transport: {},
  report: null,
  ws: null,
  wsOk: false,
  pollTimer: null,
};

const $ = (sel) => document.querySelector(sel);
const fmtTime = (ms) => ms == null ? "-" : new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const ETYPE_CN = {
  goal: "进球", penalty_goal: "点球进球", penalty_miss: "点球未进", own_goal: "乌龙球",
  yellow_card: "黄牌", red_card: "红牌", substitution: "换人",
  kickoff: "开球", halftime: "半场", fulltime: "终场",
};
const SOURCE_CN = { A: "A · 官方WS", B: "B · 快速WS", C: "C · 长轮询" };

// ---------------- 数据获取 ----------------

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return r.json();
}

async function loadMatches() {
  const data = await fetchJSON("/api/matches");
  if (!data.matches.length) return;
  state.matchId = data.matches[0].match_id;
  await loadDetail();
}

async function loadDetail() {
  const d = await fetchJSON(`/api/matches/${state.matchId}`);
  state.detail = d;
  state.summary = d.summary;
  state.transport = d.transport;
  renderAll();
}

async function loadReport() {
  const area = $("#report-area");
  try {
    state.report = await fetchJSON(`/api/matches/${state.matchId}/report`);
    renderReport();
  } catch (e) {
    area.innerHTML = `<p class="empty">比赛尚未结束，终场后自动生成报告。</p><button class="btn" id="refresh-report">刷新报告</button>`;
    $("#refresh-report").addEventListener("click", loadReport);
  }
}

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  state.ws = ws;
  ws.onopen = () => setWsStatus(true);
  ws.onclose = () => { setWsStatus(false); setTimeout(connectWS, 2000); };
  ws.onerror = () => ws.close();
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    handlePush(msg);
  };
}

function setWsStatus(ok) {
  state.wsOk = ok;
  $("#ws-dot").className = `dot ${ok ? "on" : "off"}`;
  $("#ws-text").textContent = ok ? "实时推送已连接" : "推送断开，轮询兜底中…";
}

function handlePush(msg) {
  if (!state.detail) return;
  const d = state.detail;
  switch (msg.kind) {
    case "snapshot":
      state.summary = msg.data.summary;
      state.transport = msg.data.transport;
      break;
    case "event_new":
    case "event_upsert":
    case "retraction": {
      const idx = d.events.findIndex(e => e.fused_id === msg.data.fused_id);
      if (idx >= 0) d.events[idx] = decorate(msg.data); else d.events.push(decorate(msg.data));
      break;
    }
    case "phase":
      d.summary.phase = msg.data.phase;
      state.summary = d.summary;
      if (msg.data.phase === "FINISHED") loadReport();
      break;
    case "transport": {
      const s = msg.data.source;
      Object.assign(d.transport[s], msg.data);
      state.transport = d.transport;
      break;
    }
  }
  // 增量推送后轻量同步一次摘要（比分等聚合字段）
  refreshSummarySoon();
  renderAll();
}

let summaryTimer = null;
function refreshSummarySoon() {
  if (summaryTimer) return;
  summaryTimer = setTimeout(async () => {
    summaryTimer = null;
    try {
      const data = await fetchJSON("/api/matches");
      const m = data.matches.find(x => x.match_id === state.matchId);
      if (m) { state.summary = m; state.detail.summary = m; renderAll(); }
    } catch (_) { /* 轮询兜底会覆盖 */ }
  }, 400);
}

function decorate(ev) {
  const d = state.detail;
  ev.team_name = d.team_names[ev.team_id] || null;
  ev.player_name = d.player_names[ev.player_id] || null;
  ev.player_on_name = d.player_names[ev.player_on_id] || null;
  ev.player_off_name = d.player_names[ev.player_off_id] || null;
  for (const v of ev.votes) {
    v.team_name = d.team_names[v.team_id] || null;
    v.player_name = d.player_names[v.player_id] || null;
  }
  return ev;
}

// ---------------- 渲染 ----------------

function renderAll() {
  renderMatches();
  renderScoreboard();
  renderTimeline();
  renderSources();
  renderCorrections();
}

function renderMatches() {
  if (!state.summary) return;
  const s = state.summary;
  $("#match-cards").innerHTML = `
    <div class="card">
      <h3>${esc(s.home_name)} vs ${esc(s.away_name)}</h3>
      <div class="meta">${esc(s.competition)} · 赛事号 ${esc(s.match_id)}</div>
      <div class="big-score">
        ${s.authoritative_score[0]} : ${s.authoritative_score[1]}
        ${(s.pending_score[0] !== s.authoritative_score[0] || s.pending_score[1] !== s.authoritative_score[1])
          ? `<span class="pending">待确认比分 ${s.pending_score[0]}:${s.pending_score[1]}</span>` : ""}
      </div>
      <div style="margin-top:12px"><span class="badge ${s.phase}">${phaseCn(s.phase)}</span></div>
    </div>`;
}

function phaseCn(p) {
  return { PRE_MATCH: "未开始", LIVE: "进行中", HALFTIME: "半场", FINISHED: "已结束" }[p] || p;
}

function renderScoreboard() {
  if (!state.summary) return;
  const s = state.summary;
  $("#scoreboard").innerHTML = `
    <div class="team">${esc(s.home_name)}</div>
    <div>
      <div class="score">${s.authoritative_score[0]} : ${s.authoritative_score[1]}</div>
      <div class="auth-note">
        权威比分以官方源 A 为准 ·
        <span class="pending-chip">待确认 ${s.pending_score[0]}:${s.pending_score[1]}</span>
      </div>
    </div>
    <div class="team">${esc(s.away_name)}</div>`;
}

function eventTitle(ev) {
  const name = ev.player_name ? ` · ${ev.player_name}` : "";
  const team = ev.team_name ? `（${ev.team_name}）` : "";
  let extra = "";
  if (ev.event_type === "substitution" && ev.player_off_name) {
    return `${ETYPE_CN[ev.event_type] || ev.event_type} · ${ev.player_off_name} → ${ev.player_on_name || ""} ${team}`;
  }
  if (ev.event_type === "own_goal") extra = " · 乌龙";
  return `${ETYPE_CN[ev.event_type] || ev.event_type}${extra}${name} ${team}`;
}

function renderTimeline() {
  if (!state.detail) return;
  const filter = $("#status-filter").value;
  const events = [...state.detail.events]
    .filter(e => !filter || e.status === filter)
    .sort((a, b) => (a.minute ?? -1) - (b.minute ?? -1) || a.first_seen_ts_ms - b.first_seen_ts_ms);
  $("#timeline").innerHTML = events.map(ev => `
    <div class="event ${ev.status}">
      <div class="event-head">
        <span class="minute">${ev.minute == null ? "" : ev.minute + "'"}</span>
        <span class="etype">${esc(eventTitle(ev))}</span>
        <span class="status-tag ${ev.status}">${statusCn(ev.status)}</span>
      </div>
      <div class="votes">
        ${ev.votes.map(v => `
          <div class="vote ${v.relative_ms === 0 ? "first" : ""} ${v.retracted ? "retracted" : ""}">
            <div><span class="src">${esc(SOURCE_CN[v.source_id] || v.source_id)}</span>
              ${v.relative_ms === 0 ? '<span class="pill ok">最快</span>' : ""}
              ${v.backfilled ? '<span class="bf"> 补</span>' : ""}
            </div>
            <div class="rel">相对到达 +${v.relative_ms} ms</div>
            <div class="lag">${v.lag_ms != null ? `端到端 ${v.lag_ms} ms` : ""}</div>
            ${v.retracted ? "<div style='color:var(--red)'>已撤回</div>" : ""}
          </div>`).join("")}
      </div>
    </div>`).join("") || `<p class="empty">暂无事件</p>`;
}

function statusCn(s) {
  return { PENDING: "待确认", CONFIRMED: "已确认", RETRACTED: "已撤回", CONFLICT: "有冲突" }[s] || s;
}

function renderSources() {
  if (!state.detail) return;
  const t = state.transport;
  $("#source-status").innerHTML = Object.entries(t).map(([sid, tr]) => `
    <div class="card src-card">
      <h3>${esc(SOURCE_CN[sid] || sid)} ${sid === "A" ? '<span class="pill ok">官方</span>' : ""}</h3>
      <div class="line"><span>连接状态</span><span class="${tr.connected ? "online" : "offline"}">${tr.connected ? "在线" : "离线/重连中"}</span></div>
      <div class="line"><span>连接 / 断开次数</span><span>${tr.connect_count} / ${tr.disconnect_count}</span></div>
      <div class="line"><span>补传事件数</span><span>${tr.backfilled_events}</span></div>
      <div class="line"><span>最新序号 seq</span><span>${tr.last_seq ?? "-"}</span></div>
      <div class="line"><span>传输错误</span><span>${tr.errors}</span></div>
      <div class="line"><span>映射失败</span><span>${tr.mapping_errors}</span></div>
    </div>`).join("");

  const rows = [...state.detail.events]
    .filter(e => !["kickoff", "halftime", "fulltime"].includes(e.event_type))
    .sort((a, b) => (a.minute ?? -1) - (b.minute ?? -1));
  const cell = (ev, sid) => {
    const v = ev.votes.find(x => x.source_id === sid);
    if (!v) return '<td class="slow">未报</td>';
    if (v.retracted) return '<td class="slow">已撤回</td>';
    return `<td class="${v.relative_ms === 0 ? "fast" : ""}">+${v.relative_ms} ms${v.backfilled ? ' <span class="bf">补</span>' : ""}</td>`;
  };
  $("#compare-table tbody").innerHTML = rows.map(ev => `
    <tr>
      <td>${ev.minute ?? "-"}'</td>
      <td>${esc(ETYPE_CN[ev.event_type] || ev.event_type)}</td>
      <td><span class="status-tag ${ev.status}">${statusCn(ev.status)}</span></td>
      ${cell(ev, "A")}${cell(ev, "B")}${cell(ev, "C")}
      <td class="fast">${esc(ev.votes.filter(v => !v.retracted).sort((a, b) => a.relative_ms - b.relative_ms)[0]?.source_id || "-")}</td>
    </tr>`).join("") || `<tr><td colspan="7" class="empty">暂无数据</td></tr>`;
}

function renderCorrections() {
  if (!state.detail) return;
  const rows = [...state.detail.corrections].sort((a, b) => b.ts_ms - a.ts_ms);
  $("#correction-table tbody").innerHTML = rows.map(c => `
    <tr>
      <td>${fmtTime(c.ts_ms)}</td>
      <td class="kind-${c.kind}">${kindCn(c.kind)}</td>
      <td>${esc(c.source_id)}</td>
      <td>${esc(c.event_type || "-")} <span class="pill">${esc(c.source_event_id)}</span></td>
      <td style="white-space:normal">${esc(c.detail)}</td>
    </tr>`).join("") || `<tr><td colspan="5" class="empty">暂无纠错记录</td></tr>`;
}

function kindCn(k) {
  return { RETRACT: "撤回", STATUS_CHANGE: "状态变更", SCORE_ROLLBACK: "比分回退", OFFICIAL_OVERRIDE: "官方覆盖" }[k] || k;
}

function renderReport() {
  const r = state.report;
  if (!r) return;
  const area = $("#report-area");
  area.innerHTML = `
    <div class="card" style="margin-bottom:16px">
      <h3>${esc(r.home_name)} ${r.final_authoritative_score[0]} : ${r.final_authoritative_score[1]} ${esc(r.away_name)}</h3>
      <div class="meta">阶段：${phaseCn(r.phase)} · 墙钟耗时 ${r.duration_wall_ms ? (r.duration_wall_ms / 1000).toFixed(1) + " s" : "-"}
      · 权威比分 ${r.final_authoritative_score.join(":")} / 待确认 ${r.final_pending_score.join(":")}</div>
    </div>
    <h3 class="section-title">数据源综合排名（又快又准：平均延迟 + 误报惩罚 2000ms/条）</h3>
    <div class="report-grid">
      ${r.rankings.map((x) => `
        <div class="report-card rank-${x.rank}">
          <h4>#${x.rank} ${esc(SOURCE_CN[x.source_id] || x.source_id)}</h4>
          <div class="metric"><span>综合分（越低越好）</span><span>${x.composite_score_ms} ms</span></div>
          <div class="metric"><span>准确率（有效票/总票）</span><span>${(x.accuracy * 100).toFixed(1)}%</span></div>
          <div class="metric"><span>误报后撤回</span><span>${x.retracted} 次</span></div>
          <div class="metric"><span>平均相对延迟</span><span>${x.avg_relative_ms ?? "-"} ms</span></div>
          <div class="metric"><span>最快到达次数</span><span>${x.first_count}</span></div>
          <div class="metric"><span>掉线次数</span><span>${x.disconnect_count}</span></div>
          <div class="metric"><span>断线补传</span><span>${x.backfilled} 条</span></div>
        </div>`).join("")}
    </div>
    <h3 class="section-title">逐事件明细</h3>
    <div class="table-wrap"><table>
      <thead><tr><th>分钟</th><th>事件</th><th>状态</th><th>到达顺序</th><th>最快</th></tr></thead>
      <tbody>${r.events.map(e => `
        <tr>
          <td>${e.minute ?? "-"}'</td>
          <td>${esc(ETYPE_CN[e.event_type] || e.event_type)}</td>
          <td><span class="status-tag ${e.status}">${statusCn(e.status)}</span></td>
          <td>${e.arrival_order.map(v => `${v.source_id}(+${v.relative_ms}${v.backfilled ? "补" : ""}${v.retracted ? "撤" : ""})`).join(" → ")}</td>
          <td class="fast">${esc(e.first_source)}</td>
        </tr>`).join("")}
      </tbody></table></div>
    <h3 class="section-title">纠错流水（共 ${r.corrections.length} 条）</h3>
    <div class="table-wrap"><table>
      <thead><tr><th>时间</th><th>类型</th><th>来源</th><th>说明</th></tr></thead>
      <tbody>${r.corrections.map(c => `
        <tr><td>${fmtTime(c.ts_ms)}</td><td class="kind-${c.kind}">${kindCn(c.kind)}</td>
        <td>${esc(c.source_id)}</td><td style="white-space:normal">${esc(c.detail)}</td></tr>`).join("")}
      </tbody></table></div>
    <p style="margin-top:14px"><button class="btn" id="refresh-report">刷新报告</button></p>`;
  $("#refresh-report").addEventListener("click", loadReport);
}

// ---------------- 初始化 ----------------

document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".view").forEach(v => v.classList.remove("active"));
    tab.classList.add("active");
    $(`#tab-${tab.dataset.tab}`).classList.add("active");
    if (tab.dataset.tab === "report") loadReport();
  });
});
$("#status-filter").addEventListener("change", renderTimeline);

(async function init() {
  await loadMatches();
  connectWS();
  // 轮询兜底：WS 异常或丢消息时 3s 全量对账
  state.pollTimer = setInterval(async () => {
    if (!state.wsOk) { try { await loadDetail(); } catch (_) {} }
  }, 3000);
})();
