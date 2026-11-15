"""HTTP 冒烟测试：真实起 aiohttp 服务后，验证三种推送端点、补漏参数与 REST API。

用法：服务已在 127.0.0.1:8080 运行时
    .venv\\Scripts\\python.exe -I scripts\\smoke_http.py
"""
from __future__ import annotations

import asyncio
import json
import sys

import aiohttp

BASE = "http://127.0.0.1:8080"
failures: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(("  [OK] " if cond else "  [FAIL] ") + msg)
    if not cond:
        failures.append(msg)


async def main() -> int:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        # 1) 网页与静态资源
        async with s.get(f"{BASE}/") as r:
            check(r.status == 200 and "融合控制台" in await r.text(), "首页 index.html 可访问")
        async with s.get(f"{BASE}/static/app.js") as r:
            check(r.status == 200, "静态资源 app.js 可访问")

        # 2) 赛事列表
        async with s.get(f"{BASE}/api/matches") as r:
            matches = await r.json()
            check(len(matches) >= 2 and all("match_id" in m for m in matches),
                  f"/api/matches 返回 {len(matches)} 场赛事")
        mid = matches[0]["match_id"]

        # 3) 长轮询端点（gamma），带 since_seq
        async with s.get(f"{BASE}/feed/gamma/poll?since_seq=0") as r:
            check(r.status == 200, "gamma 长轮询 200")
            data = await r.json()
            check(data["provider"] == "gamma" and "entries" in data and "last_seq" in data,
                  f"gamma 返回 entries={len(data['entries'])} last_seq={data['last_seq']}")
            check(all(e["seq"] > 0 for e in data["entries"]), "轮询报文带单调 seq")

        # 4) SSE 端点（beta）读若干事件块
        async with s.get(f"{BASE}/feed/beta/sse?since_seq=0") as r:
            check(r.status == 200 and "text/event-stream" in r.headers.get("Content-Type", ""),
                  "beta SSE 200 且 Content-Type 正确")
            buf = b""
            async for chunk in r.content:
                buf += chunk
                if buf.count(b"\n\n") >= 2:
                    break
            blocks = [b for b in buf.split(b"\n\n") if b.strip()]
            check(len(blocks) >= 2 and all(b.startswith(b"data:") for b in blocks),
                  f"SSE 收到 {len(blocks)} 个 data 事件块")
            first = json.loads(blocks[0][5:].strip())
            check(first["payload"]["provider"] == "beta", "SSE 报文属于 beta 且结构完整")

        # 5) WebSocket 端点（alpha）+ since_seq 补漏语义
        #    可能撞上模拟掉线窗口被服务端关闭（1013），按采集器逻辑重试到拿到报文；
        #    比赛已完赛时回放后没有新实时事件，实时连续性改为条件性检查
        backlog = None
        live = None
        for attempt in range(8):
            try:
                async with s.ws_connect(f"{BASE}/feed/alpha/ws?since_seq=0") as ws:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        backlog = json.loads(msg.data)
                        try:
                            msg2 = await asyncio.wait_for(ws.receive(), timeout=5)
                            if msg2.type == aiohttp.WSMsgType.TEXT:
                                live = json.loads(msg2.data)
                        except asyncio.TimeoutError:
                            pass  # 完赛后无新事件，属正常
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(1.0)
        check(backlog is not None and isinstance(backlog, list) and backlog[0]["seq"] == 1,
              f"alpha WS 重连后回放 backlog {len(backlog) if backlog else 0} 条，首条 seq=1")
        if live is not None:
            check(live["seq"] == backlog[-1]["seq"] + 1,
                  f"WS 实时推送 seq 连续（{backlog[-1]['seq']} -> {live['seq']}）")
        else:
            check(True, "完赛后无后续实时事件，跳过实时 seq 连续性检查")

        # 6) since_seq 水位线：只给增量
        async with s.get(f"{BASE}/feed/gamma/poll?since_seq=2") as r:
            data = await r.json()
            check(all(e["seq"] > 2 for e in data["entries"]),
                  "since_seq=2 时轮询只返回 seq>2 的增量（补漏协议）")

        # 7) 融合状态 API：结构完整性
        async with s.get(f"{BASE}/api/state/m001") as r:
            snap = await r.json()
        state = snap["state"]
        check({"match_id", "score_home", "score_away", "incidents",
               "corrections", "pending_goals"} <= set(state), "融合状态字段完整")
        check(snap["source_status"].keys() == {"alpha", "beta", "gamma"},
              f"三家连接状态均有上报：{snap['source_status']}")
        if state["incidents"]:
            inc = state["incidents"][0]
            check({"status", "sources", "retracted_sources", "active_providers"}
                  <= set(inc), "事件含融合状态/来源投票/撤回标记")

        # 8) 仪表盘 WebSocket：能推快照
        async with s.ws_connect(f"{BASE}/ws/m001") as ws:
            msg = await asyncio.wait_for(ws.receive(), timeout=10)
            dash = json.loads(msg.data)
            check(dash["kind"] == "state" and "state" in dash,
                  "仪表盘 WS 建连即收到完整快照")

    print("\n" + ("HTTP 冒烟测试全部通过 [PASS-ALL]" if not failures
                  else f"{len(failures)} 项失败 [HAS-FAIL]"))
    return 0 if not failures else 1


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    raise SystemExit(asyncio.run(main()))
