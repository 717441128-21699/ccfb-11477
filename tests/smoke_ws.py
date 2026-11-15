"""WS 推送冒烟脚本：连 5 秒，统计收到的增量消息类型。"""
import asyncio
import json

import aiohttp


async def main():
    kinds = {}
    samples = []
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("http://127.0.0.1:8090/ws") as ws:
            try:
                async with asyncio.timeout(6):
                    async for msg in ws:
                        d = msg.json()
                        kinds[d["kind"]] = kinds.get(d["kind"], 0) + 1
                        if len(samples) < 3:
                            samples.append({"kind": d["kind"], "keys": list(d.get("data", {}).keys())[:6]})
            except TimeoutError:
                pass
    print(json.dumps({"kinds": kinds, "samples": samples}, ensure_ascii=False, indent=2))


asyncio.run(main())
