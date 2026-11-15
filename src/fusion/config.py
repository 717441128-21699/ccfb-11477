"""三家模拟数据商的接入配置与名称/类型 -> 统一编码映射。

真实场景里这些映射由数据字典维护；这里全部本地写死，模拟各家命名差异：
  alpha  —— 英文命名，官方源（比分以它为准），走 WebSocket
  beta   —— 拼音/缩写命名，走 SSE（服务端推送，长连接的一种）
  gamma  —— 中文命名，走长轮询（long polling）
"""
from __future__ import annotations

from dataclasses import dataclass, field

# 官方源标识：比分以此家为准
OFFICIAL_PROVIDER = "alpha"

# 多数确认阈值：同一事件达到这么多家 *有效（未撤回）* 报道即确认
MAJORITY_THRESHOLD = 2


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    transport: str          # websocket / sse / long_poll
    endpoint: str           # 模拟器暴露的 URL 路径
    is_official: bool
    # 该源事件类型名 -> 统一编码
    event_map: dict[str, str] = field(default_factory=dict)
    # 该源球队名 -> 统一编码
    team_map: dict[str, str] = field(default_factory=dict)
    # 该源球员名 -> 统一编码
    player_map: dict[str, str] = field(default_factory=dict)


PROVIDERS: dict[str, ProviderConfig] = {
    "alpha": ProviderConfig(
        name="alpha",
        transport="websocket",
        endpoint="/feed/alpha/ws",
        is_official=True,
        event_map={
            "GOAL": "goal",
            "OWN_GOAL": "own_goal",
            "PENALTY_GOAL": "penalty_goal",
            "PENALTY_MISS": "penalty_missed",
            "YELLOW": "yellow_card",
            "RED": "red_card",
            "SUB": "substitution",
            "VAR_CHECK": "var",
            "KICK_OFF": "kickoff",
            "HALF_TIME": "halftime",
            "FULL_TIME": "fulltime",
        },
        team_map={
            "Red Lions": "RED_LIONS",
            "Blue Sharks": "BLUE_SHARKS",
        },
        player_map={
            "A. Foster": "P_FOSTER",
            "M. Diaz": "P_DIAZ",
            "L. Chen": "P_CHEN",
            "K. Okafor": "P_OKAFOR",
            "T. Rossi": "P_ROSSI",
            "J. Mwangi": "P_MWANGI",
            "S. Park": "P_PARK",
            "E. Novak": "P_NOVAK",
            "R. Silva": "P_SILVA",
            "D. Koch": "P_KOCH",
        },
    ),
    "beta": ProviderConfig(
        name="beta",
        transport="sse",
        endpoint="/feed/beta/sse",
        is_official=False,
        event_map={
            "goal_normal": "goal",
            "own_goal": "own_goal",
            "pen_scored": "penalty_goal",
            "pen_missed": "penalty_missed",
            "yellowcard": "yellow_card",
            "redcard": "red_card",
            "change_player": "substitution",
            "var": "var",
            "match_start": "kickoff",
            "half_end": "halftime",
            "match_end": "fulltime",
        },
        team_map={
            "RED": "RED_LIONS",
            "BSH": "BLUE_SHARKS",
        },
        player_map={
            "FOSTER": "P_FOSTER",
            "DIAZ": "P_DIAZ",
            "CHEN": "P_CHEN",
            "OKAFOR": "P_OKAFOR",
            "ROSSI": "P_ROSSI",
            "MWANGI": "P_MWANGI",
            "PARK": "P_PARK",
            "NOVAK": "P_NOVAK",
            "SILVA": "P_SILVA",
            "KOCH": "P_KOCH",
        },
    ),
    "gamma": ProviderConfig(
        name="gamma",
        transport="long_poll",
        endpoint="/feed/gamma/poll",
        is_official=False,
        event_map={
            "进球": "goal",
            "乌龙": "own_goal",
            "点球进": "penalty_goal",
            "点球失": "penalty_missed",
            "黄牌": "yellow_card",
            "红牌": "red_card",
            "换人": "substitution",
            "VAR复核": "var",
            "开球": "kickoff",
            "半场": "halftime",
            "终场": "fulltime",
        },
        team_map={
            "红狮": "RED_LIONS",
            "蓝鲨": "BLUE_SHARKS",
        },
        player_map={
            "福斯特": "P_FOSTER",
            "迪亚斯": "P_DIAZ",
            "陈": "P_CHEN",
            "奥卡福": "P_OKAFOR",
            "罗西": "P_ROSSI",
            "姆万吉": "P_MWANGI",
            "朴": "P_PARK",
            "诺瓦克": "P_NOVAK",
            "席尔瓦": "P_SILVA",
            "科赫": "P_KOCH",
        },
    ),
}

# 统一编码 -> 展示名（网页用）
TEAM_DISPLAY = {
    "RED_LIONS": "红狮 Red Lions",
    "BLUE_SHARKS": "蓝鲨 Blue Sharks",
}
PLAYER_DISPLAY = {
    "P_FOSTER": "福斯特 (A. Foster)",
    "P_DIAZ": "迪亚斯 (M. Diaz)",
    "P_CHEN": "陈 (L. Chen)",
    "P_OKAFOR": "奥卡福 (K. Okafor)",
    "P_ROSSI": "罗西 (T. Rossi)",
    "P_MWANGI": "姆万吉 (J. Mwangi)",
    "P_PARK": "朴 (S. Park)",
    "P_NOVAK": "诺瓦克 (E. Novak)",
    "P_SILVA": "席尔瓦 (R. Silva)",
    "P_KOCH": "科赫 (D. Koch)",
}
EVENT_DISPLAY = {
    "goal": "进球",
    "own_goal": "乌龙球",
    "penalty_goal": "点球进球",
    "penalty_missed": "点球未进",
    "red_card": "红牌",
    "yellow_card": "黄牌",
    "substitution": "换人",
    "var": "VAR 复核",
    "kickoff": "开球",
    "halftime": "半场",
    "fulltime": "终场",
    "unknown": "未知类型",
}

# 会改变比分的事件类型
SCORE_EVENTS = {"goal", "own_goal", "penalty_goal"}

HTTP_HOST = "127.0.0.1"
HTTP_PORT = 8080

# 采集器重连参数
RECONNECT_INITIAL_DELAY = 0.5     # 首次重连等待秒
RECONNECT_MAX_DELAY = 8.0         # 退避上限
POLL_INTERVAL = 0.8               # 长轮询客户端空闲轮询间隔
