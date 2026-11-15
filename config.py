"""全局配置：端口、时间倍率、融合规则参数。"""
import os

# 模拟器（三家假数据商）监听地址
SIM_HOST = os.environ.get("SIM_HOST", "127.0.0.1")
SIM_PORT = int(os.environ.get("SIM_PORT", "8091"))

# 融合服务网页监听地址
WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8090"))

# 剧本时间（比赛秒）-> 墙钟秒 的倍率。60 表示全场 90 分钟约 90 秒跑完。
# 网络延迟（delay_ms / jitter_ms / 撤回 after_ms / 掉线 duration_ms）始终是真实毫秒，不缩放。
TIME_SCALE = float(os.environ.get("TIME_SCALE", "60"))

# 官方数据源 id（比分与比赛状态以它为准）
OFFICIAL_SOURCE = os.environ.get("OFFICIAL_SOURCE", "A")

# 多数确认阈值：N 家活跃投票即 CONFIRMED
MAJORITY_THRESHOLD = int(os.environ.get("MAJORITY_THRESHOLD", "2"))

# 同一事件匹配的分钟容差（各家报分钟可能差 1）
MINUTE_WINDOW = int(os.environ.get("MINUTE_WINDOW", "1"))

# 长轮询单次挂起时长（毫秒）
SIM_POLL_WAIT_MS = int(os.environ.get("SIM_POLL_WAIT_MS", "5000"))

# 重连退避（秒），指数退避上限
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "0.5"))
BACKOFF_CAP = float(os.environ.get("BACKOFF_CAP", "4.0"))

SCENARIO_PATH = os.environ.get("SCENARIO", "scenarios/sample_match.json")
