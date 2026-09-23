# 机场中断影响服务

本项目提供纯后端机场中断影响服务。服务接收机场关闭、延长关闭和恢复开放事件，计算受影响的既有航班与旅客，将事件链和计算结果保存到 SQLite，并通过 HTTP 接口提供查询。

示例中的机场、航班时刻和旅客数量均为合成数据。运行期间不会请求外部航班、地图或通知服务。

## 目录

- `contracts/disruption-event.schema.json`：中断事件输入契约。
- `fixtures/airports.json`：机场时区与恢复缓冲时间。
- `fixtures/flights.json`：确定性的航班计划数据，包含跨午夜样例。
- `app/`：Python 3.12 标准库实现的业务服务。
- `tests/`：计算、校验、存储和 HTTP 集成测试。
- `scripts/selftest_client.py`：HTTP 冒烟请求示例。

## 启动

```bash
DB_PATH=./data/disruptions.db PORT=8080 python3 -m app
```

SQLite 默认位于 `./data/disruptions.db`。`DB_PATH`、`HOST`、`PORT` 和 `FIXTURES_DIR` 均可通过环境变量调整。

本地运行只需要 Python 3.12：

```bash
python3 -m unittest discover -s tests
DB_PATH=./data/disruptions.db PORT=8080 python3 -m app
```

## 接口

所有请求和响应均为 JSON，错误统一使用以下结构：

```json
{"error": {"code": "unknown_airport", "message": "...", "details": {}}}
```

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/events` | 提交关闭、延长或恢复事件 |
| `GET` | `/api/v1/events/{event_id}` | 查询事件、处理状态和影响结果 |
| `GET` | `/api/v1/airports/{AIRPORT}/summary` | 查询机场影响汇总 |
| `GET` | `/api/v1/flights/affected` | 分页查询当前受影响航班 |
| `GET` | `/healthz` | 检查服务和数据库健康状态 |

受影响航班查询支持 `airport`、`status`、`limit` 和 `offset` 参数。`status` 可取 `cancelled`、`delayed` 或 `pending_confirmation`。

## 事件规则

- 所有输入时间必须携带时区，比较前统一转换为 UTC。
- `airport.closed` 创建事件链；未知结束时间可将 `effective_until` 设为 `null`。
- `airport.extended` 通过 `supersedes_event_id` 延长尚未结束的事件链。
- `airport.reopened` 结束事件链，机场在自身恢复缓冲时间结束后重新运行。
- 时间窗口采用左闭右开语义，恰好落在恢复时刻的航班不受影响。
- 同一航班分别按出发机场的计划起飞时刻和到达机场的计划到达时刻判断。
- 无法改时或所需延误超过上限的航班标记为 `cancelled`；可在上限内改时的航班标记为 `delayed`；结束时间未知时标记为 `pending_confirmation`。
- 跨午夜依据受影响机场的本地时区判定。
- `event_id` 是幂等键。相同内容重试返回原结果；相同标识携带不同内容时返回 `409 event_conflict`。
- 事件链版本必须递增，且不能继续延长已经恢复开放的事件链。

## 验证

单元与集成测试：

```bash
python3 -m unittest discover -s tests -v
```

## 开发检查

编译或构建命令：

```bash
python3 -m compileall -q app
```

如需镜像，可在单个应用容器中构建和运行：

```bash
docker build -t airport-disruption .
docker run --rm -p 8080:8080 airport-disruption
```
