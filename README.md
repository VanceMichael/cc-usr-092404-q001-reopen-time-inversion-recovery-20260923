# 机场中断影响服务

本项目提供纯后端机场中断影响服务。服务接收机场关闭、延长关闭和恢复开放事件，计算受影响的既有航班与旅客，将事件链和计算结果保存到 SQLite，并通过 HTTP 接口提供查询。

示例中的机场、航班时刻和旅客数量均为合成数据。运行期间不会请求外部航班、地图或通知服务。

## 目录

- `contracts/disruption-event.schema.json`：中断事件输入契约。
- `contracts/correction-proposal.schema.json`：历史材料权威更正提案契约。
- `fixtures/airports.json`：机场时区与恢复缓冲时间。
- `fixtures/flights.json`：确定性的航班计划数据，包含跨午夜样例。
- `fixtures/reviewers.json`：可裁定更正的审核员角色（`audit_reader` 只读）。
- `app/`：Python 3.12 标准库实现的业务服务。
- `tests/`：计算、校验、存储、裁定、更正复核与 HTTP 集成测试。
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
| `POST` | `/api/v1/corrections` | 提交历史材料权威更正提案（进入待复核） |
| `GET` | `/api/v1/corrections` | 列出更正提案，可按 `state` 过滤 |
| `GET` | `/api/v1/corrections/{request_id}` | 查询单个提案与其重算范围 |
| `POST` | `/api/v1/corrections/{request_id}/decision` | 审核员批准或驳回提案 |
| `GET` | `/api/v1/rejections` | 查询被裁定拒绝（含倒序）的原始材料 |
| `GET` | `/api/v1/journal` | 查询裁定日志（采纳/拒绝/提案/决定） |
| `GET` | `/api/v1/projection` | 查询当前裁定版本号 |
| `GET` | `/healthz` | 检查服务和数据库健康状态 |

受影响航班查询支持 `airport`、`status`、`limit`、`offset` 与 `projection_version` 参数。`status` 可取 `cancelled`、`delayed` 或 `pending_confirmation`。事件、机场汇总和受影响航班查询都接受 `projection_version`，三者在同一裁定版本上重放，结果互相一致。

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
- 事件链版本必须沿机场历史**全局严格递增**，且延长/恢复只能引用当前链头（不能引用已被覆盖的旧事件，否则会分叉）。
- 活动链存在时不能再提交同一机场的新关闭事件。

### 时间与链路裁定

恢复事件由开放时刻、来源报告时刻和接收顺序共同裁定，任何一个都不能制造“结束早于开始”的窗口：

- **开放时刻**：恢复（含机场恢复缓冲后的实际运行时刻）不得早于或等于链起点；恢复事件本身不得晚于最近一次已宣布的关闭结束时刻；延长事件不得在覆盖区间中留下空段，也不得缩短已宣布的末端。
- **来源报告时刻**：`reported_at` 必须沿链头单调不早于前序事件，晚到的“更早事实”按历史更正处理。
- **接收顺序**：所有提交在 `BEGIN IMMEDIATE` 事务中串行裁定，并占用一个全库单调的裁定版本。延长、恢复和新关闭并发提交时，只有链头的一个赢家被采纳，其余进入拒绝记录；失败事务整体回滚，不会留下部分墓碑或错误汇总。

被裁定拒绝的材料不进入事件链，但会连同原始载荷和原因写入拒绝记录（`GET /api/v1/rejections`），服务重启后仍可查询；相同内容重试返回同一拒绝决定而不重复落库。

### 历史材料更正（待复核）

当确属权威历史材料需要更正（例如值班席补录的更早关闭/恢复时刻）时，不允许直接覆盖当前状态：

1. 通过 `POST /api/v1/corrections` 提交补丁（`airport_code`、`effective_from`、`effective_until`），携带 `base_projection_version` 与提交人。
2. 服务以补丁**纯函数式重放整条链**，若会制造倒序窗口等非法状态，提案在提交时即被 `422` 拒绝。
3. 提案计算“重算范围”（受影响事件与航班的变更前后状态）。会改变已发布结果的更正进入 `pending_review`，已发布的事件载荷与影响快照保持不变。
4. 具备 `operations_reviewer` 或 `safety_reviewer` 角色的审核员可批准或驳回（`audit_reader` 只读，返回 `403`）。批准在一个新裁定版本下写入修正案与整条链尾部的重算快照，原事件与原快照仍保留可查。
5. `request_id` 是幂等键；`base_projection_version` 过期时返回 `409 projection_conflict`，需基于新版本重新提案。

### 裁定版本与重放

每次事件采纳、事件拒绝、更正提案与更正决定都在 `projection_journal` 中占用一个严格递增的 `seq`（裁定版本）。事件、影响快照、拒绝材料和更正决定都挂在某个版本下：

- 事件查询、机场汇总、受影响航班列表接受 `projection_version`，在同一版本上重放，三者结果一致。
- `GET /api/v1/journal` 列出每个版本的裁定种类、对象和重算范围。
- 所有材料写入 SQLite 单一 WAL 数据库，容器重启后被拒绝或待复核材料、采用的决定以及重算范围仍然可见。

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
