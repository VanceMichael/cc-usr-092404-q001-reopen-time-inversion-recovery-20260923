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
| `POST` | `/api/v1/corrections` | 提交历史材料更正提案（幂等键 `request_id`） |
| `GET` | `/api/v1/corrections` | 列出更正提案（可按 `status`、`airport` 过滤） |
| `GET` | `/api/v1/corrections/{request_id}` | 查询单个更正提案与影响差异 |
| `POST` | `/api/v1/corrections/{request_id}/decision` | 复核人批准/驳回更正 |
| `GET` | `/api/v1/review-queue` | 待复核材料：被拒绝事件与待决更正 |
| `GET` | `/api/v1/projection-log` | 只追加的裁定版本日志（重算范围可追溯） |
| `GET` | `/healthz` | 检查服务和数据库健康状态 |

受影响航班查询支持 `airport`、`status`、`limit` 和 `offset` 参数。`status` 可取 `cancelled`、`delayed` 或 `pending_confirmation`。

每个读取响应都带全局 `projection_version`；事件状态、机场汇总和受影响航班列表在同一只读快照上、基于同一裁定版本重放，三者结果必然一致。

## 事件规则

- 所有输入时间必须携带时区，比较前统一转换为 UTC。
- `airport.closed` 创建事件链；未知结束时间可将 `effective_until` 设为 `null`。
- `airport.extended` 通过 `supersedes_event_id` 延长尚未结束的事件链。
- `airport.reopened` 结束事件链，机场在自身恢复缓冲时间结束后重新运行。
- 每个机场在任意时刻只有一条当前事件链，`supersedes_event_id` 必须引用当前**链头**；引用链上的旧事件会被拒绝（防止链路分叉）。链已恢复开放后只能提交新的关闭事件。
- 三种时间顺序都不能制造“结束早于开始”的窗口：
  - **开放时刻**：恢复事件的开放时间（含恢复缓冲后的窗口末端）不得早于关闭链的开始时刻，否则以 `reopen_before_chain_start` 拒绝；
  - **来源报告时刻**：沿同一事件链，后一事件的 `reported_at` 不得早于前一事件；
  - **接收顺序**：所有提交在 `BEGIN IMMEDIATE` 事务下串行裁定，事件版本必须沿链头严格递增。并发的延长/恢复/关闭中只有一个会成为唯一链头，其余被记录为拒绝。
- 与当前裁定矛盾、但材料本身完整的提交会被持久化为 `rejected`（可在复核队列与投影日志中查到），而**不会**改动当前状态；结构性错误（未知引用、机场不符等）仍然整体回滚、不落库。
- 时间窗口采用左闭右开语义，恰好落在恢复时刻的航班不受影响。恢复开放后，最终窗口内已经发生的影响仍然成立，只有被释放的航班写入 `resolved` 墓碑。
- 同一航班分别按出发机场的计划起飞时刻和到达机场的计划到达时刻判断。
- 无法改时或所需延误超过上限的航班标记为 `cancelled`；可在上限内改时的航班标记为 `delayed`；结束时间未知时标记为 `pending_confirmation`。
- 跨午夜依据受影响机场的本地时区判定。
- `event_id` 是幂等键。相同内容重试返回原结果（包括此前的拒绝决定）；相同标识携带不同内容时返回 `409 event_conflict`。
- 事件链版本必须递增，且不能继续延长已经恢复开放的事件链。

## 历史材料更正

运行控制员事后更正历史材料时使用更正提案（契约见
`contracts/correction-proposal.schema.json`），而不是用新版本事件覆盖当前状态：

- 提案针对一条已接受事件给出 `patch`（`effective_from`、`effective_until`，
  仅独立根关闭可改 `airport_code`），并声明所基于的 `base_projection_version`。
- 服务在事务内对整条链做假设重放，计算影响差异（`added`/`removed`/`changed`）和
  `replay_scope`（需要重算的事件后缀）。
- **不会改变已发布结果**的更正标记 `changes_published_result=false`；会改变结果的
  更正进入 `pending` 待复核，当前状态与已发布快照保持不变。
- 具备 `operations_reviewer` 或 `safety_reviewer` 角色的复核人（见
  `fixtures/reviewers.json`）可批准或驳回。`audit_reader` 只读，无权裁定。
- 批准后写入**新一代不可变影响快照**（`generation` 递增），原事件与原始影响快照
  永久保留；机场投影推进到新版本。若提交后机场状态已变化（基线版本过期），批准返回
  `409 stale_projection`，需要在当前投影上重新提交。
- 所有接受、拒绝、提案、批准/驳回都只追加写入 `projection_log`，服务重启后仍能看到
  被拒绝或待复核的材料、采用的决定以及每次重算范围。

## 持久化与重启

- 所有写入（事件、影响快照、投影状态、提案与决定）都在单个 `BEGIN IMMEDIATE`
  事务中原子提交；失败事务回滚，不会留下部分墓碑或错误汇总。
- `airport_state` 是每个机场唯一的裁定投影；`projection_log` 给出全局单调版本号。
- 重启时若投影行缺失会从已接受事件确定性重建；旧版本数据库会自动迁移（补充新列、
  重建含 `generation` 维度的影响唯一键），历史数据不丢失。

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
