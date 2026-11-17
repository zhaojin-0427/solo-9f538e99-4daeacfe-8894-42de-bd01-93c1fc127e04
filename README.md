# 招标公告语料采集器（Tender Corpus Collector）

面向招标语料数据员的采集与校订 API：提交多个官网公告 URL，后台按共享配额抓取，
提取 HTML 标题与正文；校对员再对混入的导航文字进行**修订、确认并发布版本化校订语料**。
全部成功后可下载原始语料，全部确认后可发布不可变的校订版本。

技术栈：FastAPI + httpx（异步）+ SQLite（持久化），Docker Compose 一键启动
API 与两个可模拟**双向跳转、限流（429/503 + Retry-After）、慢响应**的本地站点。

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8000/docs>（Swagger UI），即可**采集、校订、发布、下载**。

### 采集流程

1. `POST /jobs` 提交 1–20 个公告 URL（示例见下），返回 `job_id`；
2. `GET /jobs/{job_id}` 轮询进度（每条公告的状态/跳数/重试次数/错误）；
3. `GET /jobs/{job_id}/download` 在**全部成功**后下载完整 **原始 JSON 语料**
   （有失败或未完成时返回 409，不提供半份结果；后续校订永远不改变此原始语料）；
4. `GET /cooldowns` 查看当前生效的源冷却；`GET /jobs` 列出所有作业。

### 校订发布流程（仅对采集成功的公告）

1. `POST /jobs/{job_id}/announcements/{ann_id}/revisions` 提交修订
   （请求体可只带 `title` 或只带 `content`，未提供的字段保留当前工作文本；
   不允许空白文本）。修订写入"工作文本"，**原始提取文本原样保留**；
2. `POST /jobs/{job_id}/announcements/{ann_id}/confirm` 确认当前工作文本；
   **修改已确认文本后该条自动回到未确认状态，必须重新确认**；
3. `GET /jobs/{job_id}` 的 `confirmed` / `all_confirmed` 字段可查看确认进度；
4. 当**作业全部成功且所有公告均确认**时，`POST /jobs/{job_id}/editions`
   发布一版校订语料，返回自增 `version`；条件不满足返回 409；
5. `GET /jobs/{job_id}/editions` 列出版本；
   `GET /jobs/{job_id}/editions/{version}/download` 按版号下载冻结快照。

发布采用单事务原子写入：**发布失败不会留下任何可下载的半版**；已发布版本是
确认时刻的完整内容快照，**之后的修订不影响旧版，旧版始终按版号原样下载**。

两个模拟站点：宿主机上 `http://localhost:8001`（site_a）、`http://localhost:8002`
（site_b）可浏览；**提交给 API 的 URL 请使用容器网络名**
`http://site_a:8000/...`、`http://site_b:8000/...`（API 容器内解析）。

### 可试的 URL（在 /docs 的 POST /jobs 里提交）

| URL | 行为 |
|---|---|
| `http://site_a:8000/article/1` | 普通公告（提取标题+正文） |
| `http://site_a:8000/to-peer/2` | 302 跳到 site_b 的公告（跨源 1 跳） |
| `http://site_a:8000/chain/3` | 双向跳转链，恰好 3 跳成功 |
| `http://site_a:8000/chain/4` | 第 4 跳超出上限 → 失败 |
| `http://site_a:8000/flaky/1?fails=2&ra=3` | 前 2 次 429 + `Retry-After: 3`，第 3 次成功 |
| `http://site_b:8000/busy` | 始终 503 + `Retry-After: 1` → 重试 2 次后失败 |
| `http://site_a:8000/slow/1?delay=3` | 慢响应（3 秒） |
| `http://site_a:8000/redirect-loop` | 双向死循环跳转 → 跳数上限失败 |

## 需求实现对照

| 要求 | 实现 |
|---|---|
| 提交至多 20 个 URL | `POST /jobs`，pydantic 校验 1–20 条、http/https、去重 |
| 提取标题和正文 | `api/app/extract.py`：stdlib `html.parser` 提取 `<title>` 与 `<body>` 文本（剔除 script/style） |
| 全局最多 2 个在途请求 | 全局 `asyncio.Semaphore(2)`，所有作业共享一个引擎 |
| 同源最多 1 个 | 每个源（scheme://host:port）一条串行"pump"协程 |
| 跳转按目标源排队 | 不自动跟随重定向；302 目标作为新任务投入**目标源**的队列 |
| 每条公告最多 3 跳 | `hops >= 3` 再遇 3xx 即失败（`too many redirects`） |
| 429/503 + 整数 Retry-After → 同源所有作业未发请求延后 | 设置源级冷却（内存 + SQLite），该源 pump 发送前等待 |
| 等待不占全局名额 | 冷却 `sleep` 发生在获取全局信号量**之前** |
| 其他源仍能采集 | 冷却是 per-origin 的，其他源的 pump 不受影响 |
| 每个 URL 最多重试 2 次 | `attempts` 计数（429/503/网络错误），跳转后的新 URL 重新计数 |
| 可查进度 | `GET /jobs/{id}`（逐条状态）、`GET /jobs`、`GET /cooldowns` |
| 重启保留冷却、继续未完成、成功不重取 | SQLite 持久化作业/公告/冷却；启动时恢复未完成的（pending/fetching），成功的不重取 |
| 全部成功才开放原始下载 | `GET /jobs/{id}/download`：非全成功返回 409；只输出原始提取文本，校订不影响它 |
| /docs 可采集、查询、下载 | Swagger UI 暴露全部端点；下载带 `Content-Disposition: attachment` |
| 仅成功公告可校订 | `revisions`/`confirm` 端点对非 success 公告返回 409 |
| 修订保留原始文本 | 原始 `title/content` 不可变；修订单独存于 `revised_title/revised_content`（工作文本） |
| 修改已确认文本须重新确认 | 任何 `revisions` 调用都会清空 `confirmed` 与确认快照，直到再次 `confirm` |
| 全部成功 + 全部确认才能发布 | `POST /jobs/{id}/editions` 在单事务内校验两条前置条件，否则 409 |
| 一版取自同一时刻完整确认内容 | 发布时把每条公告的 `confirmed_*` 快照复制进 `edition_documents` |
| 发布失败不留半版 | 版号、版本行、全部文档行在同一 `BEGIN IMMEDIATE` 事务内写入，异常整体回滚 |
| 旧版按版号下载且不受后续修订影响 | 版本不可变（独立快照表）；`GET .../editions/{version}/download` 按版号取冻结内容 |
| 原始语料下载保持可用 | 原始下载端点行为不变，新增 `kind: "raw"` 标识 |
| 重启保留校订状态与版本 | 修订/确认/版本均持久化在 SQLite（`api_data` 卷），旧库自动迁移新增列 |

## 目录结构

```
api/            采集 + 校订发布 API（FastAPI）
  app/main.py   端点与生命周期：作业/进度/原始下载 + 修订/确认/版本发布/版本下载
  app/engine.py 抓取引擎：全局/同源配额、冷却、跳转、重试
  app/db.py     SQLite 持久化（jobs / announcements / cooldowns / editions / edition_documents）
  app/extract.py HTML 标题正文提取
sites/          模拟招标站点（同一镜像以环境变量区分 site_a/site_b）
tests/          端到端自测脚本（本地进程模拟 compose 拓扑）
docker-compose.yml
```

## 自测

```bash
python3 tests/e2e_test.py                 # 全量：提取/跳转/限流/配额/下载门控/重启持久化
python3 tests/cross_job_cooldown_test.py  # 专项：跨作业共享冷却
python3 tests/proofread_publish_test.py   # 校订发布全流程：门控/重新确认/版本快照/不可变/重启
python3 tests/proofread_db_test.py        # 专项：旧库迁移、发布事务回滚、并发发布序列化、HTTP 并发
```

## 已知限制

- "源"按 `scheme://host:port` 判定；同一服务器的不同端口/主机名视为不同源。
- 失败（重试耗尽）是终态，重启不会自动重试失败公告；需重新提交作业。
- 提取器面向常规 HTML 公告页，不执行 JavaScript。
- 校订为单人"最后写入生效"模型：同一公告的并发修订按字段覆盖，不保留修订历史明细
  （版本化的已发布语料 `editions` 本身不可变，可回溯每次发布时的确认内容）。
- 校订状态针对成功抓取的当前文本；若公告被重新抓取成功，其修订/确认状态会重置
  （已发布版本不受影响）。
