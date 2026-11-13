# 水文观测明细表 · 快照小文件回收服务

小文件合并后，湖仓里会留下旧快照引用、但已经不需要的 Parquet 文件。这个
FastAPI 服务负责在**不打断正在运行的导出**的前提下回收它们。

## 安全规则

清单（`manifest.json`）中的文件，只有同时满足下面两个条件才会被删除：

1. 没有任何**未退役（保留）**的快照引用它；
2. 没有任何**未完成**的导出登记引用它。

多个快照可以共用同一个文件。清理按集合计算引用：只要还有一个保留快照或
未完成导出引用该文件，它就会被保护。

### 退役与登记的并发语义

- 退役后不再接受该快照的**新**登记（返回 `409`），但已成功登记的导出继续
  保护其文件，直到导出调用 `release`。
- 登记和退役各自在 SQLite `BEGIN IMMEDIATE` 写事务中完成：
  - 登记先提交：退役可以继续，但这条已成功的登记会一直保护其文件；
  - 退役先提交：登记看到退役状态并被拒绝。
- 清理**不在删除期间持有写锁**：它先用一次短读事务（`BEGIN`，WAL 快照隔离）
  取出「退役状态 + 未完成登记」的一致视图并立即提交，然后在事务之外执行
  `unlink`。

  因此即使磁盘删除很慢、卡住甚至失败，另一个保留快照的 `POST /exports/register`
  也能立刻拿到登记回执并开始取数，不会被清理拖延，也不会因 SQLite 的
  `busy_timeout` 超时而报错。

  删除放在写事务之外仍然安全，因为**退役不可撤销**：一个文件只有在引用它的
  **所有**快照都已退役后才可删；已退役快照不再接受新登记，所以不会有新登记
  在清理决策之后又开始引用该文件。与清理并发提交的登记，要么其快照仍未退役
  （该文件出现在清理的读视图里并受保护），要么其快照已退役（登记被拒绝）。

### 失败与重启

- 退役状态和导出登记都保存在 SQLite 中。
- 删除文件失败只记入本次响应的 `failed`/`failures`，**不会撤销退役**，
  其它文件继续处理；修复磁盘问题后再次调用 `POST /cleanup` 即可按路径重试。
- 两轮清理同时碰到同一个文件时，真正删掉它的一轮把它记入 `deleted`，另一轮
  发现文件已不存在，把它记入 `already_absent`；两轮都不报 `failed`。
- 服务重启后状态仍在（Docker Compose 使用命名卷持久化）。
- 已经被删除、但清单里仍列出的文件会进入 `already_absent`，重复清理是幂等的。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

- Swagger UI：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/health>

首次启动会把镜像内的样例 Parquet 文件复制到数据卷 `/app/data`，同一卷中
还保存 `cleanup.db`。重启容器不会重新复制文件，也不会丢失退役/登记状态。

重置样例（清空数据卷）：

```bash
docker compose down -v
docker compose up --build
```

## Swagger 演示：回收旧文件的同时另一个快照仍在取数

样例清单包含三个快照：

| 快照 | 文件 |
| --- | --- |
| `snap-20261101-night` | `hydro/2026-11-01/station_a.parquet`、`.../station_b.parquet`、`hydro/shared/quality.parquet` |
| `snap-20261102-night` | `hydro/2026-11-02/station_a.parquet`、`.../station_b.parquet`、`hydro/shared/quality.parquet` |
| `snap-20261102-noon` | `hydro/2026-11-02/station_c.parquet`、`hydro/shared/quality.parquet`、`hydro/shared/station_meta.parquet` |

演示目标：**旧文件还在删的时候，另一个保留快照的导出登记也能立刻拿到回执**。
按顺序在 Swagger 中操作：

1. **退役两个旧快照**（可重复调用，退役是幂等的）：

   ```
   POST /snapshots/snap-20261102-night/retire
   POST /snapshots/snap-20261102-noon/retire
   ```

   保留 `snap-20261101-night` 作为"另一个仍在运行的快照"。此时预览
   `GET /cleanup/preview` 的 `deletable` 是 2026-11-02 的两个文件和
   `hydro/shared/station_meta.parquet`。

2. **登记保留快照的导出**：`POST /exports/register`

   ```json
   {"snapshot_id": "snap-20261101-night", "export_id": "nightly-export-001"}
   ```

   立即返回 `201` 和 `registration_id`，响应 `files` 为
   `hydro/2026-11-01/station_a.parquet`、`.../station_b.parquet`、
   `hydro/shared/quality.parquet`。这一步在真实故障场景中正是"旧文件还没删完
   时的另一次登记"——它不会等待 `POST /cleanup` 的删除完成。

3. **执行清理**：`POST /cleanup`（空 body 或 `{"dry_run": false}`）

   返回 `deleted` 为第 1 步列出的旧文件，`failed` 为空；`quality.parquet`
   因被保留快照和未完成导出共同引用而保留。

4. **确认登记仍然有效**：`GET /cleanup/preview`

   - `deletable` 为空；
   - `protected_by_retained_snapshots` 与 `protected_by_active_exports`
     都包含 2026-11-01 的两个文件和 `hydro/shared/quality.parquet`。

5. **导出完成，释放登记**：

   ```
   POST /exports/{registration_id}/release
   ```

6. **退役保留快照**：`POST /snapshots/snap-20261101-night/retire`

   退役后该快照**不再接受新登记**——再调用一次
   `POST /exports/register`（`snapshot_id` 为 `snap-20261101-night`）会返回
   `409`；但第 2 步已登记的导出（若尚未 release）继续受保护。

7. **再次预览**：`GET /cleanup/preview`，`deletable` 变为：

   ```json
   [
     "hydro/2026-11-01/station_a.parquet",
     "hydro/2026-11-01/station_b.parquet",
     "hydro/shared/quality.parquet"
   ]
   ```

8. **再次清理**：`POST /cleanup`，删除上面 3 个文件；再预览时 `deletable`
   为空。若此时文件其实已被上一轮删掉，它们会出现在 `already_absent` 而不是
   `failed`。

`POST /cleanup?dry_run=true` 等同于只预览、不删除。

`POST /cleanup?dry_run=true` 等同于只预览、不删除。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/snapshots` | 列出清单快照、退役状态、活跃导出数 |
| `GET` | `/snapshots/{snapshot_id}` | 单个快照详情 |
| `POST` | `/snapshots/{snapshot_id}/retire` | 退役快照，之后不接受新登记 |
| `POST` | `/exports/register` | 导出按 `snapshot_id` 登记，返回登记 ID |
| `POST` | `/exports/{registration_id}/release` | 导出完成后释放自己的登记（幂等） |
| `GET` | `/exports?active_only=true` | 列出登记（默认只看未完成） |
| `GET` | `/cleanup/preview` | 预览可删/受保护文件 |
| `POST` | `/cleanup` | 执行清理；body 可带 `{"dry_run": true}` |
| `GET` | `/health` | 健康检查 |

`POST /cleanup` 的响应包含：

- `deleted`：本次真正删除的路径；
- `already_absent`：清单中列出、但磁盘上已不存在的路径；
- `failed`：删除失败的路径；
- `failures`：失败路径及错误原因；
- `deletable` / `protected*` / `active_exports`：本次决策依据，便于值班员排查。

## 清单格式

服务每次请求都会读取 `MANIFEST_PATH` 指向的本地清单。标准格式：

```json
{
  "snapshots": [
    {"snapshot_id": "snap-a", "files": ["hydro/a.parquet", "hydro/shared.parquet"]},
    {"snapshot_id": "snap-b", "files": ["hydro/b.parquet", "hydro/shared.parquet"]}
  ]
}
```

也兼容 `snapshots` 使用 `id`/`name`、文件使用 `paths`/`data_files` 或对象
`{"relative_path": ...}` 等常见写法；路径必须是数据目录内的相对路径，
绝对路径和 `..` 会被拒绝。

## 本地运行（不用 Docker）

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
```

本地默认直接把仓库里的 `sample_data/` 当作数据目录，SQLite 在
`local_state/cleanup.db`。演示删除后可恢复样例文件：

```bash
git checkout -- sample_data
rm -rf local_state
```

## 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MANIFEST_PATH` | `./manifest.json` | 本地清单路径 |
| `DATA_DIR` | `./sample_data` | Parquet 文件根目录（删除只发生在这里） |
| `CLEANUP_DB_PATH` | `./local_state/cleanup.db` | SQLite 状态库 |
| `SAMPLE_DATA_DIR` | `./sample_data` | 容器内只读样例目录 |
| `SEED_SAMPLE_DATA_ON_START` | `0` | 为 `1` 时把样例复制到 `DATA_DIR`（仅一次） |

## 测试

```bash
PYTHONPATH=. python3 -m unittest tests.test_service -v   # 服务逻辑/并发/重启
pytest -q                                                # 含 HTTP API 测试
```
