# 订单表批次号在线迁移演示（Flask + PostgreSQL）

订单系统发版场景：给 `orders`（订单表）和 `order_items`（明细表）
**一起**加上可空的批次号列 `batch_no VARCHAR(64)`。页面支持值班员
制造锁冲突、提交/撤销迁移、验证等锁期间普通读写不受影响，以及
失败回滚后两表结构照旧。

迁移页还提供 **⑦ 订单库备份演练** 入口：值班员一键发起后，系统从
PostgreSQL 用 `pg_dump` 备份这两张表，**实际还原到一个临时库**，再按
备份所用的**同一个已提交快照**核对列定义和每一行数据；还原与核对都
通过后才把新备份原子换成下载文件，任一步失败都在页面显示原因并保留
上一次通过的备份，演练结束自动 DROP 临时库。**加列前后均可演练**，
备份/核对期间普通写单照常提交。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：postgres:16-alpine，带健康检查；
- `web`：Flask 应用，镜像内通过 PGDG 安装与服务端同主版本的
  `pg_dump` / `psql`（postgresql-client-16）；容器入口先等数据库就绪、
  建库建表并写入样例订单（2 笔订单、3 行明细），再启动服务；
- `./backups`：挂载到容器 `/srv/app/backups`，通过演练核对的
  `orders_backup_latest.sql` 会落到这里，也可直接在网页下载。

停止并清空数据：

```bash
docker compose down -v
```

## 页面操作路径

1. **① 演示读事务**：选择 `orders` 或 `order_items`，打开一个不提交、
   不回滚的读事务（持有表级 `AccessShareLock`），需要时手动结束。
2. **② 加列迁移申请**：提交后状态进入"等待锁中"；可勾选故障注入；
   等待中可撤销（前端有二次确认，撤销后即使释锁也不会执行）。
3. **③ 列生效情况**：实时显示两表 `batch_no` 是否存在。
4. **④ 试写订单/明细**：等锁期间也可写入和查询，页面显示耗时；
   迁移完成后可同时写入订单与明细的批次号。
5. **⑤ 锁队列实况**：直接查 `pg_locks`。等锁期间只应看到演示事务
   **已授予**的锁，不应出现 `granted=false` 的 ALTER 排队记录。
6. **⑥ 样例数据**与事件日志：页面每秒自动刷新。
7. **⑦ 订单库备份演练**：点“开始备份演练”，步骤条依次展示
   取快照 → `pg_dump` → 建临时库 → 还原 → 列定义核对 → 逐行核对 →
   换下载 → 清理临时库；通过后出现下载按钮和备份元数据。

## 备份演练：怎么做到“同一快照核对、写单不阻塞、失败保留旧备份”

一次演练（见 `app/backup.py`）：

1. 源库开 `REPEATABLE READ READ ONLY` 事务，`SELECT pg_export_snapshot()`
   导出已提交快照 ID；
2. **在同一事务**里读取两表列定义（名称/类型/长度/精度/可空/默认值/
   排序规则）、主键和全部行，作为核对基准；
3. `pg_dump --snapshot=<ID> --table=public.orders
   --table=public.order_items` 让备份进程导入**同一个快照**，所以备份
   文件与核对基准严格对应同一个已提交快照；
4. `CREATE DATABASE orders_drill_<pid>_<时间>_<随机>` 建临时库，
   `psql -v ON_ERROR_STOP=1` 把备份**真实还原**进去（SERIAL 序列、
   外键等都由 pg_dump 一并带出）；
5. 连临时库：先用 `information_schema.columns` 逐列核对定义，再按主键
   对齐、逐列比较**每一行**，不一致会给出“表 + 主键 + 列 + 期望值/
   实际值”；
6. 全部通过才 `os.replace()` 把暂存 SQL **原子替换**成
   `backups/orders_backup_latest.sql`，并写同名 `.meta.json`；
7. 无论成败都 `pg_terminate_backend` + `DROP DATABASE` 清理临时库
   （启动时还会兜底清理遗留的 `orders_drill_*` 库）。

**为什么备份/核对期间普通写单仍可提交**：快照事务和 pg_dump 只持有两表
的 `AccessShareLock`，而 INSERT 需要的 `RowExclusiveLock` 与之兼容，
不进锁等待队列。快照之后提交的写单不在本次备份内（这正是快照一致性的
含义），再演练一次即会纳入。为避免 `ALTER TABLE` 的排他锁与备份相互
影响，演练与加列迁移/重置在 API 层互斥（进行中返回 409）。

**失败保留上次通过的备份**：新备份先写在 `backups/staging/`，只有还原
和核对都通过才 replace 上线；任何一步抛错都只在页面/状态里显示原因、
删除暂存文件，绝不触碰当前的 `orders_backup_latest.sql`。页面下拉框
还提供三种故障注入，方便演示失败路径：

- `restore_failure`：备份末尾追加错误 SQL，psql 还原失败；
- `schema_mismatch`：还原后把临时库 `orders.customer` 改成 VARCHAR(200)，
  列定义核对失败；
- `row_mismatch`：还原后篡改临时库一行数据，逐行核对失败。

### 网页演练期间，另行试写一单的操作步骤

**方式 A（纯网页）**：在 ⑦ 勾选“放慢节奏（每阶段约 1 秒）”→ 点
“开始备份演练” → 立刻到 ④ 连续点“写入订单+明细并计时”，每次都应
毫秒级返回 `201`；演练最终仍“演练通过”。

**方式 B（另开终端用 curl 连续试写，即页面 ⑦ 里贴的命令）**：

```bash
# 终端 1：发起一次放慢的演练
curl -s -X POST http://localhost:8000/api/backup-drill/start \
  -H 'Content-Type: application/json' -d '{"pace":1.2}'

# 终端 2：演练进行中连续写 4 单并打印 HTTP 状态与耗时
for i in 1 2 3 4; do
  curl -s -o /dev/null -w "write $i -> HTTP %{http_code}, %{time_total}s\n" \
    -X POST http://localhost:8000/api/orders \
    -H 'Content-Type: application/json' \
    -d '{"customer":"演练期间写单","items":[{"sku":"SKU-DRILL","quantity":1,"unit_price":"8.88"}]}'
  sleep 1
done
```

预期每一单都是 `HTTP 201`、耗时几十毫秒；随后查询
`GET /api/backup-drill/state` 可见 `state=succeeded`、
`cleanup_ok=true`（Docker 正常环境），网页出现下载按钮。再发起一次
演练，新写的单会出现在新备份里并通过逐行核对。

也可以直接下载最近一次通过核对的备份：

```bash
curl -OJ http://localhost:8000/api/backup/download
# 或直接取宿主机文件：./backups/orders_backup_latest.sql
```

## 核心设计：为什么等锁期间普通读写不被堵住

PostgreSQL 中 `ALTER TABLE ... ADD COLUMN` 需要 `ACCESS EXCLUSIVE`
锁。若直接发起而表上有未结束事务，ALTER 会进入**服务器端锁等待队列**，
后续到达该表的普通读写会排在 ALTER 后面一起被阻塞。

本项目的迁移线程因此**不进入服务器端锁队列**（见 `app/migration.py`）：

1. 每 0.5s 用独立连接查 `pg_locks`，判断两表上是否存在与
   `ACCESS EXCLUSIVE` 冲突的已授予表级锁（含读事务的
   `AccessShareLock`，共 7 种模式）；
2. 无冲突时才在**同一个数据库事务**里执行两条 `ALTER TABLE`，
   并设置 `lock_timeout=250ms` 兜底；
3. 兜底超时（SQLSTATE `55P03`）则回滚本轮、重新进入等待循环；
4. 两张表的 ALTER 同事务提交：任一失败整体回滚，保证两表结构一致；
5. 撤销通过 `threading.Event` 置位，等待循环与执行前双重检查，
   确认撤销后永不执行。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/state` | 迁移状态、两列是否生效、演示事务、pg_locks 实况 |
| POST | `/api/demo-tx/open` | 在任一表打开保持不结束的读事务 |
| POST | `/api/demo-tx/close` | 手动结束演示事务（释放锁） |
| POST | `/api/migration/submit` | 提交加列申请（`inject_failure` 可故障注入） |
| POST | `/api/migration/cancel` | 撤销等待中的申请 |
| GET/POST | `/api/orders` | 查询订单明细 / 试写订单+明细 |
| GET  | `/api/columns` | 两表 batch_no 列是否存在 |
| POST | `/api/backup-drill/start` | 发起备份演练（`pace` 放慢、`inject` 故障注入），202 |
| GET  | `/api/backup-drill/state` | 演练状态、阶段、快照、临时库、核对结果、上次通过的备份 |
| GET  | `/api/backup/download` | 下载上次通过核对后上线的备份 `.sql`（无则 404） |
| POST | `/api/reset` | 重建两表与样例数据（迁移或演练进行中、或有演示事务时拒绝） |

## 测试

```bash
# 进程内模式（Flask test client，走完整 WSGI 栈）
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
PGDATABASE=orders_demo python tests/e2e_test.py

# 或对已运行的服务发真实 HTTP
BASE_URL=http://localhost:8000 python tests/e2e_test.py
```

覆盖 8 个场景、27 项断言：初始无列与样例数据、等锁状态、等锁期间
普通读写毫秒级完成且服务器端无排队 DDL、撤销后释锁也不执行、
释锁自动续跑两列同生效、批次号可写可读、故障注入后两表结构照旧、
重置恢复。

备份演练另有 `tests/backup_drill_test.py`（47 项断言，同样支持
进程内 / `BASE_URL` 两种模式）：

```bash
# 对 compose 起的真实服务（容器内已装好 pg_dump/psql）
BASE_URL=http://localhost:8000 python tests/backup_drill_test.py

# 或进程内（宿主机需有与 PG 同主版本的 pg_dump/psql）
PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
PGDATABASE=orders_demo \
PG_DUMP_BIN=/usr/lib/postgresql/16/bin/pg_dump \
PSQL_BIN=/usr/lib/postgresql/16/bin/psql \
python tests/backup_drill_test.py
```

覆盖：加列前 5 列演练通过且行数与 `/api/orders` 一致、备份可下载且
不含 `batch_no`；放慢演练并在 `pg_dump`/列核对/逐行核对阶段各写一单，
全部 201 且几十毫秒返回、提交后可查；再演练一次覆盖新写数据；演练中
重复发起/提交迁移/重置均 409；加列后两表各 6 列、备份含 `batch_no`
与批次值；三种故障注入均失败、给出原因且下载字节保持为上次通过的
备份；临时库清理状态与（进程内模式下）`pg_database` 物理消失。

## 目录结构

```
app/
  __init__.py        Flask 路由、演示事务管理、操作互斥
  __main__.py        python -m app 入口
  db.py              连接、建表、样例数据、订单读写
  migration.py       迁移状态机与应用层等锁/重试/撤销
  backup.py          备份演练：同快照 pg_dump、临时库还原、核对、原子发布、清理
  templates/index.html  单页界面（① ~ ⑦）
tests/
  e2e_test.py             迁移/锁场景端到端测试（27 项）
  backup_drill_test.py    备份演练关键场景测试（47 项）
backups/             通过核对的备份与 meta（compose 挂载到宿主机，git 忽略产物）
Dockerfile / docker-compose.yml / docker-entrypoint.sh
```

## 已知边界

- 这是演示环境：迁移状态保存在进程内存，重启后通过检查列是否存在
  恢复为"已完成/空闲"；生产场景应使用持久化的迁移记录表。
- 探测间隔 0.5s、兜底 `lock_timeout` 250ms，是为演示把影响压到
  亚秒级；高并发生产环境建议再评估参数。
- `ADD COLUMN ... 可空无默认值` 在 PG 11+ 本身是快速元数据操作，
  本演示重点是锁等待行为与原子性，不涉及大表重写。
- 备份演练是**逻辑备份级别的恢复校验**：只备份/还原 `orders`、
  `order_items` 两表（含其序列、约束），不是整实例 PITR；备份文件
  保留为单个最新的、通过核对的 `orders_backup_latest.sql`（外加
  `.meta.json`），不保留多版本历史。
- 演练临时库由应用用 `CREATE/DROP DATABASE` 在**同一个 PG 实例**内
  创建，因此 `PGUSER` 需有建库权限（compose 里是超级用户 `postgres`）。
  清理语句带 `statement_timeout`（`CLEANUP_TIMEOUT_MS`，默认 30s）与
  应用层有界等待（`CLEANUP_JOIN_TIMEOUT_S`，默认 8s）；若清理超时，
  演练仍给出通过/失败结论，清理转后台继续并在页面显示状态，重启时
  还会兜底清理遗留的 `orders_drill_*` 库。
- `pg_dump`/`psql` 主版本应与服务端一致或更新；镜像内固定安装
  PGDG 的 `postgresql-client-16`。可用 `PG_DUMP_BIN`/`PSQL_BIN`
  覆盖可执行文件路径。
