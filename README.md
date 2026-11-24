# 订单表批次号在线迁移演示（Flask + PostgreSQL）

订单系统发版场景：给 `orders`（订单表）和 `order_items`（明细表）
**一起**加上可空的批次号列 `batch_no VARCHAR(64)`。页面支持值班员
制造锁冲突、提交/撤销迁移、验证等锁期间普通读写不受影响，以及
失败回滚后两表结构照旧。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

- `db`：postgres:16-alpine，带健康检查；
- `web`：Flask 应用，容器入口先等数据库就绪、建库建表并写入样例
  订单（2 笔订单、3 行明细），再启动服务。

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
| POST | `/api/reset` | 重建两表与样例数据（迁移进行中或有演示事务时拒绝） |

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

## 目录结构

```
app/
  __init__.py        Flask 路由与演示事务管理
  __main__.py        python -m app 入口
  db.py              连接、建表、样例数据、订单读写
  migration.py       迁移状态机与应用层等锁/重试/撤销
  templates/index.html  单页界面
tests/e2e_test.py    端到端测试
Dockerfile / docker-compose.yml / docker-entrypoint.sh
```

## 已知边界

- 这是演示环境：迁移状态保存在进程内存，重启后通过检查列是否存在
  恢复为"已完成/空闲"；生产场景应使用持久化的迁移记录表。
- 探测间隔 0.5s、兜底 `lock_timeout` 250ms，是为演示把影响压到
  亚秒级；高并发生产环境建议再评估参数。
- `ADD COLUMN ... 可空无默认值` 在 PG 11+ 本身是快速元数据操作，
  本演示重点是锁等待行为与原子性，不涉及大表重写。
