"""订单库备份演练：快照备份 -> 还原临时库 -> 同一快照核对 -> 原子换下载。

完整链路
========
1. 在源库开一个 ``REPEATABLE READ READ ONLY`` 事务，用
   ``pg_export_snapshot()`` 导出**已提交快照**；
2. 仍在该事务里读取 orders / order_items 的列定义与全部行，作为核对基准；
3. 调用 ``pg_dump --snapshot=<id>`` 让备份进程导入同一个快照，因此备份
   内容与核对基准严格对应同一个已提交快照；
4. ``CREATE DATABASE orders_drill_...`` 建临时库，用 ``psql`` 把备份实际
   还原进去（ON_ERROR_STOP=1）；
5. 连临时库，逐表核对列定义（名称/类型/长度/精度/可空/默认值等），并按
   主键对齐、逐列比较**每一行**；
6. 只有还原与核对都通过，才用 ``os.replace`` 把暂存备份原子换成正式下载
   文件——上一份通过的备份在任何失败情况下都不会被覆盖；
7. 无论成败，最后都终止残留连接并 ``DROP DATABASE`` 清理临时库。

对普通写单的影响
================
快照事务与 pg_dump 只持有表的 ``AccessShareLock``，与普通 INSERT/UPDATE
所需的 ``RowExclusiveLock`` 兼容，因此备份、核对期间写单连接照常提交，
不会进入锁等待队列。注意 ALTER TABLE 与备份锁冲突，所以路由层保证演练
与加列迁移互斥；批次号列存在与否，本模块都按当时的真实列定义核对，
因此加列前后均可演练。
"""
from __future__ import annotations

import json
import os
import secrets
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from . import db

# ---------------------------------------------------------------- 常量
# 默认放到项目根（app 包的上一级）下的 backups，而不是依赖进程 CWD；
# Flask send_file 对相对路径会相对 app.root_path 解析，必须给绝对路径。
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_env_backup_dir = os.environ.get("BACKUP_DIR")
BACKUP_DIR = (
    Path(_env_backup_dir) if _env_backup_dir
    else _PROJECT_ROOT / "backups"
)
if not BACKUP_DIR.is_absolute():
    BACKUP_DIR = BACKUP_DIR.resolve()
STAGING_DIR = BACKUP_DIR / "staging"
PUBLISHED_NAME = "orders_backup_latest.sql"
META_NAME = "orders_backup_latest.meta.json"
TEMP_PREFIX = "orders_drill_"

# 状态
IDLE = "idle"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"

# 阶段（顺序即页面步骤条顺序）
STAGES = [
    "snapshot",        # 导出快照、读取基准
    "dump",            # pg_dump 备份
    "create_db",       # 创建临时库
    "restore",         # 还原到临时库
    "verify_schema",   # 核对列定义
    "verify_rows",     # 逐行核对数据
    "publish",         # 原子换成正式备份
    "cleanup",         # 清理临时库
]
STAGE_LABELS = {
    "snapshot": "取已提交快照与基准数据",
    "dump": "pg_dump 备份两表",
    "create_db": "创建临时库",
    "restore": "还原备份到临时库",
    "verify_schema": "核对列定义",
    "verify_rows": "逐行核对数据",
    "publish": "换成新备份供下载",
    "cleanup": "清理临时库",
    "done": "演练完成",
}

# 支持的故障注入（仅用于演示/自测失败路径）
INJECT_RESTORE = "restore_failure"
INJECT_SCHEMA = "schema_mismatch"
INJECT_ROW = "row_mismatch"
VALID_INJECTIONS = {"", INJECT_RESTORE, INJECT_SCHEMA, INJECT_ROW}

PG_DUMP_BIN = os.environ.get("PG_DUMP_BIN", "pg_dump")
PSQL_BIN = os.environ.get("PSQL_BIN", "psql")
# 临时库清理的语句超时：演练库只有两张表，正常 DROP 为亚秒级；
# 该兜底避免极端情况下清理卡住导致演练状态一直停在“清理临时库”。
CLEANUP_TIMEOUT_MS = int(os.environ.get("CLEANUP_TIMEOUT_MS", "30000"))
# 演练线程同步等待清理完成的最长时间；超时后演练先出结果，
# 清理转为后台守护线程继续，状态字段稍后自动刷新。
CLEANUP_JOIN_TIMEOUT_S = float(os.environ.get("CLEANUP_JOIN_TIMEOUT_S", "8"))

# 列定义核对项。information_schema 在源库与同集群临时库之间对同一
# dump/restore 产物给出完全一致的文本（已实测，含 column_default）。
COLUMN_SQL = """
SELECT ordinal_position,
       column_name,
       data_type,
       udt_name,
       character_maximum_length,
       numeric_precision,
       numeric_scale,
       datetime_precision,
       is_nullable,
       column_default,
       collation_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name = %s
 ORDER BY ordinal_position
"""

PK_SQL = """
SELECT kcu.column_name
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
 WHERE tc.constraint_type = 'PRIMARY KEY'
   AND tc.table_schema = 'public'
   AND tc.table_name = %s
 ORDER BY kcu.ordinal_position
"""


def _now() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _now_hms() -> str:
    return datetime.now().astimezone().strftime("%H:%M:%S")


# ---------------------------------------------------------------- 底层工具
def _pg_env(cfg: dict) -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = cfg["password"]
    # 不让宿主机上其它 PG* 变量干扰
    env["PGHOST"] = cfg["host"]
    env["PGPORT"] = str(cfg["port"])
    env["PGUSER"] = cfg["user"]
    return env


def _run_subprocess(argv: list[str], cfg: dict, timeout: int = 180) -> None:
    """运行 pg_dump / psql；失败时把输出尾部带进异常以便页面展示原因。"""
    try:
        proc = subprocess.run(
            argv,
            env=_pg_env(cfg),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"找不到可执行文件 {argv[0]}：请安装与服务端主版本匹配的 "
            "postgresql-client（Docker 镜像内已安装）"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"命令超时（{timeout}s）：{' '.join(argv[:2])}") from exc
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        tail = "\n".join(detail.splitlines()[-12:])
        raise RuntimeError(
            f"{Path(argv[0]).name} 执行失败（exit={proc.returncode}）：\n{tail}"
        )


def _drop_database(temp_db: str) -> None:
    """终止临时库残留连接并 DROP DATABASE（调用方须已关闭自己的连接）。"""
    conn = db.connect(dbname="postgres", autocommit=True)
    try:
        with conn.cursor() as cur:
            # 兜底：避免信号受限/后端不退出等环境下清理无限等待
            cur.execute("SET statement_timeout = %s", (CLEANUP_TIMEOUT_MS,))
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                  FROM pg_stat_activity
                 WHERE datname = %s
                   AND pid <> pg_backend_pid()
                """,
                (temp_db,),
            )
            cur.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(temp_db)
                )
            )
    finally:
        conn.close()


def cleanup_stale_databases() -> list[str]:
    """启动时尽力清理上次进程异常退出留下的 orders_drill_* 临时库。"""
    dropped: list[str] = []
    try:
        conn = db.connect(dbname="postgres", autocommit=True)
    except Exception:  # noqa: BLE001 - 启动清理只是兜底
        return dropped
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database")
            names = [r[0] for r in cur.fetchall()
                     if r[0].startswith(TEMP_PREFIX)]
        for name in names:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SET statement_timeout = %s",
                        (CLEANUP_TIMEOUT_MS,),
                    )
                    cur.execute(
                        """
                        SELECT pg_terminate_backend(pid)
                          FROM pg_stat_activity
                         WHERE datname = %s
                           AND pid <> pg_backend_pid()
                        """,
                        (name,),
                    )
                    cur.execute(
                        sql.SQL("DROP DATABASE IF EXISTS {}").format(
                            sql.Identifier(name)
                        )
                    )
                dropped.append(name)
            except Exception:  # noqa: BLE001 - 单个失败不影响启动
                pass
    finally:
        conn.close()
    return dropped


# ---------------------------------------------------------------- 管理器
class BackupDrillManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._cleanup_thread: threading.Thread | None = None
        self.state = IDLE
        self.stage: str | None = None
        self.error: str | None = None
        self.cleanup_error: str | None = None
        self.cleanup_ok: bool | None = None
        self.cleanup_pending: bool = False
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.snapshot_id: str | None = None
        self.temp_db: str | None = None
        self.inject: str = ""
        self.pace: float = 0.0
        self.result: dict | None = None
        self.published: dict | None = self._load_published()
        self.events: deque[dict] = deque(maxlen=200)
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        # 清掉上次进程被强杀时可能遗留的暂存文件
        for folder, pattern in (
            (STAGING_DIR, "drill_*.sql"),
            (BACKUP_DIR, "*.json.tmp"),
        ):
            for stale in folder.glob(pattern):
                try:
                    stale.unlink()
                except OSError:
                    pass
        self._log("备份演练管理器就绪")

    # ---------- 日志 / 状态 ----------
    def _log(self, message: str) -> None:
        self.events.appendleft({"time": _now_hms(), "message": message})

    def get_events(self) -> list[dict]:
        with self._lock:
            return list(self.events)

    @property
    def is_running(self) -> bool:
        return self.state == RUNNING

    def snapshot(self) -> dict:
        with self._lock:
            thread_alive = bool(
                self._cleanup_thread and self._cleanup_thread.is_alive()
            )
            pending = self.cleanup_pending and (
                self.cleanup_ok is None or thread_alive
            )
            return {
                "state": self.state,
                "state_label": _STATE_LABELS.get(self.state, self.state),
                "stage": self.stage,
                "stage_label": STAGE_LABELS.get(self.stage or "", self.stage),
                "stages": [
                    {"key": k, "label": STAGE_LABELS[k]} for k in STAGES
                ],
                "error": self.error,
                "cleanup_ok": self.cleanup_ok,
                "cleanup_pending": pending,
                "cleanup_error": self.cleanup_error,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "snapshot_id": self.snapshot_id,
                "temp_db": self.temp_db,
                "inject": self.inject or None,
                "result": self.result,
                "published": self.published,
                "events": list(self.events),
            }

    # ---------- 已发布备份元数据 ----------
    @staticmethod
    def _meta_path() -> Path:
        return BACKUP_DIR / META_NAME

    @staticmethod
    def published_path() -> Path:
        return BACKUP_DIR / PUBLISHED_NAME

    def _load_published(self) -> dict | None:
        sql_path = self.published_path()
        if not sql_path.exists():
            return None
        meta_path = self._meta_path()
        info: dict
        if meta_path.exists():
            try:
                info = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001 - 元数据损坏时退回 stat
                info = {}
        else:
            info = {}
        stat = sql_path.stat()
        info.setdefault("filename", PUBLISHED_NAME)
        info["size_bytes"] = stat.st_size
        info["download_url"] = "/api/backup/download"
        return info

    # ---------- 启动 ----------
    def start(self, pace: float = 0.0, inject: str = "") -> None:
        inject = (inject or "").strip()
        if inject not in VALID_INJECTIONS:
            raise RuntimeError(f"未知故障注入类型：{inject}")
        try:
            pace = max(0.0, min(float(pace), 5.0))
        except (TypeError, ValueError):
            pace = 0.0
        with self._lock:
            if self.state == RUNNING:
                raise RuntimeError("已有备份演练正在进行，请等待结束")
            self.state = RUNNING
            self.stage = STAGES[0]
            self.error = None
            self.cleanup_error = None
            self.cleanup_ok = None
            self.cleanup_pending = False
            self._cleanup_thread = None
            self.started_at = _now()
            self.finished_at = None
            self.snapshot_id = None
            self.temp_db = None
            self.inject = inject
            self.pace = pace
            self.result = None
            self._log(
                "开始订单库备份演练"
                + (f"（故障注入 {inject}）" if inject else "")
                + (f"，放慢节奏 {pace}s/阶段" if pace else "")
            )
            self._thread = threading.Thread(
                target=self._run, name="backup-drill",
                args=(pace, inject), daemon=True,
            )
            self._thread.start()

    # ---------- 阶段辅助 ----------
    def _set_stage(self, stage: str, pace: float) -> None:
        with self._lock:
            self.stage = stage
        if pace:
            time.sleep(pace)

    # ---------- 主流程 ----------
    def _run(self, pace: float, inject: str) -> None:
        cfg = db.dsn()
        source_db = cfg["dbname"]
        temp_db = (
            f"{TEMP_PREFIX}{os.getpid()}_{int(time.time() * 1000)}"
            f"_{secrets.token_hex(2)}"
        )
        staging = STAGING_DIR / (
            f"drill_{int(time.time())}_{secrets.token_hex(4)}.sql"
        )
        snap_conn = None
        verify_conn = None
        temp_created = False
        primary_error: Exception | None = None
        started = time.time()
        table_stats: dict[str, dict] = {}

        try:
            # 1) 导出快照并读取基准 --------------------------------------
            self._set_stage("snapshot", pace)
            snap_conn = db.connect(autocommit=True)
            with snap_conn.cursor() as cur:
                cur.execute(
                    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                cur.execute("SELECT pg_export_snapshot()")
                snapshot_id = cur.fetchone()[0]
            with self._lock:
                self.snapshot_id = snapshot_id
            self._log(f"已导出源库已提交快照 {snapshot_id}")

            expected: dict[str, dict] = {}
            for table in db.TABLES:
                expected[table] = self._read_expected(snap_conn, table)
                table_stats[table] = {
                    "columns": len(expected[table]["columns"]),
                    "rows": expected[table]["row_count"],
                }
            self._log(
                "同一快照读取基准："
                + "，".join(
                    f"{t} {table_stats[t]['columns']} 列/"
                    f"{table_stats[t]['rows']} 行"
                    for t in db.TABLES
                )
            )

            # 2) pg_dump 使用同一快照 ------------------------------------
            self._set_stage("dump", pace)
            argv = [
                PG_DUMP_BIN,
                "--host", cfg["host"],
                "--port", str(cfg["port"]),
                "--username", cfg["user"],
                "--dbname", source_db,
                "--snapshot", snapshot_id,
                "--format=p",
                "--no-owner",
                "--no-privileges",
                "--table", "public.orders",
                "--table", "public.order_items",
                "--file", str(staging),
            ]
            _run_subprocess(argv, cfg)
            size = staging.stat().st_size
            self._log(f"pg_dump 完成：{staging.name}（{size} 字节，同一快照）")

            # 快照已被 pg_dump 导入，基准也已读完，可以关闭快照事务
            try:
                snap_conn.close()
            finally:
                snap_conn = None

            if inject == INJECT_RESTORE:
                with staging.open("a", encoding="utf-8") as fh:
                    fh.write(
                        "\n-- 演练故障注入：还原时必然报错的 SQL\n"
                        "SELECT * FROM __drill_missing_function__();\n"
                    )
                self._log("故障注入：已在备份末尾追加错误 SQL")

            # 3) 创建临时库 ----------------------------------------------
            self._set_stage("create_db", pace)
            admin = db.connect(dbname="postgres", autocommit=True)
            try:
                with admin.cursor() as cur:
                    cur.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(temp_db)
                        )
                    )
            finally:
                admin.close()
            temp_created = True
            with self._lock:
                self.temp_db = temp_db
            self._log(f"已创建临时库 {temp_db}")

            # 4) 实际还原 -------------------------------------------------
            self._set_stage("restore", pace)
            argv = [
                PSQL_BIN,
                "--host", cfg["host"],
                "--port", str(cfg["port"]),
                "--username", cfg["user"],
                "--dbname", temp_db,
                "--no-psqlrc",
                "-v", "ON_ERROR_STOP=1",
                "-q",
                "-f", str(staging),
            ]
            _run_subprocess(argv, cfg, timeout=300)
            self._log(f"备份已实际还原到 {temp_db}")

            verify_conn = db.connect(dbname=temp_db, autocommit=True)

            # 注入：还原后篡改列定义
            if inject == INJECT_SCHEMA:
                with verify_conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE orders ALTER COLUMN customer "
                        "TYPE VARCHAR(200)"
                    )
                self._log("故障注入：临时库 orders.customer 被改成 VARCHAR(200)")

            # 5a) 核对列定义 ---------------------------------------------
            self._set_stage("verify_schema", pace)
            schema_diffs: list[str] = []
            for table in db.TABLES:
                actual_cols = self._read_columns(verify_conn, table)
                schema_diffs.extend(
                    self._diff_columns(
                        table, expected[table]["columns"], actual_cols
                    )
                )
            if schema_diffs:
                preview = "\n".join(schema_diffs[:12])
                raise RuntimeError(
                    f"列定义核对发现 {len(schema_diffs)} 处不一致，例如：\n"
                    f"{preview}"
                )
            self._log("列定义核对通过：两表列名/类型/长度/可空/默认值一致")

            # 注入：还原后篡改一行数据
            if inject == INJECT_ROW:
                with verify_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE order_items SET sku = '__DRILL_TAMPERED__' "
                        "WHERE id = (SELECT min(id) FROM order_items)"
                    )
                    if cur.rowcount == 0:
                        cur.execute(
                            "UPDATE orders SET customer = '__DRILL_TAMPERED__' "
                            "WHERE id = (SELECT min(id) FROM orders)"
                        )
                    if cur.rowcount == 0:
                        # 两表都没有可改的行：插入一行多余订单制造行数差异
                        cur.execute(
                            "INSERT INTO orders (order_no, customer) "
                            "VALUES ('__DRILL_TAMPER_EXTRA__', 'x')"
                        )
                self._log("故障注入：临时库数据被篡改/多出一行")

            # 5b) 逐行核对 ------------------------------------------------
            self._set_stage("verify_rows", pace)
            total_rows = 0
            row_diffs: list[str] = []
            for table in db.TABLES:
                exp = expected[table]
                act_rows = self._read_rows(
                    verify_conn, table, exp["column_names"], exp["pk"]
                )
                diffs = self._diff_rows(table, exp, act_rows)
                row_diffs.extend(diffs[:20])
                total_rows += exp["row_count"]
            if row_diffs:
                preview = "\n".join(row_diffs[:12])
                raise RuntimeError(
                    f"逐行核对发现数据不一致，例如：\n{preview}"
                )
            self._log(f"逐行核对通过：共核对 {total_rows} 行，完全一致")

            verify_conn.close()
            verify_conn = None

            # 6) 还原 + 核对都通过后，才原子换成新备份 --------------------
            self._set_stage("publish", pace)
            size = staging.stat().st_size
            meta = {
                "published_at": _now(),
                "snapshot_id": snapshot_id,
                "source_database": source_db,
                "filename": PUBLISHED_NAME,
                "size_bytes": size,
                "tables": table_stats,
                "rows_checked": total_rows,
                "duration_ms": int((time.time() - started) * 1000),
                "temp_database": temp_db,
                "download_url": "/api/backup/download",
            }
            # 先把 meta 临时文件写好，再连续做两个同目录原子 replace，
            # 把“SQL 已换而 meta 失败”的窗口压到两次 rename 之间。
            tmp_meta = self._meta_path().with_suffix(".json.tmp")
            tmp_meta.write_text(
                json.dumps(meta, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(staging, self.published_path())
            os.replace(tmp_meta, self._meta_path())
            with self._lock:
                self.published = {**meta, "download_url": "/api/backup/download"}
                self.result = {
                    "tables": table_stats,
                    "rows_checked": total_rows,
                    "bytes": size,
                    "duration_ms": meta["duration_ms"],
                }
            self._log(
                f"还原与核对均通过，新备份已替换上线供下载（{size} 字节，"
                f"核对 {total_rows} 行，耗时 {meta['duration_ms']}ms）"
            )

        except Exception as exc:  # noqa: BLE001 - 任何失败都要落到页面
            primary_error = exc
            with self._lock:
                self.state = FAILED
                self.error = f"{type(exc).__name__}: {exc}"
            self._log(f"演练失败：{exc}")
            # 失败时绝不动已发布备份；删除本次暂存文件
            try:
                if staging.exists():
                    staging.unlink()
            except OSError:
                pass
        finally:
            if snap_conn is not None:
                try:
                    snap_conn.close()
                except Exception:  # noqa: BLE001
                    pass
            if verify_conn is not None:
                try:
                    verify_conn.close()
                except Exception:  # noqa: BLE001
                    pass
            # 7) 无论成败都清理临时库；有界等待，超时则转后台守护线程，
            #    不把演练状态永久卡在“清理中”。
            if temp_created:
                with self._lock:
                    self.stage = "cleanup"
                cleanup_thread = self._start_cleanup(temp_db)
                cleanup_thread.join(CLEANUP_JOIN_TIMEOUT_S)
            with self._lock:
                if primary_error is None:
                    self.stage = "done"
                    self.state = SUCCEEDED
                self.finished_at = _now()

    def _start_cleanup(self, temp_db: str) -> threading.Thread:
        """在守护线程里 DROP 临时库，完成后回写清理状态。"""
        with self._lock:
            self.cleanup_ok = None
            self.cleanup_pending = True

        def _work() -> None:
            try:
                _drop_database(temp_db)
                with self._lock:
                    self.cleanup_ok = True
                    self.cleanup_error = None
                    self.cleanup_pending = False
                self._log(f"临时库 {temp_db} 已清理")
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self.cleanup_ok = False
                    self.cleanup_error = str(exc)
                    self.cleanup_pending = False
                self._log(f"临时库 {temp_db} 清理失败：{exc}")

        thread = threading.Thread(
            target=_work, name=f"drill-cleanup-{temp_db[-6:]}", daemon=True
        )
        with self._lock:
            self._cleanup_thread = thread
        thread.start()
        return thread

    # ---------- 读取 / 比较 ----------
    @staticmethod
    def _read_columns(conn, table: str) -> list[tuple]:
        with conn.cursor() as cur:
            cur.execute(COLUMN_SQL, (table,))
            return [tuple(r) for r in cur.fetchall()]

    @staticmethod
    def _read_pk(conn, table: str) -> list[str]:
        with conn.cursor() as cur:
            cur.execute(PK_SQL, (table,))
            return [r[0] for r in cur.fetchall()]

    def _read_expected(self, conn, table: str) -> dict:
        columns = self._read_columns(conn, table)
        column_names = [c[1] for c in columns]
        pk = self._read_pk(conn, table)
        rows = self._read_rows(conn, table, column_names, pk)
        return {
            "columns": columns,
            "column_names": column_names,
            "pk": pk,
            "rows": rows,
            "row_count": len(rows),
        }

    @staticmethod
    def _read_rows(conn, table: str, column_names: list[str],
                   pk: list[str]) -> list[dict]:
        """按列顺序读取全表，并按主键排序，保证可逐行对齐。"""
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(sql.Identifier(c) for c in column_names),
            sql.Identifier(table),
        )
        if pk:
            query = sql.SQL("{} ORDER BY {}").format(
                query,
                sql.SQL(", ").join(sql.Identifier(c) for c in pk),
            )
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def _diff_columns(table: str, expected: list[tuple],
                      actual: list[tuple]) -> list[str]:
        diffs: list[str] = []
        if [c[1] for c in expected] != [c[1] for c in actual]:
            diffs.append(
                f"{table}: 列集合/顺序不一致："
                f"期望 {[c[1] for c in expected]}，"
                f"实际 {[c[1] for c in actual]}"
            )
            return diffs
        # 逐列比较除 ordinal 外的全部定义属性（ordinal 也一并比较）
        for exp, act in zip(expected, actual):
            name = exp[1]
            for idx in range(len(exp)):
                if exp[idx] != act[idx]:
                    attr = _COLUMN_ATTRS[idx] if idx < len(_COLUMN_ATTRS) \
                        else f"attr{idx}"
                    diffs.append(
                        f"{table}.{name} 的 {attr} 不一致："
                        f"备份快照={exp[idx]!r}，还原库={act[idx]!r}"
                    )
        return diffs

    def _diff_rows(self, table: str, expected: dict,
                   actual_rows: list[dict]) -> list[str]:
        pk = expected["pk"]
        exp_rows = expected["rows"]
        diffs: list[str] = []
        if len(exp_rows) != len(actual_rows):
            diffs.append(
                f"{table}: 行数不一致：备份快照 {len(exp_rows)} 行，"
                f"还原库 {len(actual_rows)} 行"
            )
        if not pk:
            # 没有主键：直接按排序后的顺序逐行比较
            for i, (e, a) in enumerate(zip(exp_rows, actual_rows)):
                for col in expected["column_names"]:
                    if e.get(col) != a.get(col):
                        diffs.append(
                            f"{table} 第 {i + 1} 行 列 {col} 不一致："
                            f"快照={e.get(col)!r}，还原={a.get(col)!r}"
                        )
            return diffs

        exp_by_key = {tuple(r[c] for c in pk): r for r in exp_rows}
        act_by_key = {tuple(r[c] for c in pk): r for r in actual_rows}
        missing = [k for k in exp_by_key if k not in act_by_key]
        extra = [k for k in act_by_key if k not in exp_by_key]
        for k in missing[:10]:
            diffs.append(f"{table} 主键 {dict(zip(pk, k))} 在还原库中缺失")
        for k in extra[:10]:
            diffs.append(f"{table} 主键 {dict(zip(pk, k))} 是还原库多出的行")
        for key, exp_row in exp_by_key.items():
            act_row = act_by_key.get(key)
            if act_row is None:
                continue
            for col in expected["column_names"]:
                ev, av = exp_row.get(col), act_row.get(col)
                if ev != av:
                    diffs.append(
                        f"{table} 主键 {dict(zip(pk, key))} 列 {col} "
                        f"不一致：快照={ev!r}，还原={av!r}"
                    )
        return diffs


_COLUMN_ATTRS = [
    "ordinal_position", "column_name", "data_type", "udt_name",
    "character_maximum_length", "numeric_precision", "numeric_scale",
    "datetime_precision", "is_nullable", "column_default",
    "collation_name",
]

_STATE_LABELS = {
    IDLE: "空闲",
    RUNNING: "演练中",
    SUCCEEDED: "演练通过",
    FAILED: "演练失败",
}
