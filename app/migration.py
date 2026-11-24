"""在线迁移管理器：给两张表原子地加可空批次号列。

关键设计
========
PostgreSQL 里 ALTER TABLE ... ADD COLUMN 需要 ACCESS EXCLUSIVE 锁；
如果直接发起，而某张表上有一个未结束的演示读事务，ALTER 会进入
服务器端锁等待队列，并且会把后续到达该表的普通读写也一起堵住，
直到演示事务结束。题目要求"等锁期间新发起的正常读写仍能完成"，
因此本管理器**不进入服务器端锁队列**，而是：

1. 先查 pg_locks，看两张表上是否存在与 ACCESS EXCLUSIVE 冲突的、
   已授予(granted)的表级锁；
2. 无冲突时才发起 ALTER，并设置很短的 lock_timeout（毫秒级）兜底；
3. 拿不到锁（lock_timeout）就回滚本轮尝试，稍后重试——
   期间普通读写连接完全不经过任何服务器端等待队列；
4. 两张表的 ALTER 放在同一个数据库事务里提交，任一失败整体回滚，
   保证"加列失败时两表结构照旧"；
5. 等待期间值班员可撤销；撤销后即便演示事务释放锁也不再执行。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone

import psycopg2

from . import db

# 状态机
IDLE = "idle"                      # 没有迁移申请
WAITING = "waiting"                # 已申请，等演示事务释放锁
RUNNING = "running"                # 正在执行加列事务
SUCCEEDED = "succeeded"            # 两列均已生效
FAILED = "failed"                  # 加列失败，结构回滚到原状
CANCELLED = "cancelled"            # 等待中被撤销，永不执行

TERMINAL_STATES = {SUCCEEDED, FAILED, CANCELLED}

# 轮询间隔与兜底锁等待时间，都压到亚秒级，
# 这样即便偶发与普通读写撞锁，影响也在毫秒级。
POLL_INTERVAL = 0.5
LOCK_TIMEOUT_MS = 250

# ALTER TABLE 需要 ACCESS EXCLUSIVE；它与其余所有表级锁模式冲突
# （包括普通读事务持有的 AccessShareLock）。
_CONFLICTING_LOCKS = (
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
)


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")


class MigrationManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self.state = IDLE
        self.error: str | None = None
        self.attempts = 0
        self.requested_at: str | None = None
        self.finished_at: str | None = None
        self.inject_failure = False
        self.events: deque[dict] = deque(maxlen=200)
        self._log("迁移管理器就绪")

    # ---------- 事件日志 ----------
    def _log(self, message: str) -> None:
        self.events.appendleft({"time": _now(), "message": message})

    def get_events(self) -> list[dict]:
        with self._lock:
            return list(self.events)

    # ---------- 对外状态 ----------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "state": self.state,
                "state_label": _STATE_LABELS.get(self.state, self.state),
                "error": self.error,
                "attempts": self.attempts,
                "requested_at": self.requested_at,
                "finished_at": self.finished_at,
                "inject_failure": self.inject_failure,
                "cancellable": self.state == WAITING,
                "events": list(self.events),
            }

    # ---------- 演示用：查看阻塞中的服务器端锁队列长度 ----------
    @staticmethod
    def server_lock_queue() -> list[dict]:
        conn = db.connect(autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT l.relation::regclass::text AS table_name,
                           l.mode, l.granted, a.state,
                           left(coalesce(a.query, ''), 60) AS query
                      FROM pg_locks l
                      JOIN pg_stat_activity a ON a.pid = l.pid
                     WHERE l.relation IN
                           (SELECT oid FROM pg_class
                             WHERE relname = ANY(%s))
                     ORDER BY l.granted, l.relation
                    """,
                    (list(db.TABLES),),
                )
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    # ---------- 迁移申请 ----------
    def submit(self, inject_failure: bool = False) -> None:
        with self._lock:
            if self.state not in TERMINAL_STATES and self.state != IDLE:
                raise RuntimeError("已有迁移申请正在进行")
            if db.migration_done():
                self.state = SUCCEEDED
                self.error = None
                self._log("批次号列已存在，无需迁移")
                return
            self._cancel.clear()
            self.state = WAITING
            self.error = None
            self.attempts = 0
            self.requested_at = _now()
            self.finished_at = None
            self.inject_failure = inject_failure
            self._log(
                "收到加列申请（orders.batch_no、order_items.batch_no）"
                + ("，并启用故障注入" if inject_failure else "")
            )
            self._thread = threading.Thread(
                target=self._run, name="migration", daemon=True
            )
            self._thread.start()

    def cancel(self) -> None:
        """等待中的申请可撤销；确认撤销后即便释锁也不能执行。"""
        with self._lock:
            if self.state != WAITING:
                raise RuntimeError("仅等待中的申请可以撤销")
            self._cancel.set()
            self.state = CANCELLED
            self.finished_at = _now()
            self._log("值班员已撤销等待中的迁移申请，后续不再执行")

    def reset(self) -> None:
        """页面上的重置演示按钮使用；迁移进行中禁止重置。"""
        with self._lock:
            if self.state in (WAITING, RUNNING):
                raise RuntimeError("迁移进行中不能重置演示")
            self._cancel.clear()
            self.state = IDLE
            self.error = None
            self.attempts = 0
            self.requested_at = None
            self.finished_at = None
            self.inject_failure = False
            self._log("迁移状态已重置")

    # ---------- 后台线程 ----------
    def _run(self) -> None:
        try:
            # 等待 -> 尝试加列；若兜底 lock_timeout 触发，回到等待循环
            while not self._cancel.is_set():
                self._wait_for_locks()
                if self._cancel.is_set():
                    # 双重确认：撤销信号一旦置位，永远不再执行
                    with self._lock:
                        if self.state != CANCELLED:
                            self.state = CANCELLED
                            self.finished_at = _now()
                    self._log("撤销已确认，跳过加列")
                    return
                with self._lock:
                    if self.state != WAITING:
                        return
                    self.state = RUNNING
                    self._log("锁已释放，自动继续原加列申请")
                try:
                    self._execute_ddl()
                    break
                except _RetryAfterLockTimeout as exc:
                    with self._lock:
                        self.state = WAITING
                        self.attempts += 1
                    self._log(
                        f"兜底 lock_timeout 触发（{exc}），"
                        "本轮尝试已回滚，重新进入等待，不影响普通读写"
                    )
            else:
                # while 条件因撤销而退出
                with self._lock:
                    if self.state != CANCELLED:
                        self.state = CANCELLED
                        self.finished_at = _now()
                self._log("申请已撤销，未执行加列")
                return
            with self._lock:
                self.state = SUCCEEDED
                self.finished_at = _now()
                self._log("两表批次号列在同一事务中生效，迁移完成")
        except Exception as exc:  # noqa: BLE001 - 失败要落到页面
            with self._lock:
                if self._cancel.is_set():
                    self.state = CANCELLED
                    self.finished_at = _now()
                    self._log("申请已撤销")
                    return
                self.state = FAILED
                self.error = str(exc)
                self.finished_at = _now()
                self._log(f"加列失败，事务已回滚，两表结构照旧：{exc}")

    def _conflicting_holders(self, conn) -> list[dict]:
        """查两张表上已授予、且与 ACCESS EXCLUSIVE 冲突的表级锁。

        ACCESS EXCLUSIVE 与全部 7 种普通表级锁冲突，包括演示读事务
        持有的 AccessShareLock，因此直接按模式清单过滤已授予锁。
        探测连接自身不持有这些表的锁，不会把自己算进去。
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.relname AS table_name, l.mode, l.pid,
                       a.state, a.xact_start
                  FROM pg_locks l
                  JOIN pg_class c ON c.oid = l.relation
                  JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE c.relname = ANY(%s)
                   AND l.locktype = 'relation'
                   AND l.granted
                   AND l.mode = ANY(%s)
                 ORDER BY c.relname
                """,
                (list(db.TABLES), list(_CONFLICTING_LOCKS)),
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _wait_for_locks(self) -> None:
        """应用层等锁：不进 PostgreSQL 锁队列，普通读写不受影响。"""
        while not self._cancel.is_set():
            conn = db.connect(autocommit=True)
            try:
                holders = self._conflicting_holders(conn)
            finally:
                conn.close()
            if not holders:
                return
            with self._lock:
                self.attempts += 1
                n = self.attempts
            tables = sorted({h["table_name"] for h in holders})
            pids = sorted({str(h["pid"]) for h in holders})
            self._log(
                f"第 {n} 次探测：{', '.join(tables)} 上有未结束事务"
                f"（pid {', '.join(pids)}），稍后重试，不阻塞普通读写"
            )
            if self._cancel.wait(POLL_INTERVAL):
                return

    def _execute_ddl(self) -> None:
        conn = db.connect()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(f"SET lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
                # 两张表的 ALTER 在同一事务，任一失败整体回滚
                for table in db.TABLES:
                    cur.execute(
                        f"ALTER TABLE {table} "
                        f"ADD COLUMN {db.BATCH_COLUMN} VARCHAR(64)"
                    )
                if self.inject_failure:
                    # 演示原子性：故意制造错误，上面的 ADD COLUMN 必须一起回滚
                    self._log("故障注入：执行会报错的 SQL 以触发回滚")
                    cur.execute("SELECT 1 / 0")
            conn.commit()
        except psycopg2.Error as exc:
            conn.rollback()
            if exc.pgcode == "55P03":  # lock_timeout
                # 兜底锁等待超时：回到等待循环重试，而不是判失败
                raise _RetryAfterLockTimeout(str(exc))
            raise
        finally:
            conn.close()


class _RetryAfterLockTimeout(Exception):
    """lock_timeout 兜底触发，需要重新进入等待循环。"""


_STATE_LABELS = {
    IDLE: "空闲",
    WAITING: "等待锁中",
    RUNNING: "正在加列",
    SUCCEEDED: "已完成",
    FAILED: "失败（已回滚）",
    CANCELLED: "已撤销",
}
