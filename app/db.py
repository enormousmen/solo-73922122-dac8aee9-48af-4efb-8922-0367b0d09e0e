"""数据库连接与 schema 管理。

两张业务表：
- orders       订单表
- order_items  订单明细表
迁移要给两张表各加一列可空的 batch_no（批次号）。
"""
from __future__ import annotations

import os
import time

import psycopg2
from psycopg2.extras import RealDictCursor

ORDERS = "orders"
ORDER_ITEMS = "order_items"
BATCH_COLUMN = "batch_no"
TABLES = (ORDERS, ORDER_ITEMS)


def dsn(dbname: str | None = None) -> dict:
    return {
        "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "dbname": dbname or os.environ.get("PGDATABASE", "orders_demo"),
        "connect_timeout": 5,
    }


def connect(dbname: str | None = None, autocommit: bool = False):
    conn = psycopg2.connect(**dsn(dbname))
    conn.autocommit = autocommit
    return conn


def wait_for_database(timeout: float = 60.0) -> None:
    """容器启动时 db 可能还没就绪，轮询等待。"""
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            conn = connect(dbname="postgres", autocommit=True)
            conn.close()
            return
        except psycopg2.Error as exc:  # pragma: no cover - 启动竞态
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"数据库在 {timeout}s 内未就绪: {last_error}")


def ensure_database() -> None:
    """创建演示数据库（幂等）。"""
    conn = connect(dbname="postgres", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s",
                        (dsn()["dbname"],))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE {dsn()["dbname"]}')
    finally:
        conn.close()


SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {ORDERS} (
    id          BIGSERIAL PRIMARY KEY,
    order_no    VARCHAR(40) NOT NULL UNIQUE,
    customer    VARCHAR(100) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'created',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {ORDER_ITEMS} (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES {ORDERS}(id),
    sku         VARCHAR(60) NOT NULL,
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(12, 2) NOT NULL
);
"""

SEED_ORDERS = [
    ("SO-1001", "张三", [
        ("SKU-A100", 2, "19.90"),
        ("SKU-B200", 1, "128.00"),
    ]),
    ("SO-1002", "李四", [
        ("SKU-C300", 5, "9.90"),
    ]),
]


def init_schema(seed: bool = True) -> None:
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            if seed:
                cur.execute(f"SELECT count(*) FROM {ORDERS}")
                if cur.fetchone()[0] == 0:
                    for order_no, customer, items in SEED_ORDERS:
                        cur.execute(
                            f"INSERT INTO {ORDERS} (order_no, customer) "
                            "VALUES (%s, %s) RETURNING id",
                            (order_no, customer),
                        )
                        order_id = cur.fetchone()[0]
                        for sku, qty, price in items:
                            cur.execute(
                                f"INSERT INTO {ORDER_ITEMS} "
                                "(order_id, sku, quantity, unit_price) "
                                "VALUES (%s, %s, %s, %s)",
                                (order_id, sku, qty, price),
                            )
    finally:
        conn.close()


def column_exists(table: str, column: str = BATCH_COLUMN) -> bool:
    if table not in TABLES:
        raise ValueError(f"未知表: {table}")
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_name = %s AND column_name = %s
                """,
                (table, column),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def migration_done() -> bool:
    return all(column_exists(t) for t in TABLES)


def reset_database() -> None:
    """删除两表并重建样例数据，便于反复演示。"""
    conn = connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {ORDER_ITEMS}")
            cur.execute(f"DROP TABLE IF EXISTS {ORDERS}")
    finally:
        conn.close()
    init_schema(seed=True)


def list_orders() -> list[dict]:
    """读取订单 + 明细。迁移前列不存在，迁移后把批次号一起带出。"""
    conn = connect(autocommit=True)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            extra = f", o.{BATCH_COLUMN}" if column_exists(ORDERS) else ""
            cur.execute(
                f"SELECT o.id, o.order_no, o.customer, o.status, "
                f"o.created_at{extra} FROM {ORDERS} o ORDER BY o.id"
            )
            orders = [dict(row) for row in cur.fetchall()]
            item_extra = f", i.{BATCH_COLUMN}" if column_exists(ORDER_ITEMS) else ""
            cur.execute(
                f"SELECT i.id, i.order_id, i.sku, i.quantity, i.unit_price"
                f"{item_extra} FROM {ORDER_ITEMS} i ORDER BY i.id"
            )
            items = [dict(row) for row in cur.fetchall()]
        by_order: dict[int, list[dict]] = {}
        for item in items:
            by_order.setdefault(item["order_id"], []).append(item)
        for order in orders:
            order["items"] = by_order.get(order["id"], [])
        return orders
    finally:
        conn.close()


def insert_order(customer: str, order_no: str | None,
                 items: list[dict], batch_no: str | None) -> dict:
    """试写一笔订单及其明细；迁移后可同时写批次号。"""
    conn = connect()
    try:
        with conn.cursor() as cur:
            # order_no 为 NOT NULL UNIQUE：用序列发号而非 max(id)+1，
            # 保证并发下不撞唯一键（未使用的号段会有空洞，业务可接受）。
            if not order_no:
                cur.execute(
                    "SELECT nextval(pg_get_serial_sequence('orders', 'id'))"
                )
                order_no = f"SO-{cur.fetchone()[0]:04d}"

            order_cols = "order_no, customer, status"
            order_placeholders = "%s, %s, %s"
            params: list = [order_no, customer, "created"]
            if column_exists(ORDERS):
                order_cols += f", {BATCH_COLUMN}"
                order_placeholders += ", %s"
                params.append(batch_no)
            cur.execute(
                f"INSERT INTO {ORDERS} ({order_cols}) "
                f"VALUES ({order_placeholders}) RETURNING id",
                params,
            )
            order_id = cur.fetchone()[0]

            has_item_batch = column_exists(ORDER_ITEMS)
            for item in items:
                cols = "order_id, sku, quantity, unit_price"
                vals = "%s, %s, %s, %s"
                iparams: list = [order_id, item["sku"],
                                  int(item["quantity"]), item["unit_price"]]
                if has_item_batch:
                    cols += f", {BATCH_COLUMN}"
                    vals += ", %s"
                    iparams.append(batch_no)
                cur.execute(
                    f"INSERT INTO {ORDER_ITEMS} ({cols}) VALUES ({vals})",
                    iparams,
                )
        conn.commit()
        return {"order_id": order_id, "order_no": order_no}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
