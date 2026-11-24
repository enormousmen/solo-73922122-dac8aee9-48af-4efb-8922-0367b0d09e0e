"""Flask 迁移演示页面。

场景：订单系统发版，需要给 orders / order_items 两表一起加可空批次号列。
值班员可以：
- 打开任一表的演示读事务并保持不结束（持有与 DDL 冲突的表级锁）；
- 提交加列申请，观察迁移在应用层等锁、普通读写照常完成；
- 手动结束演示事务，迁移自动继续，两列在同一事务中一起生效；
- 等待中撤销申请（确认后即便释锁也不执行）；
- 勾选故障注入，验证加列失败时两表结构照旧；
- 试写订单/明细、查询批次号列是否生效。
"""
from __future__ import annotations

import threading
import time

from flask import Flask, jsonify, render_template, request

from . import db
from .migration import MigrationManager

app = Flask(__name__)
manager = MigrationManager()

# 保持打开的演示读事务：{token: {"conn": conn, "table": ..., "pid": ...,
#                                "started_at": ..., "rows": ...}}
_demo_lock = threading.RLock()
_demo_conns: dict[str, dict] = {}


# ---------------------------------------------------------------- 页面
@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/state")
def api_state():
    columns = {table: db.column_exists(table) for table in db.TABLES}
    with _demo_lock:
        held = [
            {
                "token": token,
                "table": item["table"],
                "pid": item["pid"],
                "started_at": item["started_at"],
                "rows": item["rows"],
            }
            for token, item in _demo_conns.items()
        ]
    return jsonify(
        migration=manager.snapshot(),
        columns=columns,
        migration_done=all(columns.values()),
        held_transactions=held,
        server_lock_queue=manager.server_lock_queue(),
    )


# ---------------------------------------------------------------- 演示读事务
@app.post("/api/demo-tx/open")
def api_demo_tx_open():
    """在任一表上打开一个保持不结束的读事务。

    显式事务中的 SELECT 会持有该表的 AccessShareLock，它与 ALTER
    需要的 AccessExclusiveLock 冲突，从而制造出等锁场景。
    """
    table = (request.json or {}).get("table")
    if table not in db.TABLES:
        return jsonify({"error": "table 必须是 orders 或 order_items"}), 400

    conn = db.connect()  # 默认 read committed，事务保持打开
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            pid = cur.fetchone()[0]
            cur.execute(f"SELECT count(*) FROM {table}")
            rows = cur.fetchone()[0]
            # 事务保持打开：不 commit / rollback，连接常驻
    except Exception:
        conn.close()
        raise
    token = f"tx-{int(time.time() * 1000)}-{pid}"
    with _demo_lock:
        _demo_conns[token] = {
            "conn": conn,
            "table": table,
            "pid": pid,
            "started_at": time.strftime("%H:%M:%S"),
            "rows": rows,
        }
    return jsonify({"token": token, "table": table, "pid": pid, "rows": rows})


@app.post("/api/demo-tx/close")
def api_demo_tx_close():
    """值班员手动结束演示事务，释放表锁，迁移随后自动继续。"""
    token = (request.json or {}).get("token")
    with _demo_lock:
        item = _demo_conns.pop(token, None)
    if item is None:
        return jsonify({"error": "演示事务不存在或已结束"}), 404
    conn = item["conn"]
    try:
        conn.rollback()  # 结束只读事务并释放锁
    finally:
        conn.close()
    return jsonify({"closed": token, "table": item["table"]})


# ---------------------------------------------------------------- 迁移操作
@app.post("/api/migration/submit")
def api_migration_submit():
    payload = request.json or {}
    try:
        manager.submit(inject_failure=bool(payload.get("inject_failure")))
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(manager.snapshot())


@app.post("/api/migration/cancel")
def api_migration_cancel():
    try:
        manager.cancel()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(manager.snapshot())


# ---------------------------------------------------------------- 普通读写
@app.post("/api/orders")
def api_create_order():
    """试写订单 + 明细；迁移完成后可带批次号。"""
    payload = request.get_json(silent=True) or {}
    customer = (payload.get("customer") or "").strip()
    if not customer:
        return jsonify({"error": "customer 必填"}), 400
    items = payload.get("items") or [
        {"sku": "SKU-DEMO", "quantity": 1, "unit_price": "1.00"}
    ]
    cleaned: list[dict] = []
    for raw in items:
        try:
            cleaned.append({
                "sku": str(raw["sku"]),
                "quantity": int(raw["quantity"]),
                "unit_price": str(raw["unit_price"]),
            })
        except (KeyError, TypeError, ValueError):
            return jsonify({"error": "明细字段不合法"}), 400
    try:
        result = db.insert_order(
            customer=customer,
            order_no=(payload.get("order_no") or None),
            items=cleaned,
            batch_no=(payload.get("batch_no") or None),
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 400
    return jsonify(result), 201


@app.get("/api/orders")
def api_list_orders():
    return jsonify(db.list_orders())


@app.get("/api/columns")
def api_columns():
    return jsonify({
        table: {db.BATCH_COLUMN: db.column_exists(table)}
        for table in db.TABLES
    })


# ---------------------------------------------------------------- 重置演示
@app.post("/api/reset")
def api_reset():
    with _demo_lock:
        if _demo_conns:
            return jsonify({"error": "请先结束所有演示事务再重置"}), 409
    try:
        manager.reset()
        db.reset_database()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify({"ok": True})


def init_app() -> None:
    db.wait_for_database()
    db.ensure_database()
    db.init_schema(seed=True)
    if db.migration_done():
        manager.state = "succeeded"
