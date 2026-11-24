"""端到端关键操作测试。

两种运行方式：
 1. 对运行中的 Flask 服务发真实 HTTP 请求：
       BASE_URL=http://localhost:8000 python tests/e2e_test.py
 2. 进程内模式（默认，使用 Flask test client，走完整 WSGI 栈）：
       PGPORT=5433 python tests/e2e_test.py

覆盖场景：
 1. 初始状态：两表无 batch_no，样例订单存在；
 2. 打开演示读事务 -> 提交迁移 -> 迁移进入等待；
 3. 等锁期间普通读/写均快速完成（有计时阈值），且服务器端锁队列
    中不存在 granted=false 的 ALTER 排队记录；
 4. 撤销 -> 结束演示事务 -> 等待后确认两列仍未生效；
 5. 再开演示事务 -> 再提交 -> 结束事务 -> 迁移自动继续，两列一同生效；
 6. 生效后可写入并读回批次号；
 7. 故障注入：重置后再次迁移并注入错误，验证两表结构照旧；
 8. 重置功能恢复到无列状态。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

# 确保可以 import 到工作区里的 app 包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_URL = os.environ.get("BASE_URL")  # 不设置则走进程内 test client
SLOW_MS = float(os.environ.get("SLOW_MS", "300"))  # 等锁期普通读写阈值

PASS, FAIL = 0, 0

if BASE_URL:
    def call(method: str, path: str, body: dict | None = None
             ) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            BASE_URL + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())
else:
    from app import app, init_app

    init_app()
    _client = app.test_client()

    def call(method: str, path: str, body: dict | None = None
             ) -> tuple[int, dict]:
        resp = _client.open(
            path,
            method=method,
            data=json.dumps(body) if body is not None else None,
            headers={"Content-Type": "application/json"},
        )
        return resp.status_code, resp.get_json()


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def timed(method: str, path: str, body=None) -> tuple[float, int, dict]:
    t0 = time.perf_counter()
    status, data = call(method, path, body)
    ms = (time.perf_counter() - t0) * 1000
    return ms, status, data


def wait_state(states: set[str], timeout: float = 10) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        _, last = call("GET", "/api/state")
        if last["migration"]["state"] in states:
            return last
        time.sleep(0.2)
    raise AssertionError(f"状态未进入 {states}，最后状态: {last}")


def reset() -> None:
    call("POST", "/api/reset")
    wait_state({"idle", "succeeded"})


def no_queued_ddl(state: dict) -> bool:
    """服务器端锁队列里不应有未授予（排队中）的锁。"""
    return all(row["granted"] for row in state["server_lock_queue"])


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    print(f"测试目标: {BASE_URL}  时间: {datetime.now():%Y-%m-%d %H:%M:%S}")

    section("1. 初始状态与样例数据")
    reset()
    _, state = call("GET", "/api/state")
    check("两表初始均无 batch_no",
          state["columns"]["orders"] is False
          and state["columns"]["order_items"] is False)
    _, orders = call("GET", "/api/orders")
    check("样例订单为 2 笔", len(orders) == 2, f"实际 {len(orders)}")
    check("样例明细共 3 行",
          sum(len(o["items"]) for o in orders) == 3)

    section("2. 演示事务 + 迁移进入等待")
    _, tx = call("POST", "/api/demo-tx/open", {"table": "orders"})
    check("演示事务已打开", tx.get("token", "").startswith("tx-"))
    call("POST", "/api/migration/submit", {})
    state = wait_state({"waiting"})
    check("迁移状态=等待锁中", state["migration"]["state"] == "waiting")

    section("3. 等锁期间普通读写不受阻")
    ms, status, data = timed("GET", "/api/orders")
    check(f"普通读成功且耗时 < {SLOW_MS}ms（实测 {ms:.1f}ms）",
          status == 200 and ms < SLOW_MS)
    payload = {"customer": "压测客户",
               "items": [{"sku": "SKU-T", "quantity": 1, "unit_price": "1.00"}]}
    ms, status, data = timed("POST", "/api/orders", payload)
    check(f"普通写成功且耗时 < {SLOW_MS}ms（实测 {ms:.1f}ms）",
          status == 201 and ms < SLOW_MS, f"resp={data}")
    ms, status, data = timed("GET", "/api/orders")
    check(f"再次普通读 {ms:.1f}ms 成功", status == 200 and ms < SLOW_MS)
    _, state = call("GET", "/api/state")
    check("服务器端无排队中（granted=false）的 DDL 锁",
          no_queued_ddl(state), str(state["server_lock_queue"]))
    check("迁移仍处于等待、尝试次数已增长",
          state["migration"]["state"] == "waiting"
          and state["migration"]["attempts"] >= 1)

    section("4. 撤销后即使释锁也不执行")
    _, cancelled = call("POST", "/api/migration/cancel")
    check("撤销接口返回已撤销", cancelled["state"] == "cancelled")
    call("POST", "/api/demo-tx/close", {"token": tx["token"]})
    time.sleep(2)  # 超过两个轮询周期
    _, state = call("GET", "/api/state")
    check("撤销后释锁，两列仍未生效",
          not any(state["columns"].values()), str(state["columns"]))
    check("状态保持已撤销", state["migration"]["state"] == "cancelled")

    section("5. 释锁后迁移自动继续，两列一同生效")
    _, tx2 = call("POST", "/api/demo-tx/open", {"table": "order_items"})
    call("POST", "/api/migration/submit", {})
    wait_state({"waiting"})
    call("POST", "/api/demo-tx/close", {"token": tx2["token"]})
    state = wait_state({"succeeded"}, timeout=15)
    check("迁移完成", state["migration"]["state"] == "succeeded")
    check("orders.batch_no 已生效", state["columns"]["orders"] is True)
    check("order_items.batch_no 已生效",
          state["columns"]["order_items"] is True)

    section("6. 生效后批次号可写可读")
    body = {"customer": "批次客户", "batch_no": "B-E2E-001",
            "items": [{"sku": "SKU-B1", "quantity": 2, "unit_price": "3.33"}]}
    status, data = call("POST", "/api/orders", body)
    check("带批次号写入成功", status == 201, str(data))
    _, orders = call("GET", "/api/orders")
    target = next(o for o in orders if o["id"] == data["order_id"])
    check("订单批次号读回正确", target["batch_no"] == "B-E2E-001")
    check("明细批次号同步写入",
          all(i["batch_no"] == "B-E2E-001" for i in target["items"]))

    section("7. 故障注入：失败时两表结构照旧")
    reset()
    _, state = call("GET", "/api/state")
    check("重置后两列均不存在", not any(state["columns"].values()))
    _, tx3 = call("POST", "/api/demo-tx/open", {"table": "orders"})
    call("POST", "/api/migration/submit", {"inject_failure": True})
    wait_state({"waiting"})
    call("POST", "/api/demo-tx/close", {"token": tx3["token"]})
    state = wait_state({"failed"}, timeout=15)
    check("迁移状态=失败（已回滚）", state["migration"]["state"] == "failed")
    check("失败原因已记录", bool(state["migration"]["error"]))
    _, cols = call("GET", "/api/columns")
    check("orders 结构照旧（无 batch_no）",
          cols["orders"]["batch_no"] is False)
    check("order_items 结构照旧（无 batch_no）",
          cols["order_items"]["batch_no"] is False)
    _, orders = call("GET", "/api/orders")
    check("失败后普通读仍可用、样例数据完好", len(orders) == 2)

    section("8. 清理：重置恢复初始演示环境")
    reset()
    _, state = call("GET", "/api/state")
    check("重置后迁移状态=空闲", state["migration"]["state"] == "idle")
    check("重置后无打开的演示事务",
          state["held_transactions"] == [])

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
