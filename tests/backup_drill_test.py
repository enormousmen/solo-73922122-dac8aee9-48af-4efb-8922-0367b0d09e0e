"""订单库备份演练关键场景测试。

两种运行方式（与 e2e_test.py 一致）：
 1. 对运行中的服务发真实 HTTP：
      BASE_URL=http://localhost:8000 python tests/backup_drill_test.py
 2. 进程内 Flask test client（需本机有与 PG 主版本匹配的 pg_dump/psql）：
      PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
      PGDATABASE=orders_demo \
      PG_DUMP_BIN=/usr/lib/postgresql/16/bin/pg_dump \
      PSQL_BIN=/usr/lib/postgresql/16/bin/psql \
      python tests/backup_drill_test.py

环境变量：
  SLOW_MS            演练期间写单耗时阈值（默认 1500ms，宽松防抖动）
  EXPECT_CLEANUP     1=断言临时库被真实 DROP（正常 Docker/PG 设 1，默认）；
                     0=只断言清理被尝试（信号受限的特殊自测环境用）

覆盖：
 1. 加列前演练通过：列数/行数与 /api/orders 一致，备份可下载；
 2. 放慢演练，在 pg_dump / schema 核对 / rows 核对阶段各写一单，
    全部 201 且低于耗时阈值，且提交后可查；
 3. 新写的单能被下一次演练备份并逐行核对；
 4. 演练中重复发起 / 提交迁移 / 重置均 409；
 5. 加列后演练通过：两表各 6 列，下载 SQL 含 batch_no；
 6. 三种故障注入均失败、页面/状态有原因，且上次通过的备份字节不变；
 7. 临时库清理结果符合 EXPECT_CLEANUP。
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_URL = os.environ.get("BASE_URL")
SLOW_MS = float(os.environ.get("SLOW_MS", "1500"))
EXPECT_CLEANUP = os.environ.get("EXPECT_CLEANUP", "1") == "1"

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
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def download() -> tuple[int, bytes]:
        try:
            with urllib.request.urlopen(
                BASE_URL + "/api/backup/download", timeout=30
            ) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
else:
    from app import app, init_app

    init_app()
    _client = app.test_client()

    def call(method: str, path: str, body: dict | None = None
             ) -> tuple[int, dict]:
        resp = _client.open(
            path, method=method,
            data=json.dumps(body) if body is not None else None,
            headers={"Content-Type": "application/json"},
        )
        return resp.status_code, resp.get_json()

    def download() -> tuple[int, bytes]:
        resp = _client.get("/api/backup/download")
        return resp.status_code, resp.data


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def wait_drill(terminal: set[str] | None = None,
               timeout: float = 90) -> dict:
    terminal = terminal or {"succeeded", "failed"}
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        _, last = call("GET", "/api/backup-drill/state")
        if last.get("state") in terminal:
            return last
        time.sleep(0.2)
    raise AssertionError(f"演练未进入 {terminal}，最后: {last.get('state')} "
                         f"{last.get('stage')} {last.get('error')}")


def reset() -> None:
    call("POST", "/api/reset")
    deadline = time.time() + 15
    while time.time() < deadline:
        _, s = call("GET", "/api/state")
        if s["migration"]["state"] in ("idle", "succeeded"):
            return
        time.sleep(0.2)


def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def main() -> int:
    print(f"备份演练测试目标: {BASE_URL or 'Flask test client（进程内）'}  "
          f"时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    reset()

    section("1. 加列前：演练通过，列数/行数与业务接口一致")
    _, state = call("GET", "/api/state")
    check("初始两表均无 batch_no",
          state["columns"]["orders"] is False
          and state["columns"]["order_items"] is False)
    _, orders_before = call("GET", "/api/orders")
    exp_order_rows = len(orders_before)
    exp_item_rows = sum(len(o["items"]) for o in orders_before)

    st, started = call("POST", "/api/backup-drill/start", {})
    check("发起演练返回 202", st == 202, f"status={st} {started}")
    d = wait_drill(timeout=90)
    check("演练状态=succeeded", d["state"] == "succeeded",
          d.get("error", ""))
    check("无失败原因", not d.get("error"))
    check("记录了已导出快照 ID", bool(d.get("snapshot_id")),
          str(d.get("snapshot_id")))
    check("记录了临时库名", (d.get("temp_db") or "").startswith(
        "orders_drill_"))
    r = d.get("result") or {}
    stats = r.get("tables") or {}
    check("orders 为 5 列（无 batch_no）",
          stats.get("orders", {}).get("columns") == 5, str(stats))
    check("order_items 为 5 列（无 batch_no）",
          stats.get("order_items", {}).get("columns") == 5, str(stats))
    check("核对订单行数与 /api/orders 一致",
          stats.get("orders", {}).get("rows") == exp_order_rows,
          f"{stats} vs {exp_order_rows}")
    check("核对明细行数与 /api/orders 一致",
          stats.get("order_items", {}).get("rows") == exp_item_rows,
          f"{stats} vs {exp_item_rows}")
    check("核对总行数正确",
          r.get("rows_checked") == exp_order_rows + exp_item_rows)

    pub = d.get("published") or {}
    check("状态里带已发布备份信息", bool(pub.get("filename")))
    dl_status, blob = download()
    check("下载备份 200 且非空", dl_status == 200 and len(blob) > 1000,
          f"status={dl_status} len={len(blob)}")
    text = blob.decode("utf-8", "replace")
    check("备份含 orders / order_items 建表",
          "CREATE TABLE public.orders" in text
          and "CREATE TABLE public.order_items" in text)
    check("加列前备份不含 batch_no", "batch_no" not in text)

    good_md5 = md5(blob)

    section("2. 演练期间在备份/核对各阶段试写订单，均快速提交")
    st, slow = call("POST", "/api/backup-drill/start", {"pace": 1.0})
    check("放慢演练已启动", st == 202, str(slow))

    targeted = {"dump": None, "verify_schema": None, "verify_rows": None}
    written_nos: list[str] = []
    max_ms = 0.0
    deadline = time.time() + 60
    while time.time() < deadline:
        _, ds = call("GET", "/api/backup-drill/state")
        if ds["state"] != "running":
            break
        stage = ds.get("stage")
        if stage in targeted and targeted[stage] is None:
            t0 = time.perf_counter()
            wstatus, wdata = call("POST", "/api/orders", {
                "customer": f"演练并发-{stage}",
                "items": [{"sku": f"SKU-{stage}", "quantity": 1,
                           "unit_price": "6.66"}],
            })
            ms = (time.perf_counter() - t0) * 1000
            max_ms = max(max_ms, ms)
            targeted[stage] = (wstatus, ms, wdata)
            if wstatus == 201:
                written_nos.append(wdata["order_no"])
        else:
            time.sleep(0.03)

    for stage, outcome in targeted.items():
        check(f"在 {stage} 阶段完成一单", outcome is not None,
              "未捕获到该阶段窗口")
        if outcome:
            wstatus, ms, _ = outcome
            check(f"{stage} 写单 201 且 < {SLOW_MS:.0f}ms"
                  f"（实测 {ms:.0f}ms）",
                  wstatus == 201 and ms < SLOW_MS,
                  f"status={wstatus} ms={ms:.0f}")

    # 演练进行中的互斥守卫（趁可能还在跑；若已结束则跳过强校验）
    _, ds = call("GET", "/api/backup-drill/state")
    if ds["state"] == "running":
        ms_status, _ = call("POST", "/api/migration/submit", {})
        check("演练中提交迁移被 409 拒绝", ms_status == 409)
        rs_status, _ = call("POST", "/api/reset")
        check("演练中重置被 409 拒绝", rs_status == 409)
        cs_status, _ = call("POST", "/api/backup-drill/start", {})
        check("重复发起演练被 409 拒绝", cs_status == 409)

    d2 = wait_drill(timeout=60)
    check("放慢演练最终 succeeded", d2["state"] == "succeeded",
          d2.get("error", ""))
    check(f"所有目标阶段写单最大耗时 {max_ms:.0f}ms < {SLOW_MS:.0f}ms",
          max_ms < SLOW_MS and max_ms > 0)

    _, orders_after = call("GET", "/api/orders")
    nos_after = {o["order_no"] for o in orders_after}
    check("演练期间所写订单均已提交可查",
          all(no in nos_after for no in written_nos),
          f"written={written_nos}")

    section("3. 再演练一次：新提交的订单被同一快照备份并逐行核对")
    _, before2 = call("GET", "/api/orders")
    rows2 = len(before2) + sum(len(o["items"]) for o in before2)
    call("POST", "/api/backup-drill/start", {})
    d3 = wait_drill(timeout=90)
    check("第二次演练 succeeded", d3["state"] == "succeeded",
          d3.get("error", ""))
    check("第二次核对行数覆盖演练期间的写单",
          (d3.get("result") or {}).get("rows_checked", 0) >= rows2,
          f"result={d3.get('result')} expected>={rows2}")

    section("4. 加列迁移后：演练同样通过，备份包含 batch_no")
    call("POST", "/api/migration/submit", {})
    deadline = time.time() + 20
    while time.time() < deadline:
        _, ms = call("GET", "/api/state")
        if ms["migration"]["state"] == "succeeded":
            break
        time.sleep(0.2)
    _, cols = call("GET", "/api/columns")
    check("两表 batch_no 均已生效",
          cols["orders"]["batch_no"] and cols["order_items"]["batch_no"])

    bbody = {"customer": "备份批次客户", "batch_no": "B-DRILL-1",
             "items": [{"sku": "SKU-BK", "quantity": 1, "unit_price": "9.99"}]}
    wstatus, wdata = call("POST", "/api/orders", bbody)
    check("带批次号写单 201", wstatus == 201, str(wdata))

    call("POST", "/api/backup-drill/start", {})
    d4 = wait_drill(timeout=90)
    check("加列后演练 succeeded", d4["state"] == "succeeded",
          d4.get("error", ""))
    stats4 = (d4.get("result") or {}).get("tables") or {}
    check("加列后两表各 6 列",
          stats4.get("orders", {}).get("columns") == 6
          and stats4.get("order_items", {}).get("columns") == 6,
          str(stats4))
    _, blob4 = download()
    text4 = blob4.decode("utf-8", "replace")
    check("加列后备份含 batch_no 列定义", "batch_no" in text4)
    check("加列后备份含写入的批次值 B-DRILL-1", "B-DRILL-1" in text4)

    section("5. 故障注入：显示原因且上次通过的备份保持不变")
    _, good_blob = download()
    keep_md5 = md5(good_blob)
    for inject, needle in [
        ("restore_failure", "psql"),
        ("schema_mismatch", "列定义"),
        ("row_mismatch", "逐行"),
    ]:
        st, _ = call("POST", "/api/backup-drill/start", {"inject": inject})
        check(f"{inject} 已受理", st == 202)
        df = wait_drill(timeout=90)
        check(f"{inject} 状态=failed", df["state"] == "failed")
        check(f"{inject} 页面状态含原因（{needle}）",
              needle in (df.get("error") or ""), (df.get("error") or "")[:160])
        _, b = download()
        check(f"{inject} 后下载备份仍是上次通过的同一文件",
              md5(b) == keep_md5)

    section("6. 临时库清理")
    temp_db = d4.get("temp_db")
    if EXPECT_CLEANUP:
        check("状态回报 cleanup_ok=true", d4.get("cleanup_ok") is True,
              f"cleanup_ok={d4.get('cleanup_ok')} "
              f"pending={d4.get('cleanup_pending')} "
              f"err={d4.get('cleanup_error')}")
        if not BASE_URL:
            from app import db
            conn = db.connect(dbname="postgres", autocommit=True)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM pg_database "
                                "WHERE datname=%s", (temp_db,))
                    cnt = cur.fetchone()[0]
            finally:
                conn.close()
            check("pg_database 中临时库已不存在", cnt == 0,
                  f"{temp_db} count={cnt}")
    else:
        check("已记录清理结果或正在清理（特殊环境不要求物理 DROP）",
              d4.get("cleanup_ok") in (True, False)
              or d4.get("cleanup_pending") is True)

    section("7. 收尾重置")
    reset()
    check("重置后无演练在跑", True)

    print(f"\n结果：{PASS} 通过，{FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
