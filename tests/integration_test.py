#!/usr/bin/env python3
"""端到端集成测试：真实启动 Flask 服务，验证两个关键场景。

场景 A 中断后补读：
  第 1 轮带着坏扇区读盘 -> 杀掉服务 -> 重启 -> 第 2 轮只补缺失扇区 -> 完成。

场景 B 边读边下载：
  生成快照后，一边继续上报扇区一边下载镜像与缺失清单，校验两者属于
  同一 snapshot_id、内容互相吻合（缺失位补零、已读位与样盘一致）。

运行：python3 -I tests/integration_test.py
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SECTOR_SIZE = 512


# ---------------------------------------------------------------------- #
# 小型 HTTP 客户端（标准库实现 multipart 上传）
# ---------------------------------------------------------------------- #
class Client:
    def __init__(self, base: str):
        self.base = base

    def _open(self, method: str, path: str, data=None, headers=None):
        req = urllib.request.Request(
            self.base + path, data=data, headers=headers or {}, method=method
        )
        return urllib.request.urlopen(req, timeout=30)

    def upload(self, image: bytes):
        boundary = "----rescue-boundary-" + uuid.uuid4().hex
        body = io.BytesIO()
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            b'Content-Disposition: form-data; name="image"; filename="sample.img"\r\n'
        )
        body.write(b"Content-Type: application/octet-stream\r\n\r\n")
        body.write(image)
        body.write(f"\r\n--{boundary}--\r\n".encode())
        return json.loads(
            self._open(
                "POST",
                "/api/job",
                body.getvalue(),
                {"Content-Type": f"multipart/form-data; boundary={boundary}"},
            ).read()
        )

    def get_json(self, path: str):
        return json.loads(self._open("GET", path).read())

    def post_json(self, path: str, payload: dict):
        return json.loads(
            self._open(
                "POST",
                path,
                json.dumps(payload).encode(),
                {"Content-Type": "application/json"},
            ).read()
        )

    def get_bytes(self, path: str):
        resp = self._open("GET", path)
        return resp.headers, resp.read()

    def delete(self, path: str):
        return json.loads(self._open("DELETE", path).read())


def wait_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise TimeoutError(f"端口 {port} 未在 {timeout}s 内就绪")


def start_server(data_dir: str, port: int) -> subprocess.Popen:
    env = dict(os.environ, RESCUE_DATA_DIR=data_dir, PORT=str(port))
    proc = subprocess.Popen(
        [sys.executable, "-I", os.path.join(ROOT, "wsgi.py")],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_port(port)
    except Exception:
        proc.terminate()
        print(proc.stdout.read().decode(errors="replace"))
        raise
    return proc


def make_image(sectors: int) -> bytes:
    # 确定性内容，并故意放入几个全零扇区
    buf = bytearray(sectors * SECTOR_SIZE)
    for i in range(sectors * SECTOR_SIZE):
        buf[i] = (i * 31 + 7) % 256
    for zero_sector in (2, 9):
        buf[zero_sector * SECTOR_SIZE : (zero_sector + 1) * SECTOR_SIZE] = b"\x00" * SECTOR_SIZE
    return bytes(buf)


def report_sectors(client: Client, image: bytes, indexes) -> None:
    entries = [
        {
            "sector": i,
            "data": base64.b64encode(image[i * 512 : (i + 1) * 512]).decode(),
        }
        for i in indexes
    ]
    # 分批，每批 25 个
    for start in range(0, len(entries), 25):
        client.post_json("/api/sectors", {"sectors": entries[start : start + 25]})


def report_raw_bytes(client: Client, pairs) -> None:
    """pairs: [(index, 512 字节)]，用于上报「长度够但内容错」的扇区。"""
    entries = [
        {"sector": i, "data": base64.b64encode(data).decode()} for i, data in pairs
    ]
    for start in range(0, len(entries), 25):
        client.post_json("/api/sectors", {"sectors": entries[start : start + 25]})


def check(cond: bool, message: str) -> None:
    if not cond:
        raise AssertionError("校验失败：" + message)
    print("  ✓", message)


def main() -> int:
    sectors = 40
    image = make_image(sectors)
    base_port = 8910

    # ================= 场景 A：中断后补读 =================
    print("== 场景 A：中断重启 + 下一轮补读 ==")
    work = tempfile.mkdtemp(prefix="rescue-e2e-a-")
    port = base_port
    client = Client(f"http://127.0.0.1:{port}")

    proc = start_server(work, port)
    try:
        state = client.upload(image)["progress"]
        check(state["sector_count"] == sectors, f"建任务 {sectors} 扇区")
        check(state["sample_sha256"] == hashlib.sha256(image).hexdigest(),
              "样盘 SHA-256 与本地一致")

        round1_fail = {3, 11, 17, 22, 35}
        round1_read = [i for i in range(sectors) if i not in round1_fail]
        report_sectors(client, image, round1_read)
        state = client.get_json("/api/job")["progress"]
        check(state["saved_count"] == sectors - len(round1_fail),
              f"第 1 轮救出 {state['saved_count']} 个")
        check(sorted(state["missing"]) == sorted(round1_fail),
              "缺失清单恰好是本轮坏扇区")

        # 伪造写到一半的临时文件，验证重启对账
        shard = os.path.join(work, "sectors", f"{11 % 1000:03d}")
        os.makedirs(shard, exist_ok=True)
        with open(os.path.join(shard, "11.tmp"), "wb") as fh:
            fh.write(b"half-written-data")
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        print("  → 服务已停止（模拟值班中断）")

    # 重启：同一数据目录
    proc = start_server(work, port)
    try:
        client = Client(f"http://127.0.0.1:{port}")
        state = client.get_json("/api/job")["progress"]
        check(state["saved_count"] == sectors - len(round1_fail),
              "重启后已完整保存的扇区全部仍可用")
        check(11 in state["missing"], "写到一半的 11.tmp 被清理，扇区 11 仍算缺失")
        check(not os.path.exists(os.path.join(shard, "11.tmp")), "临时文件已对账删除")

        # 第 2 轮：只拉缺失清单补读（坏扇区集合换成另一组，35 仍坏）
        round2_fail = {35}
        missing = client.get_json("/api/missing")["missing"]
        round2_read = [i for i in missing if i not in round2_fail]
        report_sectors(client, image, round2_read)
        state = client.get_json("/api/job")["progress"]
        check(state["saved_count"] == sectors - 1, "第 2 轮后只剩 1 个坏扇区")
        check(state["missing"] == [35], "仍缺失的正是还没修好的 35")

        # 第 3 轮：全部恢复
        report_sectors(client, image, client.get_json("/api/missing")["missing"])
        state = client.get_json("/api/job")["progress"]
        check(state["complete"] is True and state["missing_count"] == 0,
              "全部扇区救出，任务完成")

        snap = client.post_json("/api/snapshots", {})["snapshot"]
        _, img = client.get_bytes(f"/api/snapshots/{snap['snapshot_id']}/image.bin")
        check(img == image, "完整救援镜像与样盘逐字节一致")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    # ================= 场景 B：边读边下载 =================
    print("\n== 场景 B：下载期间读盘继续，快照内容固定 ==")
    work_b = tempfile.mkdtemp(prefix="rescue-e2e-b-")
    port = base_port + 1
    proc = start_server(work_b, port)
    try:
        client = Client(f"http://127.0.0.1:{port}")
        client.upload(image)

        first_batch = list(range(0, 20))
        report_sectors(client, image, first_batch)

        snap = client.post_json("/api/snapshots", {})["snapshot"]
        sid = snap["snapshot_id"]
        check(snap["saved"] == first_batch, "快照记录生成瞬间的 20 个已救扇区")

        # 下载过程中继续读盘（先发起慢一点的镜像下载：分块读取）
        resp = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/snapshots/{sid}/image.bin", timeout=30
        )
        check(resp.headers["X-Snapshot-Id"] == sid, "镜像响应头带快照号")
        downloaded = bytearray()
        chunk = resp.read(512 * 4)  # 先读一部分，期间让读盘继续
        downloaded.extend(chunk)

        report_sectors(client, image, range(20, sectors))  # 全部剩余扇区
        progress_now = client.get_json("/api/job")["progress"]
        check(progress_now["complete"] is True, "下载期间读盘已把全部扇区救完")

        downloaded.extend(resp.read())
        resp.close()

        listing_headers, listing_raw = client.get_bytes(
            f"/api/snapshots/{sid}/missing.json"
        )
        listing = json.loads(listing_raw)
        check(listing_headers["X-Snapshot-Id"] == sid,
              "缺失清单与镜像对应同一 snapshot_id")
        check(listing["missing"] == list(range(20, sectors)),
              "清单仍是快照时刻的缺失（20..39），未被后续读盘改写")
        check(len(downloaded) == sectors * 512, "镜像长度等于整盘大小")

        # 镜像与清单必须互相吻合：清单缺失位补零，其余位等于样盘
        zero = b"\x00" * 512
        missing_set = set(listing["missing"])
        ok = True
        for i in range(sectors):
            block = bytes(downloaded[i * 512 : (i + 1) * 512])
            expect = zero if i in missing_set else image[i * 512 : (i + 1) * 512]
            if block != expect:
                ok = False
                break
        check(ok, "镜像逐扇区与清单吻合：缺失补零、已读原样（含真实全零扇区）")

        # 再生成一份新快照，应显示已全部救出
        snap2 = client.post_json("/api/snapshots", {})["snapshot"]
        check(snap2["missing"] == [], "新快照反映最新进度：无缺失")
        _, img2 = client.get_bytes(f"/api/snapshots/{snap2['snapshot_id']}/image.bin")
        check(img2 == image, "新快照镜像与样盘逐字节一致")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    # ================= 场景 C：核对与补读交错 + 重启 + 快照 =================
    print("\n== 场景 C：样盘核对、退回缺失、补读交错、快照与重启 ==")
    work_c = tempfile.mkdtemp(prefix="rescue-e2e-c-")
    port = base_port + 2
    sectors_c = 30
    proc = start_server(work_c, port)
    try:
        client = Client(f"http://127.0.0.1:{port}")
        client.upload(image[: sectors_c * 512])

        # 第 1 轮：多数扇区正确救出；12、18 上报了长度够但内容错的字节；
        # 5、21 读失败不上报；一个真实全零扇区 2 正常保存。
        wrong = {
            12: b"\xab" * 512,
            18: bytes(reversed(image[18 * 512 : 19 * 512])),
        }
        good = [
            i
            for i in range(sectors_c)
            if i not in wrong and i not in (5, 21)
        ]
        report_sectors(client, image, good)
        report_raw_bytes(client, list(wrong.items()))
        state = client.get_json("/api/job")["progress"]
        check(state["missing"] == [5, 21], "第 1 轮后只有真正读失败的 5、21 缺失")
        check(state["saved_count"] == sectors_c - 2, "读足字节的错扇区暂算已救")

        # 核对前先生成一份旧快照：之后即使核对退回，它仍保留原字节与旧缺失清单
        sid_old = client.post_json("/api/snapshots", {})["snapshot"]["snapshot_id"]
        old_img = client.get_bytes(f"/api/snapshots/{sid_old}/image.bin")[1]
        old_missing = client.get_json(f"/api/snapshots/{sid_old}/missing.json")["missing"]
        check(old_missing == [5, 21], "旧快照缺失清单 = 核对前进度")
        check(old_img[12 * 512 : 13 * 512] == wrong[12],
              "旧快照保留 12 号扇区的原始（错误）字节")

        # 核对与补读交错：核对请求发出后，立即补读 5 号；两者都应收敛
        import threading

        verify_out = {}

        def run_verify():
            verify_out.update(client.post_json("/api/verify", {}))

        verifier = threading.Thread(target=run_verify)
        verifier.start()
        # 核对进行中照常补读：5 号被救出（内容正确），21 号本轮仍失败
        report_sectors(client, image, [5])
        verifier.join(60)
        check(not verifier.is_alive(), "核对线程结束")

        retracted = verify_out.get("retracted", [])
        check(sorted(verify_out.get("mismatched", [])) == [12, 18],
              "核对发现 12、18 内容不符")
        check(retracted == [12, 18], "12、18 被退回缺失并列出")
        state = client.get_json("/api/job")["progress"]
        check(12 in state["missing"] and 18 in state["missing"],
              "退回的 12、18 在进度中算缺失")
        check(5 not in state["missing"], "核对期间补读的 5 保留，未被误退")
        check(client.get_json("/api/missing")["missing"] == [12, 18, 21],
              "最终缺失清单恰为 12、18、21")

        # 旧快照仍可下载且内容不变
        check(client.get_bytes(f"/api/snapshots/{sid_old}/image.bin")[1] == old_img,
              "核对退回后旧快照镜像逐字节不变")
        check(client.get_json(f"/api/snapshots/{sid_old}/missing.json")["missing"]
              == old_missing, "核对退回后旧快照缺失清单不变")

        # 重启后：退回的 12、18 仍算缺失；旧快照仍可下载
        proc.terminate()
        proc.wait(timeout=10)
        proc = start_server(work_c, port)
        client = Client(f"http://127.0.0.1:{port}")
        state = client.get_json("/api/job")["progress"]
        check(state["missing"] == [12, 18, 21], "重启后退回的 12、18 仍算缺失")
        check(client.get_bytes(f"/api/snapshots/{sid_old}/image.bin")[1] == old_img,
              "重启后旧快照仍可下载原字节")
        check(client.get_json(f"/api/snapshots/{sid_old}/missing.json")["missing"]
              == old_missing, "重启后旧快照缺失清单仍可下载")

        # 新快照按更新后的进度生成：12、18、21 缺失补零
        sid_new = client.post_json("/api/snapshots", {})["snapshot"]["snapshot_id"]
        new_missing = client.get_json(f"/api/snapshots/{sid_new}/missing.json")["missing"]
        new_img = client.get_bytes(f"/api/snapshots/{sid_new}/image.bin")[1]
        check(new_missing == [12, 18, 21], "新快照缺失 = 更新后进度")
        check(all(new_img[i * 512 : (i + 1) * 512] == b"\x00" * 512 for i in (12, 18, 21)),
              "新快照对退回扇区补零")

        # 补读交错收尾：把 12、18、21 全部补读回来，再核对应无退回
        report_sectors(client, image, [12, 18, 21])
        final_verify = client.post_json("/api/verify", {})
        check(final_verify["retracted"] == [], "补读后核对不再退回任何扇区")
        state = client.get_json("/api/job")["progress"]
        check(state["saved_count"] == sectors_c and state["complete"] is True,
              "补读完成后任务完整")
        sid_final = client.post_json("/api/snapshots", {})["snapshot"]["snapshot_id"]
        _, final_img = client.get_bytes(f"/api/snapshots/{sid_final}/image.bin")
        check(final_img == image[: sectors_c * 512], "最终快照与样盘逐字节一致")

        # 样盘读不全：截断样盘后核对应报错且不改动进度
        sample_path = os.path.join(work_c, "sample.img")
        before = client.get_json("/api/job")["progress"]
        with open(sample_path, "rb") as fh:
            full = fh.read()
        with open(sample_path, "wb") as fh:
            fh.write(full[:-512])
        try:
            client.post_json("/api/verify", {})
            check(False, "截断样盘后核对应报错")
        except urllib.error.HTTPError as exc:
            check(exc.code == 422, "样盘读不全返回 422")
        after = client.get_json("/api/job")["progress"]
        check(before["saved"] == after["saved"], "样盘读不全时进度保持原样")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print("\n全部集成测试通过 ✅")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.HTTPError as exc:
        print("HTTP 错误", exc.code, exc.read().decode(errors="replace"))
        raise
