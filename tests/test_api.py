"""HTTP 层测试：上传、上报、快照下载的完整接口行为。"""

import base64
import io
import json
import os
import shutil
import tempfile
import unittest

from rescue.app import create_app
from rescue.storage import SECTOR_SIZE


class ApiTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rescue-api-")
        self.app = create_app(self.tmp)
        self.app.testing = True
        self.client = self.app.test_client()
        self.sectors = 10
        self.image = bytes(
            (i * 13 + 5) % 256 for i in range(self.sectors * SECTOR_SIZE)
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def upload(self, image: bytes | None = None, filename: str = "sample.img"):
        data = image if image is not None else self.image
        return self.client.post(
            "/api/job",
            data={"image": (io.BytesIO(data), filename)},
            content_type="multipart/form-data",
        )

    def report(self, entries):
        return self.client.post(
            "/api/sectors",
            data=json.dumps({"sectors": entries}),
            content_type="application/json",
        )

    def report_raw(self, index: int, data: bytes):
        return self.client.post(
            f"/api/sectors?sector={index}", data=data
        )


class JobApiTests(ApiTestBase):
    def test_index_page(self):
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("坏盘救援台", resp.get_data(as_text=True))

    def test_upload_then_status(self):
        resp = self.upload()
        self.assertEqual(resp.status_code, 201)
        body = resp.get_json()
        self.assertEqual(body["progress"]["sector_count"], 10)
        self.assertTrue(self.client.get("/api/job").is_json)

    def test_upload_bad_size(self):
        resp = self.upload(image=b"x" * 600)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("512", resp.get_json()["error"])

    def test_upload_twice_conflict(self):
        self.assertEqual(self.upload().status_code, 201)
        resp = self.upload()
        self.assertEqual(resp.status_code, 409)

    def test_status_without_job_404(self):
        self.assertEqual(self.client.get("/api/job").status_code, 404)
        self.assertEqual(self.client.get("/api/missing").status_code, 404)

    def test_reset(self):
        self.upload()
        self.assertEqual(self.client.delete("/api/job").status_code, 200)
        self.assertEqual(self.client.get("/api/job").status_code, 404)
        # 数据目录清空，可重新建任务
        self.assertEqual(self.upload().status_code, 201)


class ReportApiTests(ApiTestBase):
    def setUp(self):
        super().setUp()
        self.upload()

    def test_json_report_round_trip(self):
        block = self.image[3 * SECTOR_SIZE : 4 * SECTOR_SIZE]
        resp = self.report([{"sector": 3, "data": base64.b64encode(block).decode()}])
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["saved_now"], [3])

        got = self.client.get("/api/sectors/3")
        self.assertEqual(got.status_code, 200)
        self.assertEqual(got.data, block)

    def test_raw_binary_report(self):
        block = b"\x5a" * SECTOR_SIZE
        resp = self.report_raw(2, block)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.client.get("/api/sectors/2").data, block)

    def test_raw_wrong_length(self):
        resp = self.report_raw(2, b"short")
        self.assertEqual(resp.status_code, 400)

    def test_missing_sector_fetch_409(self):
        self.assertEqual(self.client.get("/api/sectors/4").status_code, 409)

    def test_idempotent_duplicate(self):
        block = self.image[:512]
        first = self.report([{"sector": 0, "data": base64.b64encode(block).decode()}])
        self.assertEqual(first.get_json()["saved_now"], [0])
        second = self.report([{"sector": 0, "data": base64.b64encode(block).decode()}])
        body = second.get_json()
        self.assertEqual(body["already_saved"], [0])
        self.assertEqual(body["saved_now"], [])

    def test_rejected_entries_are_isolated(self):
        good = self.image[1 * SECTOR_SIZE : 2 * SECTOR_SIZE]
        resp = self.report(
            [
                {"sector": 1, "data": base64.b64encode(good).decode()},
                {"sector": 99, "data": base64.b64encode(good).decode()},
                {"sector": 2, "data": "!!!not-base64!!!"},
            ]
        )
        body = resp.get_json()
        self.assertEqual(body["saved_now"], [1])
        self.assertEqual(len(body["rejected"]), 2)
        # 合法条目不受影响，缺失清单里 99 不存在、2 仍缺失
        missing = self.client.get("/api/missing").get_json()["missing"]
        self.assertIn(2, missing)
        self.assertNotIn(1, missing)

    def test_zero_sector_not_missing(self):
        resp = self.report([{"sector": 5, "data": base64.b64encode(b"\x00" * 512).decode()}])
        self.assertEqual(resp.get_json()["saved_now"], [5])
        missing = self.client.get("/api/missing").get_json()["missing"]
        self.assertNotIn(5, missing)

    def test_report_without_job_404(self):
        self.client.delete("/api/job")
        resp = self.report_raw(0, b"\x00" * 512)
        self.assertEqual(resp.status_code, 404)


class VerifyApiTests(ApiTestBase):
    def setUp(self):
        super().setUp()
        self.upload()

    def report_many(self, pairs):
        entries = [
            {"sector": i, "data": base64.b64encode(data).decode()}
            for i, data in pairs
        ]
        return self.report(entries)

    def test_verify_retracts_wrong_and_lists_ids(self):
        good0 = self.image[0:512]
        good2 = self.image[1024:1536]
        bad1 = b"\x5a" * 512
        self.report_many([(0, good0), (1, bad1), (2, good2)])

        resp = self.client.post("/api/verify")
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["checked"], 3)
        self.assertEqual(body["mismatched"], [1])
        self.assertEqual(body["retracted"], [1])
        self.assertEqual(body["kept_changed"], [])
        self.assertIn(1, body["progress"]["missing"])
        self.assertEqual(self.client.get("/api/sectors/1").status_code, 409)
        self.assertEqual(self.client.get("/api/sectors/0").data, good0)

    def test_verify_without_job_404(self):
        self.client.delete("/api/job")
        self.assertEqual(self.client.post("/api/verify").status_code, 404)

    def test_verify_missing_sample_422_and_progress_kept(self):
        self.report_many([(0, b"\x01" * 512)])
        before = self.client.get("/api/job").get_json()["progress"]
        os.unlink(os.path.join(self.tmp, "sample.img"))
        resp = self.client.post("/api/verify")
        self.assertEqual(resp.status_code, 422)
        self.assertFalse(resp.get_json()["ok"])
        after = self.client.get("/api/job").get_json()["progress"]
        self.assertEqual(before["saved"], after["saved"])
        self.assertEqual(self.client.get("/api/sectors/0").data, b"\x01" * 512)

    def test_verify_then_reread_reported(self):
        self.report_many([(1, b"\x02" * 512)])
        self.assertEqual(self.client.post("/api/verify").get_json()["retracted"], [1])
        block = self.image[512:1024]
        self.report_many([(1, block)])
        self.assertNotIn(1, self.client.get("/api/missing").get_json()["missing"])
        self.assertEqual(self.client.get("/api/sectors/1").data, block)

    def test_verify_does_not_touch_sectors_saved_after_start(self):
        # 核对开始时只有 0 已救；1 在核对期间才救出，即使内容有误也不在本轮范围
        self.report_many([(0, self.image[0:512])])
        original = self.app.extensions["rescue_storage"]._scan_saved_vs_sample
        storage = self.app.extensions["rescue_storage"]

        def hooked(start_saved):
            result = original(start_saved)
            storage.save_sector(1, b"\x03" * 512)
            return result

        storage._scan_saved_vs_sample = hooked
        body = self.client.post("/api/verify").get_json()
        self.assertEqual(body["checked"], 1)
        self.assertEqual(body["retracted"], [])
        self.assertIn(1, self.client.get("/api/job").get_json()["progress"]["saved"])


class SnapshotApiTests(ApiTestBase):
    def setUp(self):
        super().setUp()
        self.upload()
        # 救出 0,1,4,7；其余缺失
        for i in (0, 1, 4, 7):
            block = self.image[i * SECTOR_SIZE : (i + 1) * SECTOR_SIZE]
            self.report([{"sector": i, "data": base64.b64encode(block).decode()}])

    def test_snapshot_pair_same_id(self):
        resp = self.client.post("/api/snapshots")
        self.assertEqual(resp.status_code, 201)
        sid = resp.get_json()["snapshot"]["snapshot_id"]

        img = self.client.get(f"/api/snapshots/{sid}/image.bin")
        listing = self.client.get(f"/api/snapshots/{sid}/missing.json")
        self.assertEqual(img.status_code, 200)
        self.assertEqual(listing.status_code, 200)
        self.assertEqual(img.headers["X-Snapshot-Id"], sid)
        self.assertEqual(listing.headers["X-Snapshot-Id"], sid)

        payload = listing.get_json()
        self.assertEqual(payload["missing"], [2, 3, 5, 6, 8, 9])

    def test_image_content_and_zero_fill(self):
        sid = self.client.post("/api/snapshots").get_json()["snapshot"]["snapshot_id"]
        img = self.client.get(f"/api/snapshots/{sid}/image.bin").data
        self.assertEqual(len(img), self.sectors * SECTOR_SIZE)
        self.assertEqual(img[:512], self.image[:512])                       # 已救：原样
        self.assertEqual(img[2 * 512 : 3 * 512], b"\x00" * 512)            # 缺失：补零

    def test_snapshot_stays_fixed_as_progress_moves(self):
        sid = self.client.post("/api/snapshots").get_json()["snapshot"]["snapshot_id"]
        # 快照之后再救两个
        for i in (2, 3):
            block = self.image[i * 512 : (i + 1) * 512]
            self.report([{"sector": i, "data": base64.b64encode(block).decode()}])

        old = self.client.get(f"/api/snapshots/{sid}/missing.json").get_json()
        self.assertEqual(old["missing"], [2, 3, 5, 6, 8, 9])
        img = self.client.get(f"/api/snapshots/{sid}/image.bin").data
        self.assertEqual(img[2 * 512 : 3 * 512], b"\x00" * 512)

        sid2 = self.client.post("/api/snapshots").get_json()["snapshot"]["snapshot_id"]
        new = self.client.get(f"/api/snapshots/{sid2}/missing.json").get_json()
        self.assertEqual(new["missing"], [5, 6, 8, 9])

    def test_unknown_snapshot_404(self):
        self.assertEqual(self.client.get("/api/snapshots/nope/image.bin").status_code, 404)
        self.assertEqual(self.client.get("/api/snapshots/nope/missing.json").status_code, 404)

    def test_snapshot_available_after_restart_and_retraction(self):
        sid = self.client.post("/api/snapshots").get_json()["snapshot"]["snapshot_id"]
        old_img = self.client.get(f"/api/snapshots/{sid}/image.bin").data
        old_missing = self.client.get(
            f"/api/snapshots/{sid}/missing.json"
        ).get_json()["missing"]
        self.assertEqual(old_missing, [2, 3, 5, 6, 8, 9])

        # 把 0 号扇区改成错误内容，核对后退回缺失
        storage = self.app.extensions["rescue_storage"]
        storage.save_sector(0, b"\x00" * 512, persist=True)  # 已存在 -> 不覆盖
        path = os.path.join(self.tmp, "sectors", "000", "0.bin")
        with open(path, "wb") as fh:
            fh.write(b"\xee" * 512)
        storage._persist_state()
        self.assertEqual(self.client.post("/api/verify").get_json()["retracted"], [0])

        # 旧快照换一个进程重新打开（模拟重启）仍可下载原字节与旧缺失清单
        from rescue.app import create_app

        reopened = create_app(self.tmp)
        reopened.testing = True
        client2 = reopened.test_client()
        img2 = client2.get(f"/api/snapshots/{sid}/image.bin").data
        missing2 = client2.get(
            f"/api/snapshots/{sid}/missing.json"
        ).get_json()["missing"]
        self.assertEqual(img2, old_img)
        self.assertEqual(missing2, old_missing)

        # 新快照按更新后的进度生成：0 号缺失并补零
        sid2 = client2.post("/api/snapshots").get_json()["snapshot"]["snapshot_id"]
        self.assertNotEqual(sid2, sid)
        new_missing = client2.get(
            f"/api/snapshots/{sid2}/missing.json"
        ).get_json()["missing"]
        self.assertIn(0, new_missing)
        self.assertEqual(
            client2.get(f"/api/snapshots/{sid2}/image.bin").data[:512],
            b"\x00" * 512,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
