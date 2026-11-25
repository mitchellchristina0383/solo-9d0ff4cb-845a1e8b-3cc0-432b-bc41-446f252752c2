"""存储层单元测试：原子写入、崩溃对账、快照一致性。"""

import io
import os
import shutil
import tempfile
import threading
import time
import unittest

from rescue.storage import (
    MAX_IMAGE_SIZE,
    SECTOR_SIZE,
    JobChangedError,
    JobExistsError,
    JobStorage,
    SampleUnreadableError,
    StorageError,
    VerificationBusyError,
)


class StorageTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rescue-test-")
        self.storage = JobStorage(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_image(self, sectors: int) -> bytes:
        return bytes((i * 7 + 1) % 256 for i in range(sectors * SECTOR_SIZE))


class UploadTests(StorageTestBase):
    def test_valid_upload_creates_job(self):
        image = self.make_image(4)
        state = self.storage.create_job(io.BytesIO(image))
        self.assertEqual(state["sector_count"], 4)
        self.assertEqual(state["saved_count"], 0)
        self.assertEqual(state["missing_count"], 4)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "sample.img")))

    def test_empty_rejected(self):
        with self.assertRaises(StorageError):
            self.storage.create_job(io.BytesIO(b""))

    def test_non_multiple_of_512_rejected(self):
        with self.assertRaises(StorageError):
            self.storage.create_job(io.BytesIO(b"x" * (512 + 1)))

    def test_too_large_rejected(self):
        oversized = b"x" * (MAX_IMAGE_SIZE + SECTOR_SIZE)
        with self.assertRaises(StorageError):
            self.storage.create_job(io.BytesIO(oversized))
        # 超限后不留临时样盘
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "sample.img.tmp")))

    def test_8mib_boundary_accepted(self):
        image = self.make_image(MAX_IMAGE_SIZE // SECTOR_SIZE)
        state = self.storage.create_job(io.BytesIO(image))
        self.assertEqual(state["sector_count"], MAX_IMAGE_SIZE // SECTOR_SIZE)

    def test_duplicate_upload_rejected(self):
        self.storage.create_job(io.BytesIO(self.make_image(2)))
        with self.assertRaises(JobExistsError):
            self.storage.create_job(io.BytesIO(self.make_image(2)))


class SectorWriteTests(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.image = self.make_image(6)
        self.storage.create_job(io.BytesIO(self.image))

    def test_save_and_readback(self):
        data = self.image[:512]
        self.assertTrue(self.storage.save_sector(0, data))
        self.assertEqual(self.storage.read_saved_sector(0), data)

    def test_duplicate_save_is_idempotent(self):
        self.storage.save_sector(1, self.image[512:1024])
        self.assertFalse(self.storage.save_sector(1, self.image[512:1024]))
        self.assertEqual(self.storage.get_state()["saved_count"], 1)

    def test_out_of_range_rejected(self):
        with self.assertRaises(StorageError):
            self.storage.save_sector(6, b"x" * 512)
        with self.assertRaises(StorageError):
            self.storage.save_sector(-1, b"x" * 512)

    def test_wrong_length_rejected(self):
        with self.assertRaises(StorageError):
            self.storage.save_sector(0, b"x" * 100)

    def test_all_zero_sector_is_saved_not_missing(self):
        # 实际读到的全零扇区必须正常保存，不算缺失
        self.storage.save_sector(2, b"\x00" * 512)
        state = self.storage.get_state()
        self.assertIn(2, state["saved"])
        self.assertNotIn(2, state["missing"])
        self.assertEqual(self.storage.read_saved_sector(2), b"\x00" * 512)

    def test_batch_mixed(self):
        result = self.storage.save_batch(
            [
                (0, self.image[:512]),
                (0, self.image[:512]),          # 重复
                (99, self.image[:512]),         # 越界
                (1, b"short"),                  # 长度错
                (2, self.image[1024:1536]),
            ]
        )
        self.assertEqual(result["saved_now"], [0, 2])
        self.assertEqual(result["already_saved"], [0])
        self.assertEqual(len(result["rejected"]), 2)

    def test_missing_list(self):
        for i in (0, 2, 4):
            self.storage.save_sector(i, self.image[i * 512 : (i + 1) * 512])
        self.assertEqual(self.storage.missing_sectors(), [1, 3, 5])


class RestartTests(StorageTestBase):
    def test_state_rebuilt_from_disk(self):
        image = self.make_image(5)
        self.storage.create_job(io.BytesIO(image))
        for i in (0, 1, 3):
            self.storage.save_sector(i, image[i * 512 : (i + 1) * 512])

        reopened = JobStorage(self.tmp)
        state = reopened.get_state()
        self.assertEqual(state["saved"], [0, 1, 3])
        self.assertEqual(state["missing"], [2, 4])
        self.assertEqual(reopened.read_saved_sector(1), image[512:1024])

    def test_half_written_tmp_treated_as_missing(self):
        image = self.make_image(4)
        self.storage.create_job(io.BytesIO(image))
        self.storage.save_sector(0, image[:512])

        # 伪造一个写到一半的临时文件（进程崩溃场景）
        shard = os.path.join(self.tmp, "sectors", "002")
        os.makedirs(shard, exist_ok=True)
        with open(os.path.join(shard, "2.tmp"), "wb") as fh:
            fh.write(b"partial")

        reopened = JobStorage(self.tmp)
        # 临时文件被清理
        self.assertFalse(os.path.exists(os.path.join(shard, "2.tmp")))
        state = reopened.get_state()
        self.assertEqual(state["saved"], [0])
        self.assertIn(2, state["missing"])

    def test_saved_file_without_state_entry_still_recognized(self):
        # job.json 落后于磁盘：磁盘上多出来的扇区也应被认回
        image = self.make_image(3)
        self.storage.create_job(io.BytesIO(image))
        self.storage.save_sector(0, image[:512])
        target = os.path.join(self.tmp, "sectors", "001", "1.bin")
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as fh:
            fh.write(image[512:1024])

        reopened = JobStorage(self.tmp)
        self.assertEqual(reopened.get_state()["saved"], [0, 1])


class SnapshotTests(StorageTestBase):
    def setUp(self):
        super().setUp()
        self.image = self.make_image(8)
        self.storage.create_job(io.BytesIO(self.image))
        for i in (0, 1, 3, 4, 7):
            self.storage.save_sector(i, self.image[i * 512 : (i + 1) * 512])

    def test_snapshot_image_zero_fills_missing(self):
        snap = self.storage.create_snapshot()
        stream = self.storage.open_snapshot_image(snap["snapshot_id"])
        content = b"".join(stream)
        self.assertEqual(len(content), 8 * 512)
        zero = b"\x00" * 512
        for i in range(8):
            block = content[i * 512 : (i + 1) * 512]
            if i in (0, 1, 3, 4, 7):
                self.assertEqual(block, self.image[i * 512 : (i + 1) * 512])
            else:
                self.assertEqual(block, zero)

    def test_snapshot_missing_list_matches(self):
        snap = self.storage.create_snapshot()
        listing = self.storage.snapshot_missing_list(snap["snapshot_id"])
        self.assertEqual(listing["missing"], [2, 5, 6])
        self.assertEqual(listing["missing_count"], 3)
        self.assertEqual(listing["snapshot_id"], snap["snapshot_id"])

    def test_snapshot_immutable_while_reading_continues(self):
        snap = self.storage.create_snapshot()
        sid = snap["snapshot_id"]
        # 快照后继续救出扇区
        self.storage.save_sector(2, self.image[2 * 512 : 3 * 512])
        self.storage.save_sector(5, self.image[5 * 512 : 6 * 512])

        old_listing = self.storage.snapshot_missing_list(sid)
        self.assertEqual(old_listing["missing"], [2, 5, 6])  # 仍是旧进度
        content = b"".join(self.storage.open_snapshot_image(sid))
        self.assertEqual(content[2 * 512 : 3 * 512], b"\x00" * 512)  # 旧快照补零

        new_snap = self.storage.create_snapshot()
        new_listing = self.storage.snapshot_missing_list(new_snap["snapshot_id"])
        self.assertEqual(new_listing["missing"], [6])

    def test_unknown_snapshot_404(self):
        self.assertIsNone(self.storage.open_snapshot_image("nope"))
        self.assertIsNone(self.storage.snapshot_missing_list("nope"))

    def test_snapshot_survives_restart_and_keeps_original_bytes(self):
        snap = self.storage.create_snapshot()
        sid = snap["snapshot_id"]
        original = b"".join(self.storage.open_snapshot_image(sid))

        # 快照后把已救扇区改成别的字节，再“重启”
        self.storage.save_sector(0, b"\xaa" * 512)  # 已存在 -> 不覆盖
        reopened = JobStorage(self.tmp)
        reopened.save_sector(6, b"\xbb" * 512)  # 新救出（旧快照应仍补零）

        self.assertEqual(b"".join(reopened.open_snapshot_image(sid)), original)
        listing = reopened.snapshot_missing_list(sid)
        self.assertEqual(listing["missing"], [2, 5, 6])


class VerifyTests(StorageTestBase):
    """用样盘核对：退回读错的扇区、并发补读、样盘异常、任务被换。"""

    def setUp(self):
        super().setUp()
        self.image = self.make_image(10)
        self.storage.create_job(io.BytesIO(self.image))

    def _write_sector_file(self, index: int, data: bytes) -> None:
        path = os.path.join(
            self.tmp, "sectors", f"{index % 1000:03d}", f"{index}.bin"
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(data)

    def _save_good(self, indexes) -> None:
        for i in indexes:
            self.storage.save_sector(i, self.image[i * 512 : (i + 1) * 512])

    def test_retracts_only_wrong_bytes_and_lists_them(self):
        self._save_good((0, 2))
        bad = b"\xde\xad" * 256
        self.storage.save_sector(1, bad)
        self.storage.save_sector(3, b"\x00" * 512)  # 真实全零，但与样盘不符

        result = self.storage.verify_saved_sectors()
        self.assertEqual(result["checked"], 4)
        self.assertEqual(result["mismatched"], [1, 3])
        self.assertEqual(result["retracted"], [1, 3])
        self.assertEqual(result["kept_changed"], [])

        state = self.storage.get_state()
        self.assertEqual(state["saved"], [0, 2])
        self.assertIn(1, state["missing"])
        self.assertIn(3, state["missing"])

    def test_correct_sector_kept_and_all_zero_sample_sector(self):
        image = bytearray(self.image)
        image[4 * 512 : 5 * 512] = b"\x00" * 512
        storage = JobStorage(self.tmp + "-zero")
        try:
            storage.create_job(io.BytesIO(bytes(image)))
            storage.save_sector(4, b"\x00" * 512)
            result = storage.verify_saved_sectors()
            self.assertEqual(result["retracted"], [])
            self.assertNotIn(4, storage.get_state()["missing"])
        finally:
            shutil.rmtree(self.tmp + "-zero", ignore_errors=True)

    def test_retraction_survives_restart_and_can_be_reread(self):
        self._save_good((0,))
        self.storage.save_sector(1, b"\x11" * 512)
        self.storage.verify_saved_sectors()
        self.assertEqual(self.storage.get_state()["missing"], list(range(1, 10)))

        reopened = JobStorage(self.tmp)
        self.assertIn(1, reopened.get_state()["missing"])
        self.assertIsNone(reopened.read_saved_sector(1))

        reopened.save_sector(1, self.image[512:1024])
        self.assertNotIn(1, reopened.get_state()["missing"])
        self.assertEqual(reopened.read_saved_sector(1), self.image[512:1024])

    def test_reading_continues_during_verification(self):
        self._save_good((0, 2))
        self.storage.save_sector(1, b"\x22" * 512)

        scan_started = threading.Event()
        scan_go = threading.Event()
        original = self.storage._sample_usable
        original_scan = self.storage._scan_saved_vs_sample

        def hooked_usable(size, sha):
            original(size, sha)
            scan_started.set()
            self.assertTrue(scan_go.wait(10))

        def hooked_scan(start_saved):
            result = original_scan(start_saved)
            scan_started.set()
            self.assertTrue(scan_go.wait(10))
            return result

        self.storage._sample_usable = hooked_usable
        self.storage._scan_saved_vs_sample = hooked_scan

        outcome = {}

        def run_verify():
            outcome["result"] = self.storage.verify_saved_sectors()

        worker = threading.Thread(target=run_verify)
        worker.start()
        self.assertTrue(scan_started.wait(10))

        # 核对进行中，补读不受影响：新扇区立即可写入并出现在进度里
        started = time.monotonic()
        batch = self.storage.save_batch(
            [(5, self.image[5 * 512 : 6 * 512])]
        )
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(batch["saved_now"], [5])
        self.assertIn(5, self.storage.get_state()["saved"])

        scan_go.set()
        worker.join(10)
        self.assertFalse(worker.is_alive())

        result = outcome["result"]
        self.assertEqual(result["checked"], 3)  # 只核对核对开始时已救的 0,1,2
        self.assertEqual(result["retracted"], [1])
        self.assertIn(5, self.storage.get_state()["saved"])  # 核对后仍保留

    def test_only_current_bad_bytes_are_retracted(self):
        self._save_good((0, 2))
        self.storage.save_sector(1, b"\x33" * 512)
        self.storage.save_sector(3, b"\x44" * 512)

        original_scan = self.storage._scan_saved_vs_sample

        def hooked_scan(start_saved):
            suspects = original_scan(start_saved)
            # 模拟核对窗口内扇区 3 被并发写入了正确字节
            self._write_sector_file(3, self.image[3 * 512 : 4 * 512])
            return suspects

        self.storage._scan_saved_vs_sample = hooked_scan
        result = self.storage.verify_saved_sectors()
        self.assertEqual(result["mismatched"], [1, 3])
        self.assertEqual(result["retracted"], [1])
        self.assertEqual(result["kept_changed"], [3])
        self.assertEqual(self.storage.get_state()["saved"], [0, 2, 3])

    def test_missing_sample_file_aborts_without_changes(self):
        self._save_good((0,))
        self.storage.save_sector(1, b"\x55" * 512)
        before = self.storage.get_state()
        os.unlink(os.path.join(self.tmp, "sample.img"))
        with self.assertRaises(SampleUnreadableError):
            self.storage.verify_saved_sectors()
        self.assertEqual(self.storage.get_state(), before)
        self.assertEqual(self.storage.read_saved_sector(1), b"\x55" * 512)

    def test_truncated_sample_aborts_without_changes(self):
        self._save_good((0,))
        self.storage.save_sector(1, b"\x66" * 512)
        before = self.storage.get_state()
        sample_path = os.path.join(self.tmp, "sample.img")
        with open(sample_path, "rb") as fh:
            data = fh.read()
        with open(sample_path, "wb") as fh:
            fh.write(data[:-512])
        with self.assertRaises(SampleUnreadableError):
            self.storage.verify_saved_sectors()
        self.assertEqual(self.storage.get_state(), before)

    def test_replaced_sample_aborts_without_changes(self):
        self._save_good((0,))
        self.storage.save_sector(1, b"\x77" * 512)
        before = self.storage.get_state()
        sample_path = os.path.join(self.tmp, "sample.img")
        with open(sample_path, "r+b") as fh:
            fh.seek(0)
            fh.write(b"\xff")
        with self.assertRaises(SampleUnreadableError):
            self.storage.verify_saved_sectors()
        self.assertEqual(self.storage.get_state(), before)

    def test_job_replaced_during_verification_discards_result(self):
        self._save_good((0, 2))
        self.storage.save_sector(1, b"\x88" * 512)
        new_image = self.make_image(4)
        original_scan = self.storage._scan_saved_vs_sample

        def hooked_scan(start_saved):
            result = original_scan(start_saved)
            self.storage.reset()
            self.storage.create_job(io.BytesIO(new_image))
            return result

        self.storage._scan_saved_vs_sample = hooked_scan
        with self.assertRaises(JobChangedError):
            self.storage.verify_saved_sectors()

        # 新任务进度不受上一轮核对影响
        state = self.storage.get_state()
        self.assertEqual(state["sector_count"], 4)
        self.assertEqual(state["saved"], [])
        self.assertEqual(state["missing"], [0, 1, 2, 3])

    def test_concurrent_verification_rejected(self):
        self._save_good((0,))
        self.storage._verify_gate.acquire()
        try:
            with self.assertRaises(VerificationBusyError):
                self.storage.verify_saved_sectors()
        finally:
            self.storage._verify_gate.release()

    def test_snapshot_before_retraction_keeps_old_bytes_and_list(self):
        self._save_good((0, 2))
        bad = b"\x99" * 512
        self.storage.save_sector(1, bad)
        sid = self.storage.create_snapshot()["snapshot_id"]
        old_image = b"".join(self.storage.open_snapshot_image(sid))
        self.assertEqual(old_image[512:1024], bad)  # 旧快照保留原（错误）字节
        self.assertEqual(
            self.storage.snapshot_missing_list(sid)["missing"],
            [3, 4, 5, 6, 7, 8, 9],
        )

        self.storage.verify_saved_sectors()
        # 旧快照仍可下载，内容与缺失清单固定
        self.assertEqual(b"".join(self.storage.open_snapshot_image(sid)), old_image)
        self.assertEqual(
            self.storage.snapshot_missing_list(sid)["missing"],
            [3, 4, 5, 6, 7, 8, 9],
        )

        sid2 = self.storage.create_snapshot()["snapshot_id"]
        self.assertEqual(
            self.storage.snapshot_missing_list(sid2)["missing"],
            [1, 3, 4, 5, 6, 7, 8, 9],
        )
        new_image = b"".join(self.storage.open_snapshot_image(sid2))
        self.assertEqual(new_image[512:1024], b"\x00" * 512)  # 退回后补零


if __name__ == "__main__":
    unittest.main(verbosity=2)
