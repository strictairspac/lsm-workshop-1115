"""端到端验收流程：单测之上，按用户验收点串成一条完整操作流。

运行：python3 -m tests.test_acceptance_flow
覆盖：跨层旧值不复活 → 快照创建/释放与原值可读 → 合并期间写入不丢 → 切换前中断恢复。
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.merge_engine import MergeEngine, new_state  # noqa: E402


class AcceptanceFlowTests(unittest.TestCase):
    def test_full_acceptance_flow(self):
        eng = MergeEngine(new_state({
            'memtable_limit': 2, 'l0_trigger': 2,
            'max_levels': 3, 'final_compaction': True}))

        # —— 场景一：跨层旧值。a 旧值入表后再写墓碑，b 多次覆盖 ——
        for op, k, v in [('put', 'a', 'old'), ('put', 'b', 'b1'),
                         ('put', 'c', 'c1'), ('delete', 'a', None),
                         ('put', 'b', 'b2'), ('put', 'd', 'd1')]:
            eng.write(op, k, v)
        self.assertFalse(eng.point_read('a')['found'])
        self.assertEqual(eng.point_read('b')['value'], 'b2')

        # —— 场景二：创建快照，随后覆盖/删除，快照仍读原值 ——
        snap = eng.create_snapshot()
        eng.write('put', 'b', 'b3')
        eng.write('delete', 'c', None)
        self.assertEqual(eng.snapshot_read(snap['id'], 'a')['found'], False)
        self.assertEqual(eng.snapshot_read(snap['id'], 'b')['value'], 'b2')
        self.assertEqual(eng.snapshot_read(snap['id'], 'c')['value'], 'c1')

        # tiered 合并 L0 -> L1，墓碑保留，旧表因快照引用归档
        l0 = [t['id'] for t in eng.s['levels'][0]]
        eng.start_drill('tiered-1', l0, mode='tiered')
        eng.prepare_drill()
        wa_before_apply = eng.write_amplification()
        eng.apply_drill()
        self.assertGreaterEqual(eng.write_amplification(), wa_before_apply)
        # 旧表因仍被快照引用而归档（而非清理）
        self.assertTrue(eng.s['archive'])
        # 快照仍可读
        self.assertEqual(eng.snapshot_read(snap['id'], 'b')['value'], 'b2')

        # —— 场景三：合并期间（ready 后 apply 前）继续写入与建快照，新写入不丢 ——
        eng.write('put', 'e', 'e1')
        eng.write('put', 'f', 'f1')  # 刷出新 L0
        snap2 = eng.create_snapshot()
        all_ids = [t['id'] for lv in eng.s['levels'] for t in lv]
        eng.start_drill('full-1', all_ids, mode='full', drop_tombstones=True)
        # 注意：有存续快照，full 不会真正清墓碑
        self.assertFalse(eng.s['drill']['drop_allowed'])
        eng.prepare_drill()
        # 预构后再写入
        eng.write('put', 'g', 'g1')
        eng.write('put', 'b', 'b-final')
        eng.apply_drill()
        self.assertEqual(eng.point_read('b')['value'], 'b-final')
        self.assertEqual(eng.point_read('g')['value'], 'g1')
        # 已删键不复活
        self.assertFalse(eng.point_read('a')['found'])
        self.assertFalse(eng.point_read('c')['found'])
        # 两个快照都仍读创建时刻的值
        self.assertEqual(eng.snapshot_read(snap['id'], 'b')['value'], 'b2')
        self.assertEqual(eng.snapshot_read(snap2['id'], 'b')['value'], 'b3')

        # —— 场景四：释放快照后，归档与墓碑可清理；再验证切换前中断恢复 ——
        eng.release_snapshot(snap['id'])
        eng.release_snapshot(snap2['id'])
        self.assertEqual(eng.s['archive'], [])
        # full 表墓碑在无快照后被重写清理，但已删键依旧不复活
        self.assertFalse(eng.point_read('a')['found'])
        self.assertFalse(eng.point_read('c')['found'])
        self.assertTrue(eng.checksums()['actual_matches_oracle'])

        # 切换前中断：新演练卡在 preparing，recover 回 pending，重试不重复计数
        eng.write('put', 'h', 'h1')
        eng.write('put', 'i', 'i1')
        ids = [t['id'] for t in eng.s['levels'][0]]
        if ids:
            eng.start_drill('interrupted', ids, mode='tiered')
            eng.prepare_drill()
            comp_writes = eng.s['counters']['compaction_writes']
            eng.s['drill']['status'] = 'preparing'
            eng.s['drill']['shadow'] = None
            recovered = eng.recover()
            self.assertEqual(recovered['status'], 'pending')
            eng.prepare_drill()
            # 重试预构不产生新的合并写放大
            self.assertEqual(eng.s['counters']['compaction_writes'], comp_writes)
            output = eng.s['drill']['output_entries']
            eng.apply_drill()
            # 切换只计一次合并输出
            self.assertEqual(eng.s['counters']['compaction_writes'], comp_writes + output)

        # 最终：序列化往返后状态与校验完整
        restored = MergeEngine(json.loads(json.dumps(eng.s)))
        self.assertTrue(restored.checksums()['actual_matches_oracle'])
        self.assertEqual(restored.current_view(), eng.current_view())

        # GC 依据全部可核对
        for row in restored.gc_evaluate():
            self.assertIn(row['decision'], ('retain', 'reclaimable'))
            self.assertTrue(row['reason'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
