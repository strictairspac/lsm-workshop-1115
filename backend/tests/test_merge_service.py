"""服务层持久化语义验证（零第三方依赖）。

环境无 SQLAlchemy/MySQL，这里用标准库 sqlite3 + 最小伪会话模拟 MergeDrill 表的
JSON 整存与行锁，直接驱动 services/merge_drill.py 的纯逻辑分支，验证：
- 写入/快照/合并提交的持久化往返；
- 合并预构后继续写入不被吞、快照隔离；
- 切换前中断恢复、重试写放大只计一次；
- 释放快照后归档清理与 full 表墓碑重写依据。
"""
import json
import os
import sqlite3
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class FakeQuery:
    def __init__(self, store):
        self.store = store

    def scalar(self):
        return None  # 名称唯一校验默认通过


class FakeSession:
    """最小会话：支持 get(行锁)、add、commit、scalar 查询，状态落 SQLite。

    模拟 SQLAlchemy 的脏检查：所有 get 加载的实例在 commit 时统一 UPDATE 回库。
    """
    def __init__(self, conn):
        self.conn = conn
        self.added = []
        self.loaded = {}

    def get(self, model, ident, with_for_update=False):
        row = self.conn.execute(
            'SELECT name, config_id, status, state, last_error FROM merge_drills WHERE id=?',
            (ident,)).fetchone()
        if not row:
            return None
        x = types.SimpleNamespace()
        x.id = ident
        x.name, x.config_id, x.status, state_raw, x.last_error = row
        x.state = json.loads(state_raw)
        x.created_at = 'now'
        x.updated_at = 'now'
        self.loaded[ident] = x
        return x

    def add(self, x):
        self.added.append(x)

    def scalar(self, stmt):
        return None

    def scalars(self, stmt):
        rows = self.conn.execute(
            'SELECT id FROM merge_drills ORDER BY id').fetchall()
        # 模拟 SQLAlchemy scalars().all() 返回模型实例
        objects = []
        for (ident,) in rows:
            if ident not in self.loaded:
                self.get(None, ident)
            objects.append(self.loaded[ident])
        return _Scalar(objects)

    def commit(self):
        for x in self.added:
            self.conn.execute(
                'INSERT INTO merge_drills(name, config_id, status, state, last_error) VALUES(?,?,?,?,?)',
                (x.name, x.config_id, x.status, json.dumps(x.state, ensure_ascii=False), None))
        self.added = []
        for ident, x in self.loaded.items():
            self.conn.execute(
                'UPDATE merge_drills SET status=?, state=?, last_error=? WHERE id=?',
                (x.status, json.dumps(x.state, ensure_ascii=False), x.last_error, ident))
        self.conn.commit()
        self.loaded = {}

    def refresh(self, x):
        cur = self.conn.execute('SELECT id FROM merge_drills WHERE name=?', (x.name,))
        x.id = cur.fetchone()[0]


class _Scalar:
    def __init__(self, items):
        self.items = items

    def all(self):
        return list(self.items)

    def first(self):
        return self.items[0] if self.items else None


class FakeRedis:
    def __init__(self):
        self.jobs = []

    def rpush(self, key, value):
        self.jobs.append((key, value))

    def blpop(self, *a, **k):
        return None


def make_db():
    conn = sqlite3.connect(':memory:')
    conn.execute(
        'CREATE TABLE merge_drills(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, config_id INT,'
        ' status TEXT, state TEXT, last_error TEXT)')
    conn.commit()
    return conn


def load_state(conn, ident):
    raw = conn.execute('SELECT state FROM merge_drills WHERE id=?', (ident,)).fetchone()[0]
    return json.loads(raw)


# 在导入服务模块前注入第三方桩模块，避免依赖 fastapi/pydantic/sqlalchemy/redis
fake_redis = FakeRedis()
redis_mod = types.ModuleType('redis')
redis_mod.Redis = types.SimpleNamespace(from_url=lambda url: fake_redis)
sys.modules['redis'] = redis_mod

fastapi_mod = types.ModuleType('fastapi')


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


fastapi_mod.HTTPException = HTTPException
fastapi_mod.APIRouter = object
fastapi_mod.Depends = lambda *a, **k: None
fastapi_mod.Response = object
sys.modules['fastapi'] = fastapi_mod

class _Select:
    def where(self, *a, **k):
        return self

    def order_by(self, *a, **k):
        return self


sqlalchemy_mod = types.ModuleType('sqlalchemy')
sqlalchemy_mod.select = lambda *a, **k: _Select()
sys.modules['sqlalchemy'] = sqlalchemy_mod

# settings 桩：merge_drill 模块只在 import 时取 REDIS_URL
settings_mod = types.ModuleType('src.config.settings')
settings_mod.REDIS_URL = 'redis://stub'
settings_mod.DATABASE_URL = 'sqlite://'
settings_mod.JWT_SECRET = 'stub'
sys.modules['src.config.settings'] = settings_mod

# 模型桩：MergeDrill 需支持关键字构造；类属性访问返回占位表达式
class _StubMeta(type):
    def __getattr__(cls, item):
        return _Select()


class _MergeDrillStub(metaclass=_StubMeta):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.last_error = None
        self.created_at = 'now'
        self.updated_at = 'now'


m = types.ModuleType('src.models.merge_drill')
m.MergeDrill = _MergeDrillStub
sys.modules['src.models.merge_drill'] = m

m2 = types.ModuleType('src.models.config')
m2.Config = _StubMeta('Config', (), {})
sys.modules['src.models.config'] = m2

# config 服务桩：serialize
config_svc = types.ModuleType('src.services.config')
config_svc.serialize = lambda c: vars(c)
sys.modules['src.services.config'] = config_svc

from src.services import merge_drill as svc  # noqa: E402
from src.services.merge_engine import MergeEngine  # noqa: E402


CONFIG = {'memtable_limit': 2, 'l0_trigger': 2, 'max_levels': 3, 'final_compaction': True}


def entry(op, key, value=None):
    ns = types.SimpleNamespace(op=op, key=key, value=value)
    ns.model_dump = lambda: {'op': op, 'key': key, 'value': value}
    return ns


class ServicePersistenceTests(unittest.TestCase):
    def setUp(self):
        self.conn = make_db()
        fake_redis.jobs = []

    def session(self):
        return FakeSession(self.conn)

    def create_drill(self, name='d'):
        db = self.session()
        data = types.SimpleNamespace(name=name, config_id=1)
        # service 会先 db.get(Config) 再 config_dict；伪会话返回带配置字段的对象
        fake_config = types.SimpleNamespace(**CONFIG)
        with mock.patch.object(db, 'get', side_effect=lambda model, ident, **k: fake_config):
            x = svc.create(db, data)
        return x['id']

    def test_full_lifecycle_persists_and_concurrent_writes_survive(self):
        ident = self.create_drill()
        db = self.session()
        svc.bulk_write(db, ident, types.SimpleNamespace(entries=[
            entry('put', 'a', '1'),
            entry('put', 'b', '2'),
            entry('put', 'c', '3'),
            entry('put', 'd', '4')]))
        # 创建快照
        r = svc.create_snapshot(db, ident)
        snap_id = r['snapshot']['id']
        # 发起 tiered 合并并入队
        state = load_state(self.conn, ident)
        l0 = [t['id'] for t in state['levels'][0]]
        svc.start_drill(db, ident, types.SimpleNamespace(
            name='m1', selected_table_ids=l0, mode='tiered', drop_tombstones=True))
        self.assertTrue(('merge:jobs', ident) in fake_redis.jobs)
        # worker 预构
        svc.prepare_job(self.session(), ident)
        self.assertEqual(load_state(self.conn, ident)['drill']['status'], 'ready')
        # 预构后继续写入：新写入必须保留
        svc.bulk_write(self.session(), ident, types.SimpleNamespace(entries=[
            entry('put', 'e', '5'),
            entry('put', 'a', '99')]))
        # 整批切换
        out = svc.apply_drill(self.session(), ident)
        eng = MergeEngine(load_state(self.conn, ident))
        self.assertEqual(eng.point_read('a')['value'], '99')
        self.assertEqual(eng.point_read('e')['value'], '5')
        # 快照仍读创建时刻的旧值 a=1
        self.assertEqual(eng.snapshot_read(snap_id, 'a')['value'], '1')
        self.assertTrue(eng.checksums()['actual_matches_oracle'])
        # 写放大只计一次合并
        self.assertEqual(eng.s['counters']['compactions'], 1)

    def test_interrupt_before_switch_recovers_and_retry_idempotent(self):
        ident = self.create_drill()
        db = self.session()
        svc.bulk_write(db, ident, types.SimpleNamespace(entries=[
            entry('put', 'a', '1'),
            entry('put', 'b', '2')]))
        state = load_state(self.conn, ident)
        l0 = [t['id'] for t in state['levels'][0]]
        svc.start_drill(db, ident, types.SimpleNamespace(
            name='m', selected_table_ids=l0, mode='tiered', drop_tombstones=True))
        svc.prepare_job(self.session(), ident)
        # 模拟进程在 ready 前崩溃到 preparing：直接改持久化状态
        st = load_state(self.conn, ident)
        st['drill']['status'] = 'preparing'
        st['drill']['shadow'] = None
        self.conn.execute('UPDATE merge_drills SET state=? WHERE id=?',
                          (json.dumps(st), ident))
        self.conn.commit()
        # worker 恢复
        recovered = svc.recover_pending(self.session())
        self.assertIn(ident, recovered)
        # 重新预构不计数
        svc.prepare_job(self.session(), ident)
        eng0 = MergeEngine(load_state(self.conn, ident))
        self.assertEqual(eng0.s['counters']['compaction_writes'], 0)
        # 切换，计一次
        svc.apply_drill(self.session(), ident)
        eng = MergeEngine(load_state(self.conn, ident))
        self.assertEqual(eng.s['counters']['compactions'], 1)
        expected = round((eng.s['counters']['flush_writes']
                          + eng.s['counters']['compaction_writes'])
                         / max(eng.s['user_ops'], 1), 4)
        self.assertEqual(eng.write_amplification(), expected)

    def test_snapshot_release_clears_archive_and_records_gc_basis(self):
        ident = self.create_drill()
        db = self.session()
        svc.bulk_write(db, ident, types.SimpleNamespace(entries=[
            entry('put', 'a', '1'),
            entry('delete', 'a', None),
            entry('put', 'b', '2'),
            entry('put', 'c', '3')]))
        r = svc.create_snapshot(db, ident)
        snap_id = r['snapshot']['id']
        state = load_state(self.conn, ident)
        all_ids = [t['id'] for lv in state['levels'] for t in lv]
        svc.start_drill(db, ident, types.SimpleNamespace(
            name='full', selected_table_ids=all_ids, mode='full', drop_tombstones=True))
        svc.prepare_job(self.session(), ident)
        svc.apply_drill(self.session(), ident)
        eng = MergeEngine(load_state(self.conn, ident))
        # 有快照：墓碑保留，full 表含墓碑
        self.assertTrue(any(r2['deleted'] for t in eng.s['levels'][-1]
                            for r2 in t['rows'].values()))
        # 释放前 GC 依据为 retain
        gc_before = svc.gc_evaluate(self.session(), ident)
        self.assertTrue(all(g['decision'] in ('retain', 'reclaimable') for g in gc_before))
        # 释放快照：触发重写清墓碑，a 不复活
        svc.release_snapshot(self.session(), ident, snap_id)
        eng2 = MergeEngine(load_state(self.conn, ident))
        self.assertFalse(eng2.point_read('a')['found'])
        self.assertGreater(eng2.s['counters']['gc_rewrite_writes'], 0)
        # GC 重写不计入合并写放大
        self.assertEqual(eng2.s['counters']['compactions'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
