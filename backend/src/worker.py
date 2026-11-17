import time,json,traceback
from pathlib import Path
from sqlalchemy import select
from .config.database import Session
from .models.trial import Trial
from .models.merge_drill import MergeDrill
from .services.trial import queue
from .services import merge_drill as drill_svc
from .services.lsm_engine import simulate
heartbeat=Path('/artifacts/worker.heartbeat')
def run_one(ident):
    with Session() as db:
        x=db.get(Trial,ident)
        if not x or x.result is not None:return
        x.status='running';db.commit()
        try:
            x.result=simulate(x.input_snapshot['entries'],x.input_snapshot['config']);x.status='passed'
        except Exception as exc:x.result={'error':str(exc)[:500]};x.status='failed'
        db.commit();Path(f'/artifacts/lsm-trial-{ident}.json').write_text(json.dumps(x.result,ensure_ascii=False,indent=2))
def run_drill(ident):
    # 在线合并演练：worker 只负责把 pending/preparing 预构到 ready，切换由页面显式触发
    with Session() as db:
        x=db.get(MergeDrill,ident)
        if not x:return
        if x.state.get('drill') and x.state['drill']['status'] in ('pending','preparing'):
            drill_svc.prepare_job(db,ident)
            Path(f'/artifacts/merge-drill-{ident}.json').write_text(json.dumps(x.state,ensure_ascii=False,indent=2))
def recover_all():
    with Session() as db:
        # 恢复中断的整批试验与在线演练
        pending=list(db.scalars(select(Trial.id).where(Trial.status.in_(['queued','running']))))
        for ident in pending:run_one(ident)
        drill_svc.recover_pending(db)
while True:
    try:
        heartbeat.write_text(str(time.time()))
        item=queue.blpop(['lsm:jobs','merge:jobs'],timeout=2)
        if item:
            key,raw=item
            # redis-py 默认返回 bytes，统一解码后再分发
            key=key.decode() if isinstance(key,bytes) else key
            if key=='merge:jobs':run_drill(int(raw))
            else:run_one(int(raw))
        else:
            recover_all()
    except Exception:traceback.print_exc();time.sleep(2)
