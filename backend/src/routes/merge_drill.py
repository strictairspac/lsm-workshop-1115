import json
from fastapi import APIRouter, Depends, Response, HTTPException
from ..config.database import get_db
from ..schemas.merge_drill import (DrillCreate, DrillWrite, DrillBulkWrite,
                                   DrillStart, DrillKeyRead)
from ..services.auth import current_user, writer
from ..services import merge_drill as svc
from ..models.merge_drill import MergeDrill

router = APIRouter(prefix='/merge-drills')


@router.get('')
def get(db=Depends(get_db), user=Depends(current_user)):
    return svc.listing(db)


@router.post('', status_code=201)
def add(data: DrillCreate, db=Depends(get_db), user=Depends(writer)):
    return svc.create(db, data)


@router.get('/{ident}')
def one(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return svc.detail(db, ident)


@router.post('/{ident}/writes', status_code=200)
def write(ident: int, data: DrillWrite, db=Depends(get_db), user=Depends(writer)):
    return svc.write_one(db, ident, data)


@router.post('/{ident}/bulk-writes', status_code=200)
def bulk_write(ident: int, data: DrillBulkWrite, db=Depends(get_db), user=Depends(writer)):
    return svc.bulk_write(db, ident, data)


@router.post('/{ident}/snapshots', status_code=201)
def snapshot(ident: int, db=Depends(get_db), user=Depends(writer)):
    return svc.create_snapshot(db, ident)


@router.delete('/{ident}/snapshots/{snapshot_id}', status_code=200)
def release(ident: int, snapshot_id: int, db=Depends(get_db), user=Depends(writer)):
    return svc.release_snapshot(db, ident, snapshot_id)


@router.post('/{ident}/read')
def read_key(ident: int, data: DrillKeyRead, db=Depends(get_db), user=Depends(current_user)):
    return svc.read_key(db, ident, data.key, data.snapshot_id)


@router.post('/{ident}/start', status_code=202)
def start(ident: int, data: DrillStart, db=Depends(get_db), user=Depends(writer)):
    return svc.start_drill(db, ident, data)


@router.post('/{ident}/apply', status_code=200)
def apply(ident: int, db=Depends(get_db), user=Depends(writer)):
    return svc.apply_drill(db, ident)


@router.get('/{ident}/gc')
def gc(ident: int, db=Depends(get_db), user=Depends(current_user)):
    return svc.gc_evaluate(db, ident)


@router.get('/{ident}/export')
def export(ident: int, db=Depends(get_db), user=Depends(current_user)):
    x = db.get(MergeDrill, ident)
    if not x:
        raise HTTPException(404, '合并演练不存在')
    return Response(
        json.dumps(svc.full_detail(x), ensure_ascii=False, indent=2),
        media_type='application/json',
        headers={'Content-Disposition': f'attachment; filename="lsm-merge-drill-{ident}.json"'})
