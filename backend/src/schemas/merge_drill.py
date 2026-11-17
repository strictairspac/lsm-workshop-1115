from typing import Literal
from pydantic import BaseModel, Field
from .dataset import Entry


class DrillCreate(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    config_id: int = Field(gt=0)


class DrillWrite(BaseModel):
    op: Literal['put', 'delete']
    key: str = Field(min_length=1, max_length=64)
    value: str | None = Field(default=None, max_length=200)


class DrillBulkWrite(BaseModel):
    entries: list[Entry] = Field(min_length=1, max_length=2000)


class DrillStart(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    selected_table_ids: list[int] = Field(min_length=1)
    mode: Literal['auto', 'tiered', 'full'] = 'auto'
    drop_tombstones: bool = True


class DrillKeyRead(BaseModel):
    key: str = Field(min_length=1, max_length=64)
    snapshot_id: int | None = None
