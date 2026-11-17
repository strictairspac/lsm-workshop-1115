from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, JSON, ForeignKey, DateTime, Text, func
from ..config.database import Base


class MergeDrill(Base):
    """可持续写入的在线合并演练：引擎状态整存在 state JSON 字段，可中断恢复。"""
    __tablename__ = 'merge_drills'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    config_id: Mapped[int] = mapped_column(ForeignKey('configs.id'))
    status: Mapped[str] = mapped_column(String(20), default='idle')
    # MergeEngine 的完整可序列化状态（层级/快照/归档/演练/计数器/时间线/GC 依据）
    state: Mapped[dict] = mapped_column(JSON)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now())
