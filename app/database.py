# app/database.py
import json
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Text, func, select
from app import APP_CONFIG

# --- 数据库引擎和会话设置 ---
engine = create_async_engine(APP_CONFIG['SQLALCHEMY_DATABASE_URI'])
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# --- ORM 模型基类 ---
class Base(DeclarativeBase):
    pass

# --- ORM 模型定义 ---
class Shares(Base):
    __tablename__ = 'shares'
    share_url: Mapped[str] = mapped_column(String, primary_key=True)
    share_code: Mapped[str] = mapped_column(String, nullable=True)
    receive_code: Mapped[str] = mapped_column(String, nullable=True)
    share_title: Mapped[str] = mapped_column(String, nullable=True)
    timestamp: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[int] = mapped_column(Integer, default=0, index=True)
    path_hierarchy: Mapped[str] = mapped_column(Text, nullable=True)
    shared_cids: Mapped[str] = mapped_column(Text, nullable=True)

# --- 数据库会话上下文管理器 ---
@asynccontextmanager
async def get_session():
    """提供一个数据库会话，并自动处理提交、回滚和关闭"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

# --- 数据库初始化与迁移 ---
async def init_db():
    """初始化数据库，创建表结构"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("[DB] 数据库初始化完成。")