from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite:///./rajnandini.db"
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "check_same_thread": False,
        "timeout": 30,                  # wait up to 30s for a lock instead of failing
    },
    pool_size=10,                       # keep 10 connections ready
    max_overflow=20,                    # allow 20 more under peak load
    pool_pre_ping=True,                 # discard stale connections automatically
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
