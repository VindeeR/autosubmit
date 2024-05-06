import os
from sqlalchemy import Engine, NullPool, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from autosubmitconfigparser.config.basicconfig import BasicConfig


def create_sqlite_engine(path: str = "") -> Engine:
    if path:
        return create_engine(f"sqlite:///{os.path.abspath(path)}", poolclass=NullPool)
    # Else return memory database
    return create_engine("sqlite://", poolclass=NullPool)


BasicConfig.read()
if BasicConfig.DATABASE_BACKEND == "postgres":
    engine = create_engine(BasicConfig.DATABASE_CONN_URL)
else:
    engine = create_sqlite_engine()  # Placeholder sqlite engine

session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)
