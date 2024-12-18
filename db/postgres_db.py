from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from base.base import Base
from configs.postgres_url import connection_url

engine = create_engine(connection_url)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

def init_db():
    Base.metadata.create_all(bind=engine)