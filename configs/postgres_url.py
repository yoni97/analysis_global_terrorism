from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from base.base import Base
from configs.database_url import DATABASE_URL

engine = create_engine(DATABASE_URL)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

def init_db():
    Base.metadata.create_all(bind=engine)