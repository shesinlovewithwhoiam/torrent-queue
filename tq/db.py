
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

def make_sessionmaker(db_url):
    engine = create_engine(f'sqlite:///{db_url}', echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    return sessionmaker(bind=engine, expire_on_commit=False), engine