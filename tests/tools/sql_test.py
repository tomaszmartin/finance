import pytest
import sqlite3
from app.tools import sql


@pytest.fixture
def db_session():
    connection = sqlite3.connect(":memory:")
    db_session = connection.cursor()
    yield db_session
    connection.close()


def insert(db_sess, table: str, values: tuple):
    db_sess.execute(f"INSERT INTO {table} VALUES{values}")


def test_sql_not_distinct(db_session):
    db_session.execute("CREATE TABLE test (key text, value int)")
    insert(db_session, "test", ("key1", 10))
    insert(db_session, "test", ("key1", 20))
    cur = db_session.execute(sql.distinct("key", "test"))
    assert not cur.fetchone()[0]


def test_sql_distinct(db_session):
    db_session.execute("CREATE TABLE test (key text, value int)")
    insert(db_session, "test", ("key1", 10))
    insert(db_session, "test", ("key2", 20))
    cur = db_session.execute(sql.distinct("key", "test"))
    assert cur.fetchone()[0]


def test_sql_distinct_in_group(db_session):
    db_session.execute("CREATE TABLE test (key text, grouping text, value int)")
    insert(db_session, "test", ("key1", "g1", 10))
    insert(db_session, "test", ("key1", "g2", 10))
    cur = db_session.execute(sql.distinct("key", "test", groupby="grouping"))
    assert cur.fetchone()[0]