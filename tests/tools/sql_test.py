from hypothesis import given, settings, HealthCheck, strategies as sn
from app.tools import sql


def insert(db_sess, table: str, values: tuple):
    db_sess.execute(f"INSERT INTO `{table}` VALUES{values}")


@given(chars=sn.from_regex(r"[AZaz09]+", fullmatch=True))
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_sql_not_distinct(chars, db_session):
    query = sql.is_distinct("key", "test")
    db_session.execute("CREATE TABLE `test` (key text, value int);")
    insert(db_session, "test", (chars, 10))
    insert(db_session, "test", (chars, 20))
    cur = db_session.execute(query)
    is_distinct = cur.fetchone()[0]
    assert not is_distinct
    db_session.execute("DROP TABLE 'test';")


def test_sql_distinct(db_session):
    db_session.execute("CREATE TABLE test (key text, value int)")
    insert(db_session, "test", ("key1", 10))
    insert(db_session, "test", ("key2", 20))
    cur = db_session.execute(sql.is_distinct("key", "test"))
    is_distinct = cur.fetchone()[0]
    assert is_distinct


def test_sql_distinct_with_where(db_session):
    db_session.execute("CREATE TABLE test (key text, value int)")
    insert(db_session, "test", ("key1", 10))
    insert(db_session, "test", ("key2", 20))
    cur = db_session.execute(sql.is_distinct("key", "test", where="value=10"))
    is_distinct = cur.fetchone()[0]
    assert is_distinct


def test_sql_distinct_in_group(db_session):
    db_session.execute("CREATE TABLE test (key text, grouping text, value int)")
    insert(db_session, "test", ("key1", "g1", 10))
    insert(db_session, "test", ("key1", "g2", 10))
    cur = db_session.execute(sql.is_distinct("key", "test", groupby="grouping"))
    is_distinct = cur.fetchone()[0]
    assert is_distinct
