from innerbloom.src.core.database import Database
from os.path import isfile


def test_create():
    db = Database("test")
    assert isfile("db/test.db") is True
    db.delete()


def test_delete():
    db = Database("test")
    db.delete()
    assert isfile("db/test.db") is False


def test_connect():
    db = Database("test")
    assert db.connection is not None
    assert db.cursor is not None
    db.close()
    db.delete()


def test_disconnect():
    db = Database("test")
    db.connect()
    db.close()
    assert db.connection is None
    assert db.cursor is None
    db.delete()


def test_create_table():
    db = Database("test")
    db.create_table("test", ["id INTEGER PRIMARY KEY", "name TEXT"])
    assert db.check_table("test") == ("test",)
    db.delete()


def test_drop_table():
    db = Database("test")
    db.create_table("test", ["id INTEGER PRIMARY KEY", "name TEXT"])
    db.drop_table("test")
    assert db.check_table("test") is None
    db.delete()


def test_insert_table():
    db = Database("test")
    db.create_table("test", ["id INTEGER PRIMARY KEY", "name TEXT"])
    db.push("INSERT INTO test (name) VALUES ('foo')")
    assert db.select("test", ["name"]) == ("foo",)
    db.delete()


def test_fetch_one():
    db = Database("test")
    db.create_table("test", ["id INTEGER PRIMARY KEY", "name TEXT"])
    db.push("INSERT INTO test (name) VALUES ('foo')")
    assert db.select("test", ["name"], fetch_option="one") == ("foo",)
    db.delete()


def test_fetch_multiple():
    db = Database("test")
    db.create_table("test", ["id INTEGER PRIMARY KEY", "name TEXT"])
    db.push("INSERT INTO test (name) VALUES ('foo')")
    db.push("INSERT INTO test (name) VALUES ('bar')")
    assert db.select("test", ["name"], fetch_option="all") == [("foo",), ("bar",)]
    db.delete()
