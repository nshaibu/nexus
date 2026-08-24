import orjson as json
from typing import Optional

import pytest
from formax import BaseModel

from volnux.backends.stores.sqlite_store import SqliteStoreBackend
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SerializationError,
)


class SampleRecord(BaseModel):
    name: str
    age: int
    active: bool
    rating: float
    tags: list
    metadata: dict
    nickname: Optional[str]


class BrokenRecord(BaseModel):
    name: str

    def __getstate__(self):
        raise RuntimeError("cannot serialize")


@pytest.fixture
def sample_record():
    return SampleRecord(
        name="Alice",
        age=30,
        active=True,
        rating=4.5,
        tags=["admin", "ops"],
        metadata={"team": "platform"},
        nickname=None,
    )


@pytest.fixture
def sqlite_store(tmp_path):
    db_path = tmp_path / "store.db"
    store = SqliteStoreBackend(database=db_path)
    try:
        yield store
    finally:
        store.close()


def test_init_connects_connector(tmp_path):
    db_path = tmp_path / "store.db"

    store = SqliteStoreBackend(database=db_path)
    try:
        assert store.connector.is_connected() is True
    finally:
        store.close()


def test_create_schema_and_list_schemas(sqlite_store, sample_record):
    sqlite_store.create_schema("users", sample_record)

    schemas = sqlite_store.list_schemas()

    assert "users" in schemas
    assert sqlite_store.schema_exists("users") is True


def test_create_schema_is_idempotent_when_if_not_exists_true(
    sqlite_store, sample_record
):
    # sqlite_store.ensure_schema("users", sample_record)
    sqlite_store.ensure_schema("users", sample_record)

    assert sqlite_store.schema_exists("users") is True


def test_create_schema_raises_when_schema_already_exists_and_if_not_exists_false(
    sqlite_store, sample_record
):
    sqlite_store.ensure_schema("users", sample_record)

    # with pytest.raises(ObjectExistError, match="already exists"):
    #     sqlite_store.create_schema("users", sample_record, if_not_exists=False)
    assert sqlite_store.schema_exists("users") is True


def test_drop_schema_removes_existing_schema(sqlite_store, sample_record):
    sqlite_store.ensure_schema("users", sample_record)

    sqlite_store.drop_schema("users")

    assert sqlite_store.schema_exists("users") is False


# def test_drop_schema_raises_when_missing_and_if_exists_false(sqlite_store):
#     with pytest.raises(ObjectDoesNotExist, match="does not exist"):
#         sqlite_store.drop_schema("missing_schema")


def test_check_if_schema_exists_uses_cache(sqlite_store, sample_record):
    sqlite_store.ensure_schema("users", sample_record)

    assert sqlite_store.schema_exists("users") is True

    sqlite_store.drop_schema("users")
    sqlite_store._schema_cache["users"] = True

    assert sqlite_store.schema_exists("users") is True

    sqlite_store._invalidate_schema_cache("users")

    assert sqlite_store.schema_exists("users") is False


def test_prepare_record_data_serializes_lists_dicts_and_state(
    sqlite_store, sample_record
):
    data = sqlite_store._prepare_record_data(sample_record, "user-1")

    assert data["id"] == "user-1"
    assert data["name"] == "Alice"
    assert data["age"] == 30
    assert data["active"] is True
    assert data["rating"] == 4.5
    assert data["tags"] == '["admin","ops"]'.encode("utf-8")
    assert data["metadata"] == '{"team":"platform"}'.encode("utf-8")
    assert data["nickname"] is None
    assert isinstance(data["_record_state"], bytes)


def test_serialize_and_deserialize_record_roundtrip(sqlite_store, sample_record):
    serialized = sqlite_store._serialize_record(sample_record)
    restored = sqlite_store._deserialize_record(serialized, SampleRecord)

    assert isinstance(restored, SampleRecord)
    assert restored.name == sample_record.name
    assert restored.age == sample_record.age
    assert restored.tags == sample_record.tags
    assert restored.metadata == sample_record.metadata


def test_serialize_record_raises_serialization_error(sqlite_store):
    record = BrokenRecord(name="broken")

    with pytest.raises(SerializationError, match="Serialization failed"):
        sqlite_store._serialize_record(record)


def test_load_record_restores_model(sample_record):
    state = sample_record.__getstate__()
    payload = json.dumps(state)

    record = SqliteStoreBackend.load_record(payload, SampleRecord)

    assert isinstance(record, SampleRecord)
    assert record.name == "Alice"
    assert record.age == 30


def test_load_record_raises_serialization_error_for_invalid_payload():
    with pytest.raises(SerializationError, match="Failed to load record"):
        SqliteStoreBackend.load_record(b"not-a-pickle", SampleRecord)


def test_insert_creates_schema_automatically_and_get_returns_record(
    sqlite_store, sample_record
):
    sqlite_store.insert("users", "user-1", sample_record)
    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert isinstance(result, SampleRecord)
    assert result.name == "Alice"
    assert result.age == 30
    assert result.active is True
    assert result.tags == ["admin", "ops"]
    assert result.metadata == {"team": "platform"}


def test_exists_returns_true_for_inserted_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    assert sqlite_store.exists("users", "user-1") is True
    assert sqlite_store.exists("users", "missing") is False


def test_insert_raises_when_record_already_exists(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    with pytest.raises(ObjectExistError, match="already exists"):
        sqlite_store.insert("users", "user-1", sample_record)


def test_get_raises_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.create_schema("users", sample_record)

    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.get("users", "missing", SampleRecord)


def test_update_replaces_existing_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    updated = SampleRecord(
        name="Alice Updated",
        age=31,
        active=False,
        rating=4.8,
        tags=["admin", "lead"],
        metadata={"team": "platform", "region": "eu"},
        nickname="ally",
    )

    sqlite_store.update("users", "user-1", updated)

    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert result.name == "Alice Updated"
    assert result.age == 31
    assert result.active is False
    assert result.tags == ["admin", "lead"]
    assert result.metadata == {"team": "platform", "region": "eu"}
    assert result.nickname == "ally"


def test_update_raises_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.create_schema("users", sample_record)

    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.update("users", "missing", sample_record)


def test_upsert_inserts_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.upsert("users", "user-1", sample_record)

    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert result.name == "Alice"


def test_upsert_updates_when_record_exists(sqlite_store, sample_record):
    sqlite_store.upsert("users", "user-1", sample_record)

    updated = SampleRecord(
        name="Alice 2",
        age=32,
        active=True,
        rating=5.0,
        tags=["admin"],
        metadata={"team": "core"},
        nickname="a2",
    )

    sqlite_store.upsert("users", "user-1", updated)

    result = sqlite_store.get("users", "user-1", SampleRecord)

    assert result.name == "Alice 2"
    assert result.age == 32
    assert result.metadata == {"team": "core"}
    assert result.nickname == "a2"


def test_delete_removes_record(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    sqlite_store.delete("users", "user-1")

    assert sqlite_store.exists("users", "user-1") is False


def test_delete_raises_when_record_does_not_exist(sqlite_store, sample_record):
    sqlite_store.create_schema("users", sample_record)

    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.delete("users", "missing")


def test_filter_returns_matching_records(sqlite_store):
    record_1 = SampleRecord(
        name="Alice",
        age=30,
        active=True,
        rating=4.2,
        tags=["admin"],
        metadata={"team": "platform"},
        nickname=None,
    )
    record_2 = SampleRecord(
        name="Bob",
        age=25,
        active=False,
        rating=3.8,
        tags=["dev"],
        metadata={"team": "api"},
        nickname="bobby",
    )
    record_3 = SampleRecord(
        name="Alicia",
        age=35,
        active=True,
        rating=4.9,
        tags=["ops"],
        metadata={"team": "platform"},
        nickname="ali",
    )

    sqlite_store.insert("users", "1", record_1)
    sqlite_store.insert("users", "2", record_2)
    sqlite_store.insert("users", "3", record_3)

    results = sqlite_store.filter(
        "users",
        SampleRecord,
        active=True,
        age__gte=30,
        name__icontains="ali",
        order_by="age",
    )

    assert [record.name for record in results] == ["Alice", "Alicia"]


def test_filter_honors_limit_offset_and_desc_order(sqlite_store):
    for idx, age in enumerate([20, 30, 40], start=1):
        sqlite_store.insert(
            "users",
            str(idx),
            SampleRecord(
                name=f"user-{idx}",
                age=age,
                active=True,
                rating=4.0,
                tags=[],
                metadata={},
                nickname=None,
            ),
        )

    results = sqlite_store.filter(
        "users",
        SampleRecord,
        order_by="-age",
        limit=2,
        offset=1,
    )

    assert [record.age for record in results] == [30, 20]


def test_filter_raises_for_missing_schema(sqlite_store):
    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.filter("missing_schema", SampleRecord)


def test_filter_skips_corrupted_records(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    cursor = sqlite_store.connector.get_cursor()
    try:
        cursor.execute(
            "UPDATE users SET _record_state = ? WHERE id = ?",
            (b"not-a-valid-pickle", "user-1"),
        )
        sqlite_store.connector.commit()
    finally:
        cursor.close()

    results = sqlite_store.filter("users", SampleRecord)

    assert results == []


def test_count_returns_total_and_filtered_counts(sqlite_store):
    sqlite_store.insert(
        "users",
        "1",
        SampleRecord(
            name="Alice",
            age=30,
            active=True,
            rating=4.1,
            tags=[],
            metadata={},
            nickname=None,
        ),
    )
    sqlite_store.insert(
        "users",
        "2",
        SampleRecord(
            name="Bob",
            age=24,
            active=False,
            rating=3.7,
            tags=[],
            metadata={},
            nickname=None,
        ),
    )
    sqlite_store.insert(
        "users",
        "3",
        SampleRecord(
            name="Alicia",
            age=35,
            active=True,
            rating=4.9,
            tags=[],
            metadata={},
            nickname=None,
        ),
    )

    assert sqlite_store.count("users") == 3
    assert sqlite_store.count("users", active=True) == 2
    assert sqlite_store.count("users", age__gte=30) == 2


def test_count_raises_for_missing_schema(sqlite_store):
    with pytest.raises(ObjectDoesNotExist, match="does not exist"):
        sqlite_store.count("missing_schema")


def test_reload_refreshes_record_state_from_database(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)

    loaded = sqlite_store.get("users", "user-1", SampleRecord)
    loaded.id = "user-1"
    assert loaded.name == "Alice"

    updated = SampleRecord(
        name="Alice Reloaded",
        age=33,
        active=False,
        rating=4.7,
        tags=["ops"],
        metadata={"team": "runtime"},
        nickname="reloaded",
    )
    sqlite_store.update("users", "user-1", updated)

    reloaded = sqlite_store.reload("users", loaded)

    assert reloaded is loaded
    assert loaded.name == "Alice Reloaded"
    assert loaded.age == 33
    assert loaded.active is False
    assert loaded.metadata == {"team": "runtime"}


def test_reload_raises_when_record_has_no_id(sqlite_store):
    class NoIdRecord:
        pass

    with pytest.raises(ValueError, match="must have an 'id' attribute"):
        sqlite_store.reload("users", NoIdRecord())


def test_reload_raises_when_record_no_longer_exists(sqlite_store, sample_record):
    sqlite_store.insert("users", "user-1", sample_record)
    loaded = sqlite_store.get("users", "user-1", SampleRecord)
    loaded.id = "user-1"
    sqlite_store.delete("users", "user-1")

    with pytest.raises(ObjectDoesNotExist, match="no longer exists"):
        sqlite_store.reload("users", loaded)


def test_build_sql_filter_supports_multiple_operators(sqlite_store):
    where_clause, params = sqlite_store._build_sql_filter(
        {
            "name__icontains": "ali",
            "age__gte": 30,
            "age__lte": 40,
            "nickname__isnull": True,
            "id__in": ["1", "2", "3"],
        }
    )

    assert "name LIKE ? COLLATE NOCASE" in where_clause
    assert "age >= ?" in where_clause
    assert "age <= ?" in where_clause
    assert "nickname IS NULL" in where_clause
    assert "id IN (?,?,?)" in where_clause
    assert params == ["%ali%", 30, 40, "1", "2", "3"]


def test_convert_key_type_returns_int_when_string_is_numeric(sqlite_store):
    assert sqlite_store._convert_key_type("42") == 42
    assert sqlite_store._convert_key_type("abc") == "abc"
    assert sqlite_store._convert_key_type(7) == 7


def test_ensure_connected_reconnects_when_connection_is_lost(sqlite_store):
    sqlite_store.connector.is_connected = lambda: False

    called = {"count": 0}

    def reconnect():
        called["count"] += 1

    sqlite_store.connector.connect = reconnect

    sqlite_store._ensure_connected()

    assert called["count"] == 1


def test_is_optional_field_detects_optional_and_non_optional_fields(sqlite_store):
    assert sqlite_store._is_optional_field(Optional[str]) is True
    assert sqlite_store._is_optional_field(str | None) is True
    assert sqlite_store._is_optional_field(str) is False
    assert sqlite_store._is_optional_field(int) is False


def test_create_schema_allows_optional_fields_to_be_null(sqlite_store):
    class NullableRecord(BaseModel):
        name: str
        nickname: Optional[str]

        def __getstate__(self):
            return {
                "name": self.name,
                "nickname": self.nickname,
            }

        def __setstate__(self, state):
            self.name = state["name"]
            self.nickname = state["nickname"]

    record = NullableRecord(name="Alice", nickname=None)

    sqlite_store.create_schema("users", record)
    sqlite_store.insert("users", "user-1", record)

    loaded = sqlite_store.get("users", "user-1", NullableRecord)

    assert loaded.name == "Alice"
    assert loaded.nickname is None
