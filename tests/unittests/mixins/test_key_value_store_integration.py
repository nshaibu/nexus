import asyncio
import pickle
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from volnux.exceptions import ImproperlyConfigured, ObjectDoesNotExist, ObjectExistError
from volnux.mixins.key_value_store_integration import (
    KeyValueStoreIntegrationMixin,
    backend_operation,
)


class ObjIDMixin:

    @property
    def id(self):
        return self._id


class ExampleModel(ObjIDMixin, KeyValueStoreIntegrationMixin):
    _backend_store = None
    _backend_config = None

    def __init__(self, id="1", name="Alice", status="active"):
        super().__init__()
        self._id = id
        self.name = name
        self.status = status
        self.__model_init__()
        self._loaded_from_backend = False

    def get_state(self):
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
        }

    def set_state(self, state):
        self._id = state["id"]
        self.name = state["name"]
        self.status = state["status"]


class BrokenStateModel(ObjIDMixin, KeyValueStoreIntegrationMixin):
    _backend_store = None
    _backend_config = None

    def __init__(self):
        super().__init__()
        self._id = "broken"
        self.__model_init__()
        self._loaded_from_backend = False

    def get_state(self):
        raise NotImplementedError

    def set_state(self, state):
        raise NotImplementedError


@pytest.fixture
def backend():
    backend = MagicMock()
    backend.connector = MagicMock()
    backend.connector.is_connected.return_value = True
    backend.connector.connect = MagicMock()
    backend.connector.disconnect = MagicMock()
    backend.insert = MagicMock()
    backend.update = MagicMock()
    backend.upsert = MagicMock()
    backend.delete = MagicMock()
    backend.reload = MagicMock()
    backend.exists = MagicMock(return_value=True)
    backend.get = MagicMock()
    backend.filter = MagicMock(return_value=[])
    backend.count = MagicMock(return_value=0)
    backend.bulk_insert = MagicMock()
    backend.bulk_delete = MagicMock()
    backend.clear_schema = MagicMock()
    backend.close = MagicMock()
    return backend


@pytest.fixture(autouse=True)
def reset_backend_store():
    ExampleModel._backend_store = None
    ExampleModel._backend_config = None
    BrokenStateModel._backend_store = None
    BrokenStateModel._backend_config = None
    yield
    ExampleModel._backend_store = None
    ExampleModel._backend_config = None
    BrokenStateModel._backend_store = None
    BrokenStateModel._backend_config = None


def test_backend_operation_decorator_with_auto_save():
    class DecoratedModel:
        def __init__(self):
            self.saved = 0
            self.value = None

        def save(self):
            self.saved += 1

        @backend_operation(auto_save=True)
        def set_value(self, value):
            self.value = value
            return "done"

    obj = DecoratedModel()

    result = obj.set_value("x")

    assert result == "done"
    assert obj.value == "x"
    assert obj.saved == 1


def test_backend_operation_decorator_without_auto_save():
    class DecoratedModel:
        def __init__(self):
            self.saved = 0

        def save(self):
            self.saved += 1

        @backend_operation(auto_save=False)
        def touch(self):
            return "ok"

    obj = DecoratedModel()

    result = obj.touch()

    assert result == "ok"
    assert obj.saved == 0


def test_initialize_backend_creates_backend_from_config(backend):
    config = {
        "ENGINE": "fake.backend.Class",
        "CONNECTOR_CONFIG": {"host": "localhost", "port": 11211},
    }

    class FakeBackendClass:
        __name__ = "FakeBackendClass"

        def __new__(cls, **kwargs):
            return backend

    with patch.object(
        __import__(
            "volnux.mixins.key_value_store_integration", fromlist=["CONFIG"]
        ).CONFIG,
        "KEY_VALUE_STORE_CONFIG",
        config,
        create=True,
    ), patch(
        "volnux.mixins.key_value_store_integration.import_string",
        return_value=FakeBackendClass,
    ) as mock_import:
        ExampleModel._initialize_backend()

    assert ExampleModel._backend_store is backend
    assert ExampleModel._backend_config == config
    mock_import.assert_called_once_with("fake.backend.Class")


def test_initialize_backend_connects_if_connector_not_connected(backend):
    backend.connector.is_connected.return_value = False
    config = {
        "ENGINE": "fake.backend.Class",
        "CONNECTOR_CONFIG": {},
    }

    class FakeBackendClass:
        __name__ = "FakeBackendClass"

        def __new__(cls, **kwargs):
            return backend

    with patch.object(
        __import__(
            "volnux.mixins.key_value_store_integration", fromlist=["CONFIG"]
        ).CONFIG,
        "KEY_VALUE_STORE_CONFIG",
        config,
        create=True,
    ), patch(
        "volnux.mixins.key_value_store_integration.import_string",
        return_value=FakeBackendClass,
    ):
        ExampleModel._initialize_backend()

    backend.connector.connect.assert_called_once()


def test_initialize_backend_raises_when_engine_not_configured():
    config = {"CONNECTOR_CONFIG": {}}

    with patch(
        "volnux.mixins.key_value_store_integration.CONFIG",
        new=type("Config", (), {"KEY_VALUE_STORE_CONFIG": config})(),
    ):
        with pytest.raises(ImproperlyConfigured, match="Backend initialization failed"):
            ExampleModel._initialize_backend()


def test_get_backend_initializes_when_missing(backend):
    with patch.object(ExampleModel, "_initialize_backend") as mock_init:
        mock_init.side_effect = lambda: setattr(ExampleModel, "_backend_store", backend)

        result = ExampleModel.get_backend()

    assert result is backend
    mock_init.assert_called_once()


def test_get_schema_name_returns_class_name():
    assert ExampleModel.get_schema_name() == "ExampleModel"


def test_mark_as_loaded_sets_flag():
    ExampleModel._backend_store = MagicMock()

    obj = ExampleModel()
    assert obj._is_loaded_from_backend() is False

    obj._mark_as_loaded()

    assert obj._is_loaded_from_backend() is True


def test_save_force_insert_calls_backend_insert(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    obj.save(force_insert=True, ttl=60)

    backend.insert.assert_called_once_with("ExampleModel", "123", obj, ttl=60)
    assert obj._is_loaded_from_backend() is True


def test_save_uses_upsert_when_available(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    backend.upsert.reset_mock()

    obj.save()

    backend.upsert.assert_called_once_with("ExampleModel", "123", obj)
    assert obj._is_loaded_from_backend() is True


def test_save_falls_back_to_insert_then_update_when_no_upsert(backend):
    ExampleModel._backend_store = backend
    del backend.upsert
    backend.insert.side_effect = ObjectExistError("exists")
    obj = ExampleModel(id="123")

    backend.insert.reset_mock()
    backend.update.reset_mock()

    obj.save()

    backend.insert.assert_called_once_with("ExampleModel", "123", obj, ttl=None)
    backend.update.assert_called_once_with("ExampleModel", "123", obj)
    assert obj._is_loaded_from_backend() is True


def test_save_force_insert_propagates_object_exist_error(backend):
    ExampleModel._backend_store = backend
    backend.insert.side_effect = ObjectExistError("exists")
    obj = ExampleModel(id="123")

    with pytest.raises(ObjectExistError):
        obj.save(force_insert=True)


def test_save_async_delegates_to_save(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    asyncio.run(obj.save_async(force_insert=True, ttl=30))

    backend.insert.assert_called_once_with("ExampleModel", "123", obj, ttl=30)


def test_update_calls_backend_update(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    obj.update()

    backend.update.assert_called_once_with("ExampleModel", "123", obj)


def test_delete_calls_backend_delete(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    obj.delete()

    backend.delete.assert_called_once_with("ExampleModel", "123")


def test_reload_calls_backend_reload_and_marks_loaded(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")

    obj.reload()

    backend.reload.assert_called_once_with("ExampleModel", obj)
    assert obj._is_loaded_from_backend() is True


def test_refresh_aliases_reload(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="123")
    obj.reload = MagicMock()

    obj.refresh()

    obj.reload.assert_called_once()


def test_exists_returns_backend_result(backend):
    ExampleModel._backend_store = backend
    backend.exists.return_value = True
    obj = ExampleModel(id="123")

    assert obj.exists() is True
    backend.exists.assert_called_once_with("ExampleModel", "123")


def test_exists_returns_false_on_exception(backend):
    ExampleModel._backend_store = backend
    backend.exists.side_effect = RuntimeError("boom")
    obj = ExampleModel(id="123")

    assert obj.exists() is False


def test_get_returns_loaded_instance(backend):
    ExampleModel._backend_store = backend
    loaded = ExampleModel(id="123")
    backend.get.return_value = loaded

    result = ExampleModel.get("123")

    assert result is loaded
    backend.get.assert_called_once_with("ExampleModel", "123", ExampleModel)
    assert result._is_loaded_from_backend() is True


def test_get_or_none_returns_none_when_missing(backend):
    ExampleModel._backend_store = backend
    backend.get.side_effect = ObjectDoesNotExist("missing")

    result = ExampleModel.get_or_none("123")

    assert result is None


def test_filter_returns_loaded_instances(backend):
    ExampleModel._backend_store = backend
    one = ExampleModel(id="1")
    two = ExampleModel(id="2")
    backend.filter.return_value = [one, two]

    result = ExampleModel.filter(status="active")

    assert result == [one, two]
    backend.filter.assert_called_once_with(
        "ExampleModel",
        ExampleModel,
        status="active",
    )
    assert one._is_loaded_from_backend() is True
    assert two._is_loaded_from_backend() is True


def test_all_delegates_to_filter(backend):
    ExampleModel._backend_store = backend
    backend.filter.return_value = [ExampleModel(id="1")]

    result = ExampleModel.all()

    assert len(result) == 1
    backend.filter.assert_called_once_with("ExampleModel", ExampleModel)


def test_count_calls_backend_count(backend):
    ExampleModel._backend_store = backend
    backend.count.return_value = 5

    result = ExampleModel.count(status="active")

    assert result == 5
    backend.count.assert_called_once_with("ExampleModel", status="active")


def test_exists_in_backend_calls_backend_exists(backend):
    ExampleModel._backend_store = backend
    backend.exists.return_value = True

    result = ExampleModel.exists_in_backend("123")

    assert result is True
    backend.exists.assert_called_once_with("ExampleModel", "123")


def test_bulk_create_uses_bulk_insert_when_available(backend):
    ExampleModel._backend_store = backend
    instances = [ExampleModel(id="1"), ExampleModel(id="2")]

    ExampleModel.bulk_create(instances)

    backend.bulk_insert.assert_called_once()
    call_args = backend.bulk_insert.call_args[0]
    assert call_args[0] == "ExampleModel"
    assert set(call_args[1].keys()) == {"1", "2"}
    assert all(instance._is_loaded_from_backend() for instance in instances)


def test_bulk_create_falls_back_to_save_force_insert(backend):
    ExampleModel._backend_store = backend
    del backend.bulk_insert

    one = ExampleModel(id="1")
    two = ExampleModel(id="2")
    one.save = MagicMock()
    two.save = MagicMock()

    ExampleModel.bulk_create([one, two])

    one.save.assert_called_once_with(force_insert=True)
    two.save.assert_called_once_with(force_insert=True)
    assert one._is_loaded_from_backend() is True
    assert two._is_loaded_from_backend() is True


def test_bulk_delete_uses_backend_bulk_delete(backend):
    ExampleModel._backend_store = backend

    ExampleModel.bulk_delete(["1", "2"])

    backend.bulk_delete.assert_called_once_with("ExampleModel", ["1", "2"])


def test_bulk_delete_falls_back_to_delete_one_by_one(backend):
    ExampleModel._backend_store = backend
    del backend.bulk_delete

    ExampleModel.bulk_delete(["1", "2"])

    assert backend.delete.call_count == 2
    backend.delete.assert_any_call("ExampleModel", "1")
    backend.delete.assert_any_call("ExampleModel", "2")


def test_clear_all_uses_clear_schema_when_available(backend):
    ExampleModel._backend_store = backend

    ExampleModel.clear_all()

    backend.clear_schema.assert_called_once_with("ExampleModel")


def test_clear_all_falls_back_to_all_then_bulk_delete(backend):
    ExampleModel._backend_store = backend
    del backend.clear_schema

    one = ExampleModel(id="1")
    two = ExampleModel(id="2")

    with patch.object(ExampleModel, "all", return_value=[one, two]), patch.object(
        ExampleModel, "bulk_delete"
    ) as mock_bulk_delete:
        ExampleModel.clear_all()

    mock_bulk_delete.assert_called_once_with(["1", "2"])


def test_atomic_saves_on_success(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="1", name="Alice")
    obj.save = MagicMock()

    with obj.atomic():
        obj.name = "Bob"

    obj.save.assert_called_once()
    assert obj.name == "Bob"


def test_atomic_restores_original_state_on_failure(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="1", name="Alice")

    with pytest.raises(RuntimeError, match="boom"):
        with obj.atomic():
            obj.name = "Bob"
            raise RuntimeError("boom")

    assert obj.name == "Alice"


def test_transaction_uses_connector_transaction_when_supported(backend):
    ExampleModel._backend_store = backend
    entered = []

    @contextmanager
    def fake_transaction():
        entered.append("enter")
        try:
            yield
        finally:
            entered.append("exit")

    backend.connector.transaction = fake_transaction

    with ExampleModel.transaction():
        entered.append("inside")

    assert entered == ["enter", "inside", "exit"]


def test_transaction_yields_without_connector_transaction(backend):
    ExampleModel._backend_store = backend
    if hasattr(backend.connector, "transaction"):
        del backend.connector.transaction

    with ExampleModel.transaction():
        value = "inside"

    assert value == "inside"


def test_getstate_returns_serializable_state(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="1", name="Alice")

    state = obj.__getstate__()

    assert state["id"] == "1"
    assert state["name"] == "Alice"
    assert state["status"] == "active"
    assert "_backend_class" in state
    assert "_backend_store" not in state
    assert "_backend_config" not in state


def test_getstate_raises_pickle_error_when_get_state_not_implemented():
    BrokenStateModel._backend_store = MagicMock()
    obj = BrokenStateModel()

    with pytest.raises(pickle.PickleError, match="Cannot pickle object"):
        obj.__getstate__()


def test_setstate_restores_state(backend):
    ExampleModel._backend_store = backend
    obj = ExampleModel(id="1", name="Alice")

    obj.__setstate__({"id": "2", "name": "Bob", "status": "inactive"})

    assert obj.id == "2"
    assert obj.name == "Bob"
    assert obj.status == "inactive"


def test_setstate_initializes_backend_when_missing(backend):
    obj = ExampleModel(id="1", name="Alice")
    ExampleModel._backend_store = None

    with patch.object(ExampleModel, "_initialize_backend") as mock_init:
        mock_init.side_effect = lambda: setattr(ExampleModel, "_backend_store", backend)

        obj.__setstate__({"_id": "2", "name": "Bob", "status": "inactive"})

    mock_init.assert_called_once()
    assert obj.id == "2"
    assert obj.name == "Bob"
    assert obj.status == "inactive"


def test_setstate_raises_unpickling_error_when_set_state_not_implemented():
    obj = BrokenStateModel()

    with pytest.raises(pickle.UnpicklingError, match="Cannot unpickle object"):
        obj.__setstate__({"id": "1"})


def test_close_backend_prefers_backend_close(backend):
    ExampleModel._backend_store = backend

    ExampleModel.close_backend()

    backend.close.assert_called_once()
    assert ExampleModel._backend_store is None
    assert ExampleModel._backend_config is None


def test_close_backend_falls_back_to_connector_disconnect():
    backend = MagicMock()
    backend.connector = MagicMock()
    backend.connector.disconnect = MagicMock()
    if hasattr(backend, "close"):
        del backend.close

    ExampleModel._backend_store = backend
    ExampleModel._backend_config = {"x": 1}

    ExampleModel.close_backend()

    backend.connector.disconnect.assert_called_once()
    assert ExampleModel._backend_store is None
    assert ExampleModel._backend_config is None


def test_repr_shows_exists_when_backend_has_record(backend):
    ExampleModel._backend_store = backend
    backend.exists.return_value = True
    obj = ExampleModel(id="123")

    result = repr(obj)

    assert result == "<ExampleModel:123 [exists]>"


def test_repr_shows_new_when_backend_has_no_record(backend):
    ExampleModel._backend_store = backend
    backend.exists.return_value = False
    obj = ExampleModel(id="123")

    result = repr(obj)

    assert result == "<ExampleModel:123 [new]>"
