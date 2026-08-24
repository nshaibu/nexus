import logging
import os
import threading
import typing
import itertools
from dataclasses import asdict, InitVar, dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Type, Any, List, Set, Iterator, Union

from formax.typing import is_builtin_type
from formax import Attrib, BaseModel, MiniAnnotated, InitStrategy

from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.utils import get_obj_klass_import_str, get_obj_state
from volnux.exceptions import MultiValueError
from volnux.import_utils import import_string

if typing.TYPE_CHECKING:
    from volnux.backends.store import KeyValueStoreBackendBase

__all__ = ["EventResult", "ResultSet"]

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="KeyValueStoreIntegrationMixin")

Result = typing.TypeVar(
    "Result", bound=typing.Hashable
)  # Placeholder for a Result type


class EventResult(KeyValueStoreIntegrationMixin, BaseModel):
    error: bool
    event_name: str
    content: typing.Any
    workflow_id: typing.Optional[str]
    task_id: typing.Optional[str]
    process_id: MiniAnnotated[int, Attrib(default_factory=lambda: os.getpid())]
    creation_time: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]
    order: typing.Optional[int] = 0
    is_persisted: bool = False

    # Internal state and configuration
    autosave: InitVar[bool] = False
    storage_backend: InitVar[typing.Optional["KeyValueStoreBackendBase"]] = None

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def success(self) -> bool:
        return not self.error

    @success.setter
    def success(self, value: bool) -> None:
        self.error = not value

    def should_persist(self) -> bool:
        raise NotImplementedError(
            "should_persist method must be implemented by subclasses"
        )

    def get_state(self) -> typing.Dict[str, typing.Any]:
        state = super().__getstate__().copy()
        state.pop("_objectid_lock", None)

        if self.content is not None:
            content_type = type(self.content)
            if not is_builtin_type(content_type):  # type: ignore
                state["content"] = {
                    "content_type_import_str": get_obj_klass_import_str(self.content),
                    "state": get_obj_state(self.content),
                }

        return state

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        content = state.get("content")

        if isinstance(content, dict) and "content_type_import_str" in content:
            import_str = content["content_type_import_str"]
            content_state = content["state"]
            klass = import_string(import_str)
            instance = klass.__new__(klass)  # type: ignore
            instance.__setstate__(content_state)
            state["content"] = instance

        state.pop("_objectid_lock", None)
        self.__setstate__(state)
        self._objectid_lock = threading.Lock()

    def is_error(self) -> bool:
        return self.error

    def as_dict(self) -> typing.Dict[str, typing.Any]:
        """Serialize event result"""
        content = None
        if isinstance(self.content, Exception):
            content = self.content
            self.content = None
        result_dict = asdict(self)
        if content is not None:
            if hasattr(content, "as_dict"):
                content = content.as_dict()
            elif hasattr(content, "to_dict"):
                content = content.to_dict()
            result_dict["content"] = content
        return result_dict


# class EntityContentType:
#     """Represents the content type information for an entity."""
#
#     def __init__(
#         self,
#         backend_import_str: typing.Optional[str] = None,
#         entity_content_type: typing.Optional[str] = None,
#     ):
#         self.backend_import_str = backend_import_str
#         self.entity_content_type = entity_content_type
#
#     @classmethod
#     def add_entity_content_type(
#         cls, entity: Result
#     ) -> typing.Optional["EntityContentType"]:
#         """Create an EntityContentType from an ObjectIdentityMixin instance."""
#         if not entity or not getattr(entity, "id", None):
#             return None
#
#         connector = getattr(entity, "_connector", None)
#         backend_import_str = None
#
#         if connector:
#             backend_import_str = get_obj_klass_import_str(connector)
#
#         return cls(
#             backend_import_str=backend_import_str,
#             entity_content_type=getattr(entity, "__object_import_str__", None),
#         )
#
#     def get_backend(self) -> typing.Any:
#         """Import and return the backend class."""
#         if not self.backend_import_str:
#             raise ValueError("No backend import string specified")
#         return import_string(self.backend_import_str)
#
#     def get_content_type(self) -> typing.Any:
#         """Import and return the content type class."""
#         if not self.entity_content_type:
#             raise ValueError("No entity content type specified")
#         return import_string(self.entity_content_type)
#
#     def __eq__(self, other: typing.Any) -> bool:
#         if not isinstance(other, EntityContentType):
#             return False
#         return (
#             self.backend_import_str == other.backend_import_str
#             and self.entity_content_type == other.entity_content_type
#         )
#
#     def __hash__(self) -> int:
#         return hash((self.backend_import_str, self.entity_content_type))
#
#     def __repr__(self) -> str:
#         return f"<EntityContentType: backend={self.backend_import_str}, type={self.entity_content_type}>"
#
#
# class ResultSet(typing.MutableSet[Result]):
#     """A collection of Result objects with filtering and query capabilities."""
#
#     # Dictionary of filter operators and their implementation
#     _FILTER_OPERATORS: typing.Final[typing.Set[str]] = {
#         "contains",
#         "startswith",
#         "endswith",
#         "icontains",
#         "gt",
#         "gte",
#         "lt",
#         "lte",
#         "in",
#         "exact",
#         "isnull",
#     }
#
#     def __init__(self, results: typing.Optional[typing.List[Result]] = None) -> None:
#         self._content: typing.Dict[str, Result] = {}
#         self._context_types: typing.Set[EntityContentType] = set()
#
#         if results is None:
#             results = []
#
#         for result in results:
#             self._content[self.get_hash(result)] = result
#             self._insert_entity(result)
#
#     @staticmethod
#     def get_hash(value: Result) -> str:
#         try:
#             return f"{hash(value)}"
#         except TypeError as e:
#             raise TypeError("Result must be hashable") from e
#
#     def __contains__(self, item: Result) -> bool:
#         try:
#             key = self.get_hash(item)
#         except TypeError:
#             return False
#         return key in self._content
#
#     def __iter__(self) -> typing.Iterator[Result]:
#         return iter(self._content.values())
#
#     def __len__(self) -> int:
#         return len(self._content)
#
#     def __getitem__(self, index: int) -> Result:
#         """Access a result by index."""
#         return list(self._content.values())[index]
#
#     def _insert_entity(self, record: Result) -> None:
#         """
#         Insert an entity and track its content type.
#
#         Args:
#             record: The Result object to insert.
#         """
#         self._content[self.get_hash(record)] = typing.cast(Result, record)
#         content_type = EntityContentType.add_entity_content_type(record)
#         if content_type and content_type not in self._context_types:
#             self._context_types.add(content_type)
#
#     def add(self, value: typing.Union[Result, "ResultSet"]) -> None:
#         """
#         Add a result or merge another ResultSet.
#         Args:
#             value: Result or ResultSet to add.
#         """
#         if isinstance(value, ResultSet):
#             self._content.update(value._content)
#             self._context_types.update(value._context_types)
#         else:
#             self._content[self.get_hash(value)] = value
#             self._insert_entity(value)
#
#     def extend(self, results: typing.Collection[Result]) -> None:
#         """
#         Add multiple items to set.
#         Args:
#             results: Collection of Result objects to add.
#         """
#         for result in results:
#             self.add(result)
#
#     def clear(self) -> None:
#         """Remove all results."""
#         self._content.clear()
#         self._context_types.clear()
#
#     def discard(self, value: typing.Union[Result, "ResultSet"]) -> None:
#         """Remove a result or results from another ResultSet."""
#         if isinstance(value, ResultSet):
#             for res in value:
#                 self._content.pop(self.get_hash(res), None)
#         else:
#             self._content.pop(self.get_hash(value), None)
#
#     def copy(self) -> "ResultSet":
#         """Create a shallow copy of this ResultSet."""
#         new = ResultSet([])
#         new._content = self._content.copy()
#         new._context_types = self._context_types.copy()
#         return new
#
#     def get(self, **filters: typing.Any) -> Result:
#         """
#         Get a single result matching the filters.
#         Raises MultiValueError if more than one result is found.
#         """
#         qs = self.filter(**filters)
#         if len(qs) == 0:
#             raise KeyError(f"No result found matching filters: {filters}")
#         if len(qs) > 1:
#             raise MultiValueError(
#                 f"More than one result found for filters {filters}: {len(qs)}!=1"
#             )
#         return qs[0]
#
#     def get_entry_by_hash(self, hash_id: typing.Union[str, int]) -> Result:
#         """
#         Retrieves an entry from a dictionary of content using the provided hash identifier.
#
#         :param hash_id: The unique hash identifier for the entry to be retrieved.
#         :type hash_id: str
#         :return: The entry associated with the provided hash identifier.
#         :rtype: Result
#         :raises KeyError: If no entry is found with the given hash identifier.
#         """
#         key = str(hash_id) if isinstance(hash_id, int) else hash_id
#         entry = self._content.get(key)
#         if entry is None:
#             raise KeyError(f"No result found with hash {hash_id}")
#         return entry
#
#     def filter(self, **filter_params: typing.Any) -> "ResultSet":
#         """
#         Filter results by attribute values with support for nested fields.
#
#         Features:
#         - Basic attribute matching (user=x)
#         - Nested dictionary lookups (profile__name=y)
#         - List/iterable searching (tags__contains=z)
#         - Special lookup operators:
#             - __contains: Check if value is in a list/iterable
#             - __startswith: String starts with value
#             - __endswith: String ends with value
#             - __icontains: Case-insensitive contains
#             - __gt, __gte, __lt, __lte: Comparisons
#             - __in: Check if field value is in provided list
#             - __exact: Exact matching (default behavior)
#             - __isnull: Check if field is None
#
#         Examples:
#         - rs.filter(name="Alice") - Basic field matching
#         - rs.filter(user__profile__city="New York") - Nested dict lookup
#         - rs.filter(tags__contains="urgent") - Check if list contains value
#         - rs.filter(name__startswith="A") - String prefix matching
#         """
#         filtered_results = []
#
#         for result in self._content.values():
#             if self._matches_filters(result, filter_params):
#                 filtered_results.append(result)
#
#         return ResultSet(filtered_results)
#
#     def _matches_filters(
#         self, result: Result, filters: typing.Dict[str, typing.Any]
#     ) -> bool:
#         """
#         Check if a result matches all filters, supporting nested lookups and operators.
#
#         Args:
#             result: The result object to check
#             filters: Dictionary of filter parameters
#
#         Returns:
#             True if the result matches all filters, False otherwise
#         """
#         for key, value in filters.items():
#             # Check if this is a special lookup with operator
#             if "__" in key:
#                 parts = key.split("__")
#                 if parts[-1] in self._FILTER_OPERATORS:
#                     field_path_list, operator = parts[:-1], parts[-1]
#                     field_path: str = "__".join(field_path_list)
#                     if not self._check_operator(result, field_path, operator, value):
#                         return False
#                 else:
#                     # This is a nested lookup without operator
#                     if not self._check_nested_field(result, key.split("__"), value):
#                         return False
#             else:
#                 # Simple field comparison
#                 try:
#                     field_value = getattr(result, key, None)
#                     if field_value != value:
#                         return False
#                 except (TypeError, ValueError):
#                     return False
#
#         return True
#
#     def _get_field_value(
#         self, obj: typing.Any, field_path: typing.List[str]
#     ) -> typing.Any:
#         """
#         Get a value from potentially nested objects.
#
#         Args:
#             obj: The object to extract value from
#             field_path: List of field names to traverse
#
#         Returns:
#             The value at the end of the path or None if not found
#         """
#         current = obj
#
#         for field in field_path:
#             # Handle dictionary access
#             if hasattr(current, "__getitem__") and isinstance(current, dict):
#                 try:
#                     current = current[field]
#                     continue
#                 except (KeyError, TypeError):
#                     pass
#
#             # Handle object attribute access
#             if hasattr(current, field):
#                 current = getattr(current, field)
#                 continue
#
#             # Nothing found
#             return None
#
#         return current
#
#     def _check_nested_field(
#         self, obj: typing.Any, field_path: typing.List[str], expected_value: typing.Any
#     ) -> bool:
#         """
#         Check if a nested field matches the expected value.
#
#         Args:
#             obj: The object to check
#             field_path: Path to the field
#             expected_value: Value to compare against
#
#         Returns:
#             True if the field exists and matches the value
#         """
#         actual_value = self._get_field_value(obj, field_path)
#         return actual_value == expected_value  # type: ignore
#
#     def _check_operator(
#         self, obj: typing.Any, field_path: str, operator: str, filter_value: typing.Any
#     ) -> bool:
#         """
#         Apply a filter operator to a field.
#         Args:
#             obj: The object to check
#             field_path: Path to the field (as string with __ separators)
#             operator: The operator to apply
#             filter_value: Value to compare against
#         Returns:
#             True if the condition is met, False otherwise
#         """
#         actual_value = self._get_field_value(obj, field_path.split("__"))
#
#         if operator in ("startswith", "endswith", "icontains") and not isinstance(
#             actual_value, str
#         ):
#             return False
#
#         # Handle None case for all operators except isnull
#         if actual_value is None and operator != "isnull":
#             return False
#
#         # Apply the appropriate operator
#         if operator == "contains":
#             if hasattr(actual_value, "__contains__"):
#                 return filter_value in actual_value
#             return False
#
#         elif operator == "startswith":
#             return isinstance(actual_value, str) and actual_value.startswith(
#                 filter_value
#             )
#
#         elif operator == "endswith":
#             return isinstance(actual_value, str) and actual_value.endswith(filter_value)
#
#         elif operator == "icontains":
#             if not isinstance(actual_value, str) or not isinstance(filter_value, str):
#                 return False
#             return filter_value.lower() in actual_value.lower()
#
#         elif operator == "gt":
#             return actual_value > filter_value
#
#         elif operator == "gte":
#             return actual_value >= filter_value
#
#         elif operator == "lt":
#             return actual_value < filter_value
#
#         elif operator == "lte":
#             return actual_value <= filter_value
#
#         elif operator == "in":
#             return actual_value in filter_value
#
#         elif operator == "exact":
#             return actual_value == filter_value
#
#         elif operator == "isnull":
#             return (actual_value is None) == filter_value
#
#         # Unknown operator
#         return False
#
#     def first(self) -> typing.Optional[Result]:
#         """Return the first result or None if empty."""
#         try:
#             return self[0]
#         except IndexError:
#             return None
#
#     def is_empty(self) -> bool:
#         """Return True if the result is empty."""
#         return self.first() is None
#
#     def __str__(self) -> str:
#         return str(list(self._content.values()))
#
#     def __repr__(self) -> str:
#         return f"<{self.__class__.__name__}: {len(self)}>"
#
#     def __class_getitem__(cls, item: typing.Any) -> typing.Type["ResultSet"]:
#         return cls


# Immutable operator set — prevents accidental mutation at runtime
_FILTER_OPERATORS: typing.Final[frozenset[str]] = frozenset(
    {
        "contains",
        "startswith",
        "endswith",
        "icontains",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "exact",
        "isnull",
    }
)


@dataclass(frozen=True)
class EntityContentType:
    """Immutable, serializable content type descriptor.

    Uses governed model references instead of raw import strings.
    Frozen to ensure hash stability and prevent mutation after creation.
    """

    model_path: str
    backend_alias: str = "default"

    @classmethod
    def from_entity(cls, entity: "Result") -> Optional["EntityContentType"]:
        """Create a content type descriptor from a Result entity."""
        if not entity or not getattr(entity, "id", None):
            return None
        return cls(
            model_path=get_obj_klass_import_str(entity),
            backend_alias=getattr(entity, "_backend_alias", "default"),
        )

    def resolve_model(self) -> Type["Result"]:
        """Resolve and return the referenced model class."""
        return import_string(self.model_path)  # type: ignore[return-value]

    def __repr__(self) -> str:
        return f"<EntityContentType: {self.model_path} (backend={self.backend_alias})>"


class ResultSet(typing.MutableSet[Result]):
    """A governed collection of Result objects with filtering and query capabilities.

    Identity is based on primary keys when available, ensuring stability
    across serialization boundaries (checkpoint/rehydrate cycles).
    Content type metadata is propagated through filter/exclude operations
    to avoid redundant re-inspection.
    """

    def __init__(self, results: Optional[List["Result"]] = None) -> None:
        self._content: Dict[str, "Result"] = {}
        self._context_types: Set[EntityContentType] = set()

        if results:
            for result in results:
                self._insert_entity(result)

    @staticmethod
    def _identity_key(value: "Result") -> str:
        """Generate a stable identity key that survives process boundaries.

        Priority:
          1. Formax-Py primary key (id) — stable across serialization
          2. ExecutionContext state_id — stable for execution contexts
          3. Python hash fallback — for transient/non-model results only
        """
        pk = getattr(value, "id", None) or getattr(value, "state_id", None)
        if pk is not None:
            return f"pk:{pk}"

        try:
            return f"hash:{hash(value)}"
        except TypeError:
            return f"ref:{id(value)}"

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, object):
            return False
        try:
            key = self._identity_key(typing.cast("Result", item))
        except Exception:
            return False
        return key in self._content

    def __iter__(self) -> Iterator["Result"]:
        return iter(self._content.values())

    def __len__(self) -> int:
        return len(self._content)

    def add(self, value: Union["Result", "ResultSet"]) -> None:  # type: ignore[override]
        """Add a result or merge another ResultSet."""
        if isinstance(value, ResultSet):
            for result in value._content.values():
                self._insert_entity(result)
            self._context_types.update(value._context_types)
        else:
            self._insert_entity(value)

    def discard(self, value: Union["Result", "ResultSet"]) -> None:  # type: ignore[override]
        """Remove a result or all results from another ResultSet."""
        if isinstance(value, ResultSet):
            for res in value:
                self._content.pop(self._identity_key(res), None)
        else:
            self._content.pop(self._identity_key(value), None)

    def extend(self, results: typing.Collection["Result"]) -> None:
        """Add multiple items to the set."""
        for result in results:
            self._insert_entity(result)

    def clear(self) -> None:
        """Remove all results and content type metadata."""
        self._content.clear()
        self._context_types.clear()

    def copy(self) -> "ResultSet":
        """Create a shallow copy preserving content type metadata."""
        new = ResultSet()
        new._content = self._content.copy()
        new._context_types = self._context_types.copy()
        return new

    def __getitem__(self, index: int) -> "Result":
        """Access a result by index without materializing the full values list."""
        length = len(self._content)
        if index < 0:
            index += length
        if index < 0 or index >= length:
            raise IndexError(f"ResultSet index {index} out of range [0, {length})")
        return next(itertools.islice(self._content.values(), index, index + 1))

    def first(self) -> Optional["Result"]:
        """Return the first result or None if empty."""
        try:
            return self[0]
        except IndexError:
            return None

    def is_empty(self) -> bool:
        """Return True if the result set has no entries."""
        return len(self._content) == 0

    def count(self) -> int:
        """Return the number of unique results."""
        return len(self._content)

    def _insert_entity(self, record: "Result") -> None:
        """Insert an entity and track its content type."""
        key = self._identity_key(record)
        self._content[key] = record

        content_type = EntityContentType.from_entity(record)
        if content_type is not None:
            self._context_types.add(content_type)

    def get(self, **filters: Any) -> "Result":
        """Get a single result matching filters. Raises on 0 or >1 matches."""
        qs = self.filter(**filters)
        count = qs.count()
        if count == 0:
            raise KeyError(f"No result found matching filters: {filters}")
        if count > 1:
            raise MultiValueError(
                f"More than one result found for filters {filters}: {count} != 1"
            )
        return qs[0]

    def get_entry_by_identity(self, identity: Union[str, int]) -> "Result":
        """Retrieve an entry by its stable identity key or primary key."""
        key = str(identity) if isinstance(identity, int) else identity
        # Try direct lookup first (for pk:-prefixed keys)
        entry = self._content.get(key)
        if entry is not None:
            return entry
        # Try with pk: prefix for raw IDs
        prefixed = f"pk:{key}"
        entry = self._content.get(prefixed)
        if entry is not None:
            return entry
        raise KeyError(f"No result found with identity {identity}")

    def filter(self, **filter_params: Any) -> "ResultSet":
        """Filter results by attribute values with nested field and operator support.

        Content type metadata is inherited from the parent set since
        a filtered subset cannot contain types absent from the original.
        """
        filtered = [
            r for r in self._content.values() if self._matches_filters(r, filter_params)
        ]
        result = ResultSet(filtered)
        # Inherit content types — avoids O(n) re-inspection on chained filters
        result._context_types = self._context_types.copy()
        return result

    def exclude(self, **filter_params: Any) -> "ResultSet":
        """Exclude results matching filters. Complement of filter()."""
        excluded = [
            r
            for r in self._content.values()
            if not self._matches_filters(r, filter_params)
        ]
        result = ResultSet(excluded)
        result._context_types = self._context_types.copy()
        return result

    # ------------------------------------------------------------------
    # Filter Engine Internals
    # ------------------------------------------------------------------

    def _matches_filters(self, result: "Result", filters: Dict[str, Any]) -> bool:
        """Check if a result matches ALL filters (AND semantics)."""
        for key, value in filters.items():
            if "__" in key:
                parts = key.split("__")
                if parts[-1] in _FILTER_OPERATORS:
                    field_path = "__".join(parts[:-1])
                    if not self._check_operator(result, field_path, parts[-1], value):
                        return False
                else:
                    # Nested lookup without operator suffix
                    if not self._check_nested_field(result, parts, value):
                        return False
            else:
                # Simple exact match
                try:
                    if getattr(result, key, None) != value:
                        return False
                except (TypeError, ValueError):
                    return False
        return True

    @staticmethod
    def _get_field_value(obj: Any, field_path: List[str]) -> Any:
        """Traverse nested attributes/dicts to extract a field value."""
        current = obj
        for field in field_path:
            if isinstance(current, dict):
                try:
                    current = current[field]
                    continue
                except (KeyError, TypeError):
                    return None
            if hasattr(current, field):
                current = getattr(current, field)
                continue
            return None
        return current

    def _check_nested_field(
        self, obj: Any, field_path: List[str], expected: Any
    ) -> bool:
        actual = self._get_field_value(obj, field_path)
        return actual == expected

    def _check_operator(
        self, obj: Any, field_path: str, operator: str, filter_value: Any
    ) -> bool:
        actual = self._get_field_value(obj, field_path.split("__"))

        # String operators require string operands
        if operator in ("startswith", "endswith", "icontains"):
            if not isinstance(actual, str) or not isinstance(filter_value, str):
                return False
            if operator == "startswith":
                return actual.startswith(filter_value)
            if operator == "endswith":
                return actual.endswith(filter_value)
            # icontains
            return filter_value.lower() in actual.lower()

        # Null check is valid even when actual is None
        if operator == "isnull":
            return (actual is None) == filter_value

        # All other operators return False for None actual values
        if actual is None:
            return False

        if operator == "contains":
            return hasattr(actual, "__contains__") and filter_value in actual
        if operator == "gt":
            return actual > filter_value
        if operator == "gte":
            return actual >= filter_value
        if operator == "lt":
            return actual < filter_value
        if operator == "lte":
            return actual <= filter_value
        if operator == "in":
            return actual in filter_value
        if operator == "exact":
            return actual == filter_value

        logger.warning("Unknown filter operator: %s", operator)
        return False

    def __str__(self) -> str:
        return str(list(self._content.values()))

    def __repr__(self) -> str:
        return f"<ResultSet: {self.count()} items, {len(self._context_types)} content types>"

    def __class_getitem__(cls, item: Any) -> Type["ResultSet"]:
        return cls
