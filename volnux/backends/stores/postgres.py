import json
import logging
from enum import Enum
from contextlib import contextmanager
from types import UnionType
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    TypeVar,
    TYPE_CHECKING,
)

from volnux.backends.store import KeyValueStoreBackendBase, YoyoMigrationsMixin
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SqlOperationError,
    SerializationError,
)
from volnux.backends.connectors.postgres import PostgresConnector

logger = logging.getLogger("volnux.backends.postgres")


if TYPE_CHECKING:
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

T = TypeVar("T", bound="KeyValueStoreIntegrationMixin")


class PostgresStoreBackend(
    YoyoMigrationsMixin,
    KeyValueStoreBackendBase,
):
    """PostgreSQL-backed key-value store implementation.

    This backend uses PostgreSQL tables to store records, with automatic schema
    creation and yoyo migration support. Records are serialized and stored in a
    JSONB column for fidelity, while individual fields are also stored as typed
    columns for efficient querying. Each model class maps to a table named
    after the schema name (usually 'volnux_{ClassName}').

    Features:
        - JSONB for serialized record state (with GIN indexing)
        - Typed columns for filterable fields
        - Native UPSERT (INSERT ... ON CONFLICT)
        - Connection pooling via PostgresConnector
        - Transaction support with savepoints
        - Django-style filter lookups (__gt, __contains, __in, etc.)
        - Yoyo migration support for schema evolution
        - Schema caching to avoid repeated information_schema queries

    Example:
        >>> backend = PostgresStoreBackend(
        ...     host="localhost",
        ...     port=5432,
        ...     database="volnux",
        ...     username="volnux",
        ...     password="secure_password",
        ...     use_pooling=True,
        ...     min_pool_size=2,
        ...     max_pool_size=10,
        ... )
        >>> backend.insert("volnux_Workflow", "wf_1", workflow_record)
        >>> workflow = backend.get("volnux_Workflow", "wf_1", Workflow)
        >>> active = backend.filter("volnux_Workflow", Workflow, status="active")
    """

    NAMESPACE_SEPARATOR = "_"

    connector_klass = PostgresConnector

    # PostgreSQL-specific type mapping
    TYPE_MAPPING = {
        bool: "BOOLEAN",
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        str: "TEXT",
        bytes: "BYTEA",
        dict: "JSONB",
        list: "JSONB",
    }

    def __init__(self, **connector_config: Any) -> None:
        """Initialize the PostgresSQL store backend.

        Args:
            **connector_config: Configuration passed to PostgresConnector.
                Required: host, database, username, password
                Optional: port (default 5432), use_pooling (default True),
                         min_pool_size, max_pool_size, ssl_mode, etc.
        """
        super().__init__(**connector_config)

        self._ensure_connected()

        # Schema existence cache to avoid repeated information_schema queries
        self._schema_cache: Dict[str, bool] = {}

    @contextmanager
    def _get_cursor(self):
        """Context manager for obtaining a cursor from the connector.

        Yields:
            A psycopg cursor. The cursor is closed after the context exits.
        """
        self._ensure_connected()
        cursor = self.connector.get_cursor()
        try:
            yield cursor
        finally:
            self.connector.return_cursor(cursor)

    @contextmanager
    def _transaction(self):
        """Context manager for explicit transactions.

        Uses the connector's native transaction support. If autocommit is
        enabled, the context manager still functions but without explicit
        BEGIN/COMMIT semantics.
        """
        self._ensure_connected()
        with self.connector.transaction():
            yield

    def _invalidate_schema_cache(self, schema_name: Optional[str] = None) -> None:
        """Invalidate the schema cache.

        Args:
            schema_name: Specific schema to invalidate, or None for all.
        """
        if schema_name:
            self._schema_cache.pop(schema_name, None)
        else:
            self._schema_cache.clear()

    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema (table) exists in the database.

        Uses PostgreSQL's information_schema for reliable cross-version
        compatibility. Results are cached to avoid repeated queries.

        Args:
            schema_name: The name of the schema (table) to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (schema_name,),
                )
                exists = cursor.fetchone() is not None

            self._schema_cache[schema_name] = exists
            return exists
        except Exception as e:
            logger.error("Error checking schema existence for '%s': %s", schema_name, e)
            return False

    def _map_field_to_pg_type(self, field_type: Any) -> str:
        """Map a Python/Formax field type to a PostgreSQL column type.

        Handles Optional types by unwrapping Union/Optional before mapping.

        Args:
            field_type: The type annotation from the model field.

        Returns:
            PostgreSQL column type string (e.g., 'TEXT', 'BIGINT', 'JSONB').
        """
        # Unwrap Optional[X] / Union[X, None]
        origin = get_origin(field_type)
        if origin in (Union, UnionType):
            args = get_args(field_type)
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                field_type = non_none[0]

        # Map to PostgreSQL type
        mapped = self.TYPE_MAPPING.get(field_type, "TEXT")

        # For enums, use TEXT (native PostgreSQL enums require CREATE TYPE)
        if isinstance(field_type, type) and issubclass(field_type, Enum):
            mapped = "TEXT"

        return mapped

    def _is_optional_field(self, field_type: Any) -> bool:
        """Check if a field type allows NULL values.

        Args:
            field_type: The type annotation from the model field.

        Returns:
            True if the field can be NULL, False otherwise.
        """
        origin = get_origin(field_type)
        if origin in (Union, UnionType):
            return type(None) in get_args(field_type)
        return False

    def create_schema(
        self,
        schema_name: str,
        record_class: Type["KeyValueStoreIntegrationMixin"],
        **kwargs,
    ) -> None:
        """Create a table with native FK, datetime, and list column support.

        Uses depth-first creation for FK dependencies. Mutual references
        fall back to software enforcement. No _record_state column.
        """
        from ..fields import OnDelete

        _creating = kwargs.get("_creating", set())

        self._ensure_connected()

        if schema_name in _creating:
            return

        _creating.add(schema_name)

        try:
            columns = ["id TEXT PRIMARY KEY"]
            fk_constraints: List[str] = []
            record_type_hints = get_type_hints(record_class)

            for field_name, field_type in record_type_hints.items():
                if field_name.startswith("_"):
                    continue

                field_type, attrib = self.decompose_field_type(field_type)
                metadata = attrib.metadata if attrib else {}
                meta_field_type = metadata.get("type")

                is_unique = metadata.get("unique", False)
                is_optional = self._is_optional_field(field_type) or metadata.get(
                    "nullable", False
                )

                if meta_field_type == "foreignkey":
                    has_native_fk = metadata.get("has_native_fk", False)

                    if has_native_fk:
                        target_model: Optional[
                            Type["KeyValueStoreIntegrationMixin"]
                        ] = metadata.get("target_model")
                        if target_model is None:
                            raise ValueError(
                                f"Target model missing for FK '{field_name}'"
                            )

                        target_schema = self.resolve_physical_target(
                            target_model.get_storage_route()
                        )
                        col_name = f"{field_name}_object_id"
                        col_def = f"{col_name} TEXT" + (
                            "" if is_optional else " NOT NULL"
                        )
                        columns.append(col_def)

                        if target_schema in _creating:
                            logger.warning(
                                "Mutual FK dependency: %s <-> %s. "
                                "Omitting native FK for '%s'; software enforcement active.",
                                schema_name,
                                target_schema,
                                field_name,
                            )
                            continue

                        if not self.schema_exists(schema_name):
                            self.create_schema(
                                target_schema, target_model, _creating=_creating
                            )
                            logger.info(
                                "Auto-created schema '%s' required by FK '%s.%s'",
                                target_schema,
                                record_class.__name__,
                                field_name,
                            )

                        on_delete: "OnDelete" = metadata.get(
                            "on_delete", OnDelete.PROTECT
                        )
                        on_delete_sql = {
                            OnDelete.CASCADE: "CASCADE",
                            OnDelete.SET_NULL: "SET NULL",
                            OnDelete.SET_DEFAULT: "SET DEFAULT",
                            OnDelete.PROTECT: "NO ACTION",
                            OnDelete.DO_NOTHING: "NO ACTION",
                        }.get(on_delete, "NO ACTION")

                        fk_constraints.append(
                            f"FOREIGN KEY ({col_name}) REFERENCES {target_schema}(id) "
                            f"ON DELETE {on_delete_sql}"
                        )
                    else:
                        # Software-enforced FK: Formax-Py descriptor serializes
                        # to JSON string via pre_formatter. Stored as opaque JSONB.

                        columns.append(
                            f"{field_name} JSONB" + ("" if is_optional else " NOT NULL")
                        )
                    continue

                if meta_field_type == "list":
                    col_def = f"{field_name} JSONB" + (
                        "" if is_optional else " NOT NULL"
                    )
                    columns.append(col_def)
                    continue

                if meta_field_type == "datetime":
                    pg_type = "TIMESTAMP WITH TIME ZONE"
                elif meta_field_type == "date":
                    pg_type = "DATE"
                else:
                    pg_type = self._map_field_to_pg_type(field_type)

                col_def = f"{field_name} {pg_type}"
                if not is_optional:
                    col_def += " NOT NULL"
                if is_unique:
                    col_def += " UNIQUE"
                columns.append(col_def)

            columns.extend(fk_constraints)
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {schema_name} ({', '.join(columns)})"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(create_sql)

            self._invalidate_schema_cache(schema_name)
            logger.info(
                "Created schema '%s' with %d columns", schema_name, len(columns)
            )

        except Exception as e:
            logger.error("Error creating schema '%s': %s", schema_name, e)
            raise SqlOperationError(
                f"Error creating schema '{schema_name}': {e}"
            ) from e

        finally:
            _creating.discard(schema_name)

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema (table) from the database.

        This is a destructive operation. Use with caution.

        Args:
            schema_name: The name of the schema to drop.

        Raises:
            SqlOperationError: If schema drop fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(f"DROP TABLE IF EXISTS {schema_name} CASCADE")

            self._invalidate_schema_cache(schema_name)
            logger.info("Dropped schema '%s'", schema_name)

        except Exception as e:
            logger.error("Error dropping schema '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error dropping schema '{schema_name}': {e}")

    def list_schemas(self) -> List[str]:
        """List all Volnux schemas (tables) in the database.

        Returns:
            List of schema names matching the 'volnux_%' pattern.
        """
        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name LIKE 'volnux_%' "
                    "ORDER BY table_name"
                )
                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error("Error listing schemas: %s", e)
            raise SqlOperationError(f"Error listing schemas: {e}")

    def supports_foreign_keys(self) -> bool:
        """Check if the backend supports foreign key constraints.

        Returns:
            True if foreign key constraints are supported, False otherwise.
        """
        return True

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to check.

        Returns:
            True if the record exists, False otherwise.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            return False

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT 1 FROM {schema_name} WHERE id = %s LIMIT 1",
                    (record_key,),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error("Error checking record existence: %s", e)
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """
        Inserts a record into the specified schema in the database. If the record already exists, it raises
        an `ObjectExistError`. This operation ensures that the schema is prepared before the insertion and
        executes the insertion atomically. Logs warnings and errors as required.

        :param schema_name: The name of the schema where the record should be inserted.
        :param record_key: The unique key associated with the record.
        :param record: The record instance to be inserted, adhering to the KeyValueStoreIntegrationMixin.
        :param ttl: Optional time-to-live for the record. Ignored as TTL is not supported for PostgreSQL.
        :return: None
        :raises ObjectExistError: If the record with the given key already exists in the specified schema.
        :raises SerializationError: If an unexpected serialization error occurs during the operation.
        :raises SqlOperationError: For any other errors encountered during the insert operation.
        """
        if ttl is not None:
            logger.warning("TTL not supported for PostgreSQL; ignoring")

        try:
            self.ensure_schema(schema_name, record.__class__)
            record_data = self._prepare_record_data(record, record_key)
            cols = list(record_data.keys())
            placeholders = [f"%({c})s" for c in cols]

            sql = (
                f"INSERT INTO {schema_name} ({', '.join(cols)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO NOTHING"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(sql, record_data)
                    if cursor.rowcount == 0:
                        raise ObjectExistError(
                            f"Record '{record_key}' already exists in schema '{schema_name}'"
                        )

            logger.debug("Inserted '%s' into '%s'", record_key, schema_name)
        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error inserting record: %s", e)
            raise SqlOperationError(f"Error inserting record: {e}") from e

    def update(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
    ) -> None:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            SqlOperationError: If the update fails.
        """
        self._ensure_connected()

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            record_data = self._prepare_record_data(record, record_key)

            # Build SET clause excluding the id column
            set_columns = [col for col in record_data.keys() if col != "id"]
            set_clause = ", ".join([f"{col} = %({col})s" for col in set_columns])

            update_sql = f"UPDATE {schema_name} SET {set_clause} WHERE id = %(id)s"

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(update_sql, record_data)

                    if cursor.rowcount == 0:
                        raise ObjectDoesNotExist(
                            f"Record '{record_key}' does not exist in schema '{schema_name}'"
                        )

            logger.debug("Updated record '%s' in schema '%s'", record_key, schema_name)

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error updating record: %s", e)
            raise SqlOperationError(f"Error updating record: {e}")

    def upsert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
    ) -> None:
        """Insert or update a record (upsert operation).

        Uses PostgreSQL's native INSERT ... ON CONFLICT for atomic upsert.

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.

        Raises:
            SerializationError: If serialization fails.
            SqlOperationError: If upsert fails.
        """
        self._ensure_connected()

        try:
            # Auto-create schema if it doesn't exist
            self.ensure_schema(schema_name, record.__class__)

            record_data = self._prepare_record_data(record, record_key)
            columns = list(record_data.keys())
            placeholders = [f"%({col})s" for col in columns]

            # Build UPDATE SET for conflict resolution
            update_columns = [col for col in columns if col != "id"]
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )

            upsert_sql = (
                f"INSERT INTO {schema_name} ({', '.join(columns)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO UPDATE SET {update_clause}"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(upsert_sql, record_data)

            logger.debug("Upserted record '%s' in schema '%s'", record_key, schema_name)

        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error upserting record: %s", e)
            raise SqlOperationError(f"Error upserting record: {e}")

    def delete(self, schema_name: str, record_key: str) -> None:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SqlOperationError: If deletion fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM {schema_name} WHERE id = %s", (record_key,)
                    )

                    if cursor.rowcount == 0:
                        raise ObjectDoesNotExist(
                            f"Record '{record_key}' does not exist in schema '{schema_name}'"
                        )

            logger.debug(
                "Deleted record '%s' from schema '%s'", record_key, schema_name
            )

        except ObjectDoesNotExist:
            raise
        except Exception as e:
            logger.error("Error deleting record: %s", e)
            raise SqlOperationError(f"Error deleting record: {e}")

    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If retrieval fails.
        """
        self._ensure_connected()

        try:
            with self._get_cursor() as cursor:
                cursor.execute(
                    f"SELECT * FROM {schema_name} WHERE id = %s",
                    (record_key,),
                )
                row = cursor.fetchone()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            return self._deserialize_record(
                self._sqlite_row_and_tuple_to_dict(row, cursor), record_klass
            )
        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except Exception as e:
            logger.error("Error getting record: %s", e)
            raise SqlOperationError(f"Error getting record: {e}")

    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            ObjectDoesNotExist: If the record no longer exists.
            SerializationError: If deserialization fails.
            SqlOperationError: If reload fails.
        """
        fresh = self.get(schema_name, record.id, record.__class__)
        if not fresh:
            raise ObjectDoesNotExist(
                f"Record '{record.id}' no longer exists in schema '{schema_name}'"
            )
        record.__setstate__(fresh.__getstate__())
        return record

    @staticmethod
    def _escape_like(value: str) -> str:
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _build_sql_filter(self, filter_kwargs: Dict[str, Any]) -> Tuple[str, List[Any]]:
        if not filter_kwargs:
            return "TRUE", []

        conditions: List[str] = []
        parameters: List[Any] = []

        for key, value in filter_kwargs.items():
            if "__" in key:
                field, operator = key.rsplit("__", 1)

                if operator == "in":
                    if not isinstance(value, (list, tuple)) or not value:
                        conditions.append("FALSE")
                        continue
                    placeholders = ", ".join(["%s"] * len(value))
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)

                elif operator == "isnull":
                    conditions.append(f"{field} IS {'NULL' if value else 'NOT NULL'}")

                elif operator in (
                    "contains",
                    "icontains",
                    "startswith",
                    "istartswith",
                    "endswith",
                    "iendswith",
                ):
                    escaped = self._escape_like(str(value))
                    ilike = operator.startswith("i")
                    kw = "ILIKE" if ilike else "LIKE"

                    if "startswith" in operator:
                        pattern = f"{escaped}%"
                    elif "endswith" in operator:
                        pattern = f"%{escaped}"
                    else:
                        pattern = f"%{escaped}%"

                    conditions.append(f"{field} {kw} %s ESCAPE '\\'")
                    parameters.append(pattern)

                elif operator == "exact":
                    conditions.append(f"{field} = %s")
                    parameters.append(value)
                elif operator == "gt":
                    conditions.append(f"{field} > %s")
                    parameters.append(value)
                elif operator == "gte":
                    conditions.append(f"{field} >= %s")
                    parameters.append(value)
                elif operator == "lt":
                    conditions.append(f"{field} < %s")
                    parameters.append(value)
                elif operator == "lte":
                    conditions.append(f"{field} <= %s")
                    parameters.append(value)
                elif operator == "ne":
                    conditions.append(f"{field} != %s")
                    parameters.append(value)
                else:
                    logger.warning("Unknown filter operator: %s", operator)
                    conditions.append(f"{field} = %s")
                    parameters.append(value)
            else:
                if isinstance(value, Enum):
                    value = value.value
                conditions.append(f"{key} = %s")
                parameters.append(value)

        return " AND ".join(conditions), parameters

    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        **filter_kwargs: Any,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        """Filter records matching the specified criteria.

        Results are deserialized from the _record_state JSONB column.

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Field to order by. Prefix with '-' for descending.
            **filter_kwargs: Field lookup filters.

        Returns:
            List of matching record instances.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If the query fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)

            query = f"SELECT id FROM {schema_name} WHERE {where_clause}"

            if order_by:
                if order_by.startswith("-"):
                    query += f" ORDER BY {order_by[1:]} DESC"
                else:
                    query += f" ORDER BY {order_by} ASC"

            if limit is not None:
                query += f" LIMIT %s"
                parameters.append(limit)
            if offset is not None:
                query += f" OFFSET %s"
                parameters.append(offset)

            with self._get_cursor() as cursor:
                cursor.execute(query, parameters)
                rows = cursor.fetchall()

            return self._create_result_stream(
                record_keys=[row[0] for row in rows], record_klass=record_klass
            )

        except ObjectDoesNotExist:
            raise
        except Exception as e:
            logger.error("Error filtering records: %s", e)
            raise SqlOperationError(f"Error filtering records: {e}")

    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            record_klass: The class to instantiate the record with.
            **filter_kwargs: Optional field lookup filters.

        Returns:
            The number of matching records.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SqlOperationError: If the count fails.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT COUNT(*) FROM {schema_name} WHERE {where_clause}"

            with self._get_cursor() as cursor:
                cursor.execute(query, parameters)
                result = cursor.fetchone()

            return int(result[0]) if result else 0

        except Exception as e:
            logger.error("Error counting records: %s", e)
            raise SqlOperationError(f"Error counting records: {e}")

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        """Insert multiple records in a single transaction.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Optional TTL (not implemented for PostgreSQL).

        Returns:
            Number of records inserted.

        Raises:
            SqlOperationError: If bulk insert fails.
        """
        self._ensure_connected()

        if not records:
            return 0

        if ttl is not None:
            logger.warning("TTL is not supported for PostgreSQL backend; ignoring")

        try:
            # Use the first record to ensure schema exists
            first_record = next(iter(records.values()))
            self.ensure_schema(schema_name, first_record.__class__)

            # Prepare all records
            all_data = []
            for record_key, record in records.items():
                record_data = self._prepare_record_data(record, record_key)
                all_data.append(record_data)

            if not all_data:
                return 0

            columns = list(all_data[0].keys())
            placeholders = [f"%({col})s" for col in columns]
            insert_sql = (
                f"INSERT INTO {schema_name} ({', '.join(columns)}) "
                f"VALUES ({', '.join(placeholders)}) "
                f"ON CONFLICT (id) DO NOTHING"
            )

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.executemany(insert_sql, all_data)

            logger.info(
                "Bulk inserted %d records into schema '%s'", len(all_data), schema_name
            )
            return len(all_data)

        except Exception as e:
            logger.error("Error bulk inserting records: %s", e)
            raise SqlOperationError(f"Error bulk inserting records: {e}")

    def bulk_delete(self, schema_name: str, record_keys: List[str]) -> int:
        """Delete multiple records in a single transaction.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Returns:
            Number of records deleted.

        Raises:
            SqlOperationError: If bulk delete fails.
        """
        self._ensure_connected()

        if not record_keys:
            return 0

        try:
            placeholders = ", ".join(["%s"] * len(record_keys))
            delete_sql = f"DELETE FROM {schema_name} WHERE id IN ({placeholders})"

            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(delete_sql, record_keys)
                    deleted = cursor.rowcount

            logger.info(
                "Bulk deleted %d records from schema '%s'", deleted, schema_name
            )
            return deleted

        except Exception as e:
            logger.error("Error bulk deleting records: %s", e)
            raise SqlOperationError(f"Error bulk deleting records: {e}")

    def clear_schema(self, schema_name: str) -> int:
        """Delete all records from a schema without dropping the table.

        Args:
            schema_name: The schema to clear.

        Returns:
            Number of records deleted.

        Raises:
            SqlOperationError: If the operation fails.
        """
        self._ensure_connected()

        try:
            with self._transaction():
                with self._get_cursor() as cursor:
                    cursor.execute(f"DELETE FROM {schema_name}")
                    deleted = cursor.rowcount

            logger.warning(
                "Cleared all %d records from schema '%s'", deleted, schema_name
            )
            return deleted

        except Exception as e:
            logger.error("Error clearing schema '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error clearing schema '{schema_name}': {e}")

    def get_table_info(self, schema_name: str) -> Dict[str, Any]:
        """Get information about a schema/table.

        Args:
            schema_name: The schema name.

        Returns:
            Dictionary with table metadata.

        Raises:
            SqlOperationError: If the query fails.
        """
        self._ensure_connected()

        try:
            info: Dict[str, Any] = {"schema": schema_name, "exists": False}

            with self._get_cursor() as cursor:
                # Check existence
                cursor.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (schema_name,),
                )
                if cursor.fetchone() is None:
                    return info

                info["exists"] = True

                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {schema_name}")
                info["row_count"] = cursor.fetchone()[0]

                # Get column information
                cursor.execute(
                    "SELECT column_name, data_type, is_nullable, column_default "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (schema_name,),
                )
                info["columns"] = [
                    {
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "YES",
                        "default": row[3],
                    }
                    for row in cursor.fetchall()
                ]

                # Get index information
                cursor.execute(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE schemaname = 'public' AND tablename = %s",
                    (schema_name,),
                )
                info["indexes"] = [
                    {"name": row[0], "definition": row[1]} for row in cursor.fetchall()
                ]

                # Get table size
                cursor.execute(
                    "SELECT pg_total_relation_size(%s)",
                    (schema_name,),
                )
                info["total_size_bytes"] = cursor.fetchone()[0]

            return info

        except Exception as e:
            logger.error("Error getting table info for '%s': %s", schema_name, e)
            raise SqlOperationError(f"Error getting table info: {e}")

    def __repr__(self) -> str:
        connector_info = self.connector.get_connection_info() if self.connector else {}
        host = connector_info.get("host", "unknown")
        port = connector_info.get("port", "unknown")
        database = connector_info.get("database", "unknown")
        pooling = "pooled" if connector_info.get("pooling_enabled", False) else "direct"
        status = (
            "connected" if connector_info.get("is_connected", False) else "disconnected"
        )
        return f"<PostgresStoreBackend {host}:{port}/{database} [{pooling}, {status}]>"
