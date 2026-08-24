import orjson as json
import logging
import sqlite3
from types import UnionType
from typing import (
    Any,
    Dict,
    List,
    Set,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    TYPE_CHECKING,
)

from formax.typing import get_type

from volnux.backends.connectors.sqlite import SqliteConnector
from volnux.backends.store import KeyValueStoreBackendBase, YoyoMigrationsMixin
from volnux.exceptions import (
    ObjectDoesNotExist,
    ObjectExistError,
    SqlOperationError,
    SerializationError,
)

if TYPE_CHECKING:
    from ..fields import OnDelete
    from volnux.result.stream import ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


class SqliteStoreBackend(YoyoMigrationsMixin, KeyValueStoreBackendBase):
    """SQLite-backed key-value store implementation.

    This backend uses SQLite tables to store records, with automatic schema
    creation and migration support. Records are serialized and
    stored in a dedicated column, while individual fields are also stored
    for efficient querying.

    Example:
        >>> backend = SqliteStoreBackend(database="myapp.db")
        >>> backend.insert("users", "user_1", user_record)
        >>> user = backend.get("users", UserModel, "user_1")
        >>> active_users = backend.filter("users", UserModel, status="active")
    """

    NAMESPACE_SEPARATOR = "_"

    connector_klass = SqliteConnector

    def __init__(self, **connector_config: Any):
        """Initialize the SQLite store backend.

        Args:
            **connector_config: Configuration passed to SqliteConnector.
        """
        super().__init__(**connector_config)

        self._ensure_connected()

        self._schema_cache: Dict[str, bool] = {}

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

        Args:
            schema_name: The name of the schema to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        if schema_name in self._schema_cache:
            return self._schema_cache[schema_name]

        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (schema_name,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()

            self._schema_cache[schema_name] = exists
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking schema existence: {e}")
            return False

    def _map_python_type_to_sql(self, field_type: Any) -> str:
        """Map Python types to SQLite types.

        Args:
            field_type: The Python type annotation.

        Returns:
            The corresponding SQLite type as a string.
        """
        field_type = get_type(field_type)

        type_mapping = {
            bool: "BOOLEAN",
            int: "INTEGER",
            float: "REAL",
            str: "TEXT",
            bytes: "BLOB",
        }

        return type_mapping.get(field_type, "TEXT")  # Default to TEXT for JSON

    def _is_optional_field(self, field_type: Any) -> bool:
        """Check if a field type allows None."""
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
        """
        Create a schema (table) based on a record's structure.

        Uses a two-phase approach for FK dependencies: dependent schemas are
        created first (columns only, no FKs), then FK constraints are added
        inline when the referencing table is created. If a mutual reference
        is detected (A -> B -> A), the back-reference FK is omitted and
        falls back to software-level enforcement via _on_delete_hook.

        Args:
            schema_name: The name of the schema to create.
            record_class: A sample record to derive the schema from.
            kwargs: Additional keyword arguments.
                    _creating: Internal set tracking schemas currently being created,
                                used to detect and break mutual FK dependency cycles.

        Raises:
            SqlOperationError: If schema creation fails.
        """
        self._ensure_connected()

        # Create isolated tracking set at top-level call only
        _creating: Set[str] = kwargs.get("_creating", set())

        # Mark this schema as in-progress before processing its fields,
        # so recursive calls for dependent schemas can detect the cycle.
        _creating.add(schema_name)

        try:
            from ..fields import OnDelete

            fields = ["id TEXT PRIMARY KEY"]
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
                        field_def = f"{col_name} TEXT" + (
                            "" if is_optional else " NOT NULL"
                        )
                        fields.append(field_def)

                        if target_schema in _creating:
                            logger.warning(
                                "Mutual FK dependency: %s <-> %s. "
                                "Omitting native FK for '%s'; software enforcement active.",
                                schema_name,
                                target_schema,
                                field_name,
                            )
                            continue

                        if not self.schema_exists(target_schema):
                            self.create_schema(
                                target_schema, target_model, _creating=_creating
                            )
                            logger.info(
                                f"Auto-created schema '{target_schema}' required by FK "
                                f"'{record_class.__name__}.{field_name}'"
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
                        continue
                    else:
                        # Software level Constraint. It contains a JSON describing the relationship
                        # {"object_id": "dfd","model_path": "volnux.model.Workflow","backend_alias": "default"}
                        sql_type = "TEXT"
                elif meta_field_type == "list":
                    sql_type = "TEXT"
                elif meta_field_type in ["datetime", "date"]:
                    sql_type = "TEXT"
                else:
                    sql_type = self._map_python_type_to_sql(field_type)

                field_def = f"{field_name} {sql_type}"
                if not is_optional:
                    field_def += " NOT NULL"
                if is_unique:
                    field_def += " UNIQUE"
                fields.append(field_def)

            fields.extend(fk_constraints)
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {schema_name} ({', '.join(fields)})"
            )
            print(create_sql)
            self.execute_query(create_sql, fetch_method=None)
            self._invalidate_schema_cache(schema_name)

        except Exception as e:
            logger.error(f"Error creating schema '{schema_name}': {e}")
            raise SqlOperationError(f"Error creating schema: {e}") from e

    def drop_schema(self, schema_name: str) -> None:
        """Drop a schema (table) from the database.

        Args:
            schema_name: The name of the schema to drop.

        Raises:
            SqlOperationError: If a schema drop fails.
        """
        self._ensure_connected()

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {schema_name}")
                cursor.close()

            self._invalidate_schema_cache(schema_name)
            logger.info(f"Dropped schema '{schema_name}'")

        except sqlite3.Error as e:
            logger.error(f"Error dropping schema '{schema_name}': {e}")
            raise SqlOperationError(f"Error dropping schema: {e}")

    def list_schemas(self) -> List[str]:
        """List all schemas (tables) in the database.

        Returns:
            List of schema names.
        """
        self._ensure_connected()

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return schemas
        except sqlite3.Error as e:
            logger.error(f"Error listing schemas: {e}")
            raise SqlOperationError(f"Error listing schemas: {e}")

    def supports_foreign_keys(self) -> bool:
        return True

    def execute_query(
        self, query: str, *args, fetch_method: Optional[str] = "fetchall", **kwargs
    ) -> Any:
        """
        Executes a SQL query using a database connector in a transaction-safe manner.

        :param query: The SQL query to execute.
        :type query: str
        :param args: Positional arguments to pass to the SQL query.
        :type args: tuple
        :param fetch_method: Specifies the fetch method, accepted values are 'fetchall',
            or 'fetchone'. Defaults to 'fetchall'.
        :type fetch_method: str
        :param kwargs: Optional keyword arguments to pass to the SQL execution.
        :type kwargs: dict
        :return: Query result based on the fetch method, or None if no results are found
            or the fetch method does not apply.
        :rtype: Any
        :raises SqlOperationError: If there is an issue executing the SQL query.
        """
        with self.connector.transaction():
            try:
                cursor = self.connector.get_cursor()
                cursor.execute(query, *args, **kwargs)
                if fetch_method == "fetchall":
                    return cursor.fetchall()
                elif fetch_method == "fetchone":
                    return cursor.fetchone()
                return None
            except sqlite3.Error as e:
                logger.error(f"Error executing query: {e}")
                raise SqlOperationError(f"Error executing query: {e}")
            finally:
                if cursor:
                    cursor.close()

    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to check.

        Returns:
            True if the record exists, False otherwise.
        """
        self._ensure_connected()

        if not self.schema_exists(schema_name):
            return False

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(
                f"SELECT 1 FROM {schema_name} WHERE id = ? LIMIT 1",
                (record_key,),
            )
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except sqlite3.Error as e:
            logger.error(f"Error checking record existence: {e}")
            return False

    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema to insert into.
            record_key: The unique key for the record.
            record: The record object to insert.
            ttl: Time to live in seconds for the record. If None, no TTL is set.

        Raises:
            ObjectExistError: If a record with the same key already exists.
            SerializationError: If serialization fails.
            SqlOperationError: If insertion fails.
        """
        self._ensure_connected()

        try:
            self.ensure_schema(schema_name, record.__class__)

            if self.exists(schema_name, record_key):
                raise ObjectExistError(
                    f"Record '{record_key}' already exists in schema '{schema_name}'"
                )

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            values = [record_data[field] for field in fields]

            insert_sql = f"""
                INSERT INTO {schema_name} ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(insert_sql, values)
                cursor.close()

            logger.debug(f"Inserted record '{record_key}' into schema '{schema_name}'")

        except ObjectExistError:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error inserting record: {e}")
            raise SqlOperationError(f"Error inserting record: {e}")

    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Update an existing record in the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to update.
            record: The updated record object.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SerializationError: If serialization fails.
            SqlOperationError: If the update fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.exists(schema_name, record_key):
            raise ObjectDoesNotExist(
                f"Record '{record_key}' does not exist in schema '{schema_name}'"
            )

        try:
            record_data = self._prepare_record_data(record, record_key)

            set_fields = [
                f"{field} = ?" for field in record_data.keys() if field != "id"
            ]
            values = [
                record_data[field] for field in record_data.keys() if field != "id"
            ]
            values.append(record_key)

            update_sql = f"""
                UPDATE {schema_name}
                SET {', '.join(set_fields)}
                WHERE id = ?
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(update_sql, values)
                return cursor.rowcount

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error updating record: {e}")
            raise SqlOperationError(f"Error updating record: {e}")
        finally:
            if cursor:
                cursor.close()

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema for the operation.
            record_key: The key of the record.
            record: The record object to upsert.

        Raises:
            SerializationError: If serialization fails.
            SqlOperationError: If upsert fails.
        """
        self._ensure_connected()

        cursor = None

        try:
            self.ensure_schema(schema_name, record.__class__)

            record_data = self._prepare_record_data(record, record_key)

            fields = list(record_data.keys())
            placeholders = ["?" for _ in fields]
            update_fields = [
                f"{field} = excluded.{field}" for field in fields if field != "id"
            ]
            values = [record_data[field] for field in fields]

            upsert_sql = f"""
                INSERT INTO {schema_name} ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT(id) DO UPDATE SET
                {', '.join(update_fields)}
            """

            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(upsert_sql, values)
                return cursor.rowcount

        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error upserting record: {e}")
            raise SqlOperationError(f"Error upserting record: {e}")
        finally:
            if cursor:
                cursor.close()

    def delete(self, schema_name: str, record_key: str) -> Optional[int]:
        """Delete a record from the store.

        Args:
            schema_name: The schema containing the record.
            record_key: The key of the record is to delete.

        Raises:
            ObjectDoesNotExist: If the record does not exist.
            SqlOperationError: If deletion fails.
        """
        self._ensure_connected()

        cursor = None

        try:
            with self.connector.transaction():
                cursor = self.connector.get_cursor()
                cursor.execute(f"DELETE FROM {schema_name} WHERE id = ?", (record_key,))

                return cursor.rowcount
        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error deleting record: {e}")
            raise SqlOperationError(f"Error deleting record: {e}")
        finally:
            if cursor:
                cursor.close()

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

        cursor = None

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(f"SELECT * FROM {schema_name} WHERE id = ?", (record_key,))
            row = cursor.fetchone()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' does not exist in schema '{schema_name}'"
                )

            record = self._deserialize_record(
                self._sqlite_row_and_tuple_to_dict(row, cursor), record_klass
            )
            logger.debug(f"Retrieved record '{record_key}' from schema '{schema_name}'")
            return record

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error getting record: {e}")
            raise SqlOperationError(f"Error getting record: {e}")
        finally:
            if cursor:
                cursor.close()

    @staticmethod
    def _escape_like(value: str) -> str:
        """Escape SQL LIKE metacharacters to prevent wildcard injection."""
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _build_sql_filter(self, filter_kwargs: Dict[str, Any]) -> Tuple[str, List[Any]]:
        if not filter_kwargs:
            return "1", []

        conditions: List[str] = []
        parameters: List[Any] = []

        for key, value in filter_kwargs.items():
            if "__" in key:
                field, operator = key.rsplit("__", 1)

                if operator == "in":
                    if not value:
                        conditions.append("0")  # Empty IN = no matches
                        continue
                    placeholders = ",".join(["?" for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    parameters.extend(value)

                elif operator in (
                    "contains",
                    "icontains",
                    "startswith",
                    "istartswith",
                    "endswith",
                    "iendswith",
                ):
                    escaped = self._escape_like(str(value))
                    nocase = " COLLATE NOCASE" if operator.startswith("i") else ""

                    if "startswith" in operator:
                        pattern = f"{escaped}%"
                    elif "endswith" in operator:
                        pattern = f"%{escaped}"
                    else:
                        pattern = f"%{escaped}%"

                    conditions.append(f"{field} LIKE ?{nocase} ESCAPE '\\'")
                    parameters.append(pattern)

                elif operator == "exact":
                    conditions.append(f"{field} = ?")
                    parameters.append(value)
                elif operator == "gt":
                    conditions.append(f"{field} > ?")
                    parameters.append(value)
                elif operator == "gte":
                    conditions.append(f"{field} >= ?")
                    parameters.append(value)
                elif operator == "lt":
                    conditions.append(f"{field} < ?")
                    parameters.append(value)
                elif operator == "lte":
                    conditions.append(f"{field} <= ?")
                    parameters.append(value)
                elif operator == "ne":
                    conditions.append(f"{field} != ?")
                    parameters.append(value)
                elif operator == "isnull":
                    conditions.append(f"{field} IS {'NULL' if value else 'NOT NULL'}")
                else:
                    logger.warning("Unknown filter operator: %s", operator)
                    conditions.append(f"{field} = ?")
                    parameters.append(value)
            else:
                conditions.append(f"{key} = ?")
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

        Args:
            schema_name: The schema to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return.
            offset: Number of records to skip.
            order_by: Field to order by (prefix with '-' for descending).
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            List of matching record instances.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SerializationError: If deserialization fails.
            SqlOperationError: If a query fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:

            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT id FROM {schema_name}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            if order_by:
                if order_by.startswith("-"):
                    query += f" ORDER BY {order_by[1:]} DESC"
                else:
                    query += f" ORDER BY {order_by} ASC"

            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            rows = cursor.fetchall()

            return self._create_result_stream(
                record_keys=[row[0] for row in rows], record_klass=record_klass
            )

        except ObjectDoesNotExist:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error filtering records: {e}")
            raise SqlOperationError(f"Error filtering records: {e}")
        finally:
            if cursor is not None:
                cursor.close()

    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema to count within.
            record_klass: The record class to count.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.

        Raises:
            ObjectDoesNotExist: If schema doesn't exist.
            SqlOperationError: If the count fails.
        """
        self._ensure_connected()

        cursor = None

        if not self.schema_exists(schema_name):
            raise ObjectDoesNotExist(f"Schema '{schema_name}' does not exist")

        try:
            where_clause, parameters = self._build_sql_filter(filter_kwargs)
            query = f"SELECT COUNT(*) FROM {schema_name}"

            if where_clause != "1":
                query += f" WHERE {where_clause}"

            cursor = self.connector.get_cursor()
            cursor.execute(query, parameters)
            result = cursor.fetchone()

            return int(result[0]) if result else 0

        except sqlite3.Error as e:
            logger.error(f"Error counting records: {e}")
            raise SqlOperationError(f"Error counting records: {e}")
        finally:
            if cursor:
                cursor.close()

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
        if not hasattr(record, "id"):
            raise ValueError("Record must have an 'id' attribute for reload")

        record_key = record.id
        self._ensure_connected()

        cursor = None

        try:
            cursor = self.connector.get_cursor()
            cursor.execute(f"SELECT * FROM {schema_name} WHERE id = ?", (record_key,))
            row = cursor.fetchone()

            if row is None:
                raise ObjectDoesNotExist(
                    f"Record '{record_key}' no longer exists in schema '{schema_name}'"
                )

            return self._deserialize_record(
                self._sqlite_row_and_tuple_to_dict(row, cursor), record.__class__
            )

        except ObjectDoesNotExist:
            raise
        except SerializationError:
            raise
        except sqlite3.Error as e:
            logger.error(f"Error reloading record: {e}")
            raise SqlOperationError(f"Error reloading record: {e}")
        finally:
            if cursor:
                cursor.close()
