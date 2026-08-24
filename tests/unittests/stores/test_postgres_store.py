"""
Pytest unit tests for PostgresStoreBackend.

Requires a running PostgreSQL instance. Configure connection via
environment variables or a pytest fixture override.

Environment variables:
    VOLNUX_TEST_PG_HOST     (default: localhost)
    VOLNUX_TEST_PG_PORT     (default: 5432)
    VOLNUX_TEST_PG_DATABASE (default: volnux_test)
    VOLNUX_TEST_PG_USERNAME (default: volnux)
    VOLNUX_TEST_PG_PASSWORD (default: volnux_test_password)

Or use the docker-compose provided test database.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Type, Union
from uuid import uuid4

import pytest

from formax import BaseModel, MiniAnnotated, Attrib, InitStrategy, ValidationFlags

from volnux.backends.base import (
    KeyValueStoreBackendBase,
    KeyValueStoreIntegrationMixin,
    ObjectDoesNotExist,
    ObjectExistError,
    SerializationError,
    SqlOperationError,
)
from volnux.backends.postgres import PostgresStoreBackend


# ============================================================
# TEST MODEL CLASSES
# ============================================================

class TestStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    ARCHIVED = "archived"


class TestCategory(str, Enum):
    STANDARD = "standard"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class TestUser(KeyValueStoreIntegrationMixin, BaseModel):
    """Minimal test model with common field types."""

    name: MiniAnnotated[str, Attrib(min_length=1, max_length=255)]
    email: str
    age: int = 0
    is_active: bool = True
    score: float = 0.0
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: TestStatus = TestStatus.PENDING
    created_at: float = field(
        default_factory=lambda: datetime.now(timezone.utc).timestamp()
    )

    is_persisted: bool = False
    autosave: bool = False

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)


class TestWorkflow(KeyValueStoreIntegrationMixin, BaseModel):
    """Test model with optional fields and nested structures."""

    name: str
    organization_id: str
    team_id: str
    description: Optional[str] = None
    category: TestCategory = TestCategory.STANDARD
    status: TestStatus = TestStatus.PENDING
    config: Dict[str, Any] = field(default_factory=dict)
    priority: Optional[int] = None
    created_by: str = "system"
    updated_by: Optional[str] = None
    created_at: float = field(
        default_factory=lambda: datetime.now(timezone.utc).timestamp()
    )

    is_persisted: bool = False
    autosave: bool = False

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)


class TestAuditEntry(KeyValueStoreIntegrationMixin, BaseModel):
    """Test model for immutable/appending patterns."""

    event_type: str
    actor_id: str
    actor_role: str
    target_type: str
    target_id: str
    action: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    hash: str = ""
    previous_hash: Optional[str] = None
    created_at: float = field(
        default_factory=lambda: datetime.now(timezone.utc).timestamp()
    )

    is_persisted: bool = False
    autosave: bool = False

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture(scope="session")
def pg_config() -> Dict[str, Any]:
    """PostgreSQL connection configuration from environment."""
    return {
        "host": os.getenv("VOLNUX_TEST_PG_HOST", "localhost"),
        "port": int(os.getenv("VOLNUX_TEST_PG_PORT", "5432")),
        "database": os.getenv("VOLNUX_TEST_PG_DATABASE", "volnux_test"),
        "username": os.getenv("VOLNUX_TEST_PG_USERNAME", "volnux"),
        "password": os.getenv("VOLNUX_TEST_PG_PASSWORD", "volnux_test_password"),
    }


@pytest.fixture(scope="function")
def backend(pg_config: Dict[str, Any]) -> PostgresStoreBackend:
    """Create a fresh backend instance per test.

    Each test gets its own backend. Schemas are cleaned up after each test.
    """
    backend = PostgresStoreBackend(**pg_config)
    yield backend
    # Cleanup: drop all volnux_test_* schemas created during the test
    try:
        schemas = backend.list_schemas()
        for schema in schemas:
            if schema.startswith("volnux_test_"):
                try:
                    backend.drop_schema(schema)
                except Exception:
                    pass
    except Exception:
        pass
    backend.close()


@pytest.fixture
def user_schema() -> str:
    """Unique schema name for user model tests."""
    return f"volnux_test_User_{uuid4().hex[:8]}"


@pytest.fixture
def workflow_schema() -> str:
    """Unique schema name for workflow model tests."""
    return f"volnux_test_Workflow_{uuid4().hex[:8]}"


@pytest.fixture
def audit_schema() -> str:
    """Unique schema name for audit model tests."""
    return f"volnux_test_Audit_{uuid4().hex[:8]}"


@pytest.fixture
def sample_user() -> TestUser:
    """Create a sample user instance."""
    return TestUser(
        name="Alice Test",
        email="alice@test.com",
        age=30,
        is_active=True,
        score=95.5,
        tags=["developer", "admin"],
        metadata={"department": "engineering", "level": 3},
        status=TestStatus.ACTIVE,
    )


@pytest.fixture
def sample_workflow() -> TestWorkflow:
    """Create a sample workflow instance."""
    return TestWorkflow(
        name="ETL Pipeline",
        organization_id="org-001",
        team_id="team-engineering",
        description="Daily ETL workflow for customer data",
        category=TestCategory.ENTERPRISE,
        status=TestStatus.ACTIVE,
        config={"source": "snowflake", "target": "bigquery", "schedule": "0 6 * * *"},
        priority=1,
        created_by="user-001",
    )


@pytest.fixture
def sample_audit_entry() -> TestAuditEntry:
    """Create a sample audit entry."""
    return TestAuditEntry(
        event_type="workflow_created",
        actor_id="user-001",
        actor_role="workflow_author",
        target_type="workflow",
        target_id="wf-001",
        action="create",
        metadata={"version": "1.0.0", "team": "engineering"},
    )


# ============================================================
# CONNECTION AND LIFECYCLE TESTS
# ============================================================

class TestConnection:
    """Test backend connection and lifecycle."""

    def test_connect_and_ping(self, backend: PostgresStoreBackend):
        """Verify the backend connects and responds to ping."""
        assert backend.connector.is_connected()
        assert backend.connector.ping()

    def test_close_and_reconnect(self, backend: PostgresStoreBackend):
        """Verify backend can close and reconnect."""
        backend.close()
        assert not backend.connector.is_connected()

        backend.connector.connect()
        assert backend.connector.is_connected()

    def test_context_manager(self, pg_config: Dict[str, Any]):
        """Verify backend works as a context manager."""
        with PostgresStoreBackend(**pg_config) as be:
            assert be.connector.is_connected()
        # After context exit, should be closed
        assert not be.connector.is_connected()


# ============================================================
# SCHEMA MANAGEMENT TESTS
# ============================================================

class TestSchemaManagement:
    """Test schema (table) lifecycle operations."""

    def test_schema_not_exists_initially(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify a random schema doesn't exist initially."""
        assert not backend.schema_exists(user_schema)

    def test_create_schema(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify schema creation from a record."""
        backend.create_schema(user_schema, sample_user)
        assert backend.schema_exists(user_schema)

    def test_create_schema_twice_is_idempotent(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify creating an existing schema raises ObjectExistError."""
        backend.create_schema(user_schema, sample_user)
        with pytest.raises(SqlOperationError):
            backend.create_schema(user_schema, sample_user)

    def test_drop_schema(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify schema can be dropped."""
        backend.create_schema(user_schema, sample_user)
        assert backend.schema_exists(user_schema)

        backend.drop_schema(user_schema)
        assert not backend.schema_exists(user_schema)

    def test_drop_nonexistent_schema_no_error(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify dropping a nonexistent schema doesn't raise."""
        backend.drop_schema(user_schema)  # Should not raise

    def test_list_schemas(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify schema listing includes created schemas."""
        backend.create_schema(user_schema, sample_user)
        schemas = backend.list_schemas()
        assert user_schema in schemas

    def test_ensure_schema_creates_if_not_exists(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify ensure_schema creates the schema when it doesn't exist."""
        assert not backend.schema_exists(user_schema)
        backend.ensure_schema(user_schema, sample_user)
        assert backend.schema_exists(user_schema)

    def test_ensure_schema_noop_if_exists(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify ensure_schema is a no-op when schema already exists."""
        backend.create_schema(user_schema, sample_user)
        # Should not raise
        backend.ensure_schema(user_schema, sample_user)

    def test_schema_caching(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify schema existence is cached after first check."""
        # First check populates cache
        assert not backend.schema_exists(user_schema)
        assert user_schema in backend._schema_cache
        assert not backend._schema_cache[user_schema]

        backend.create_schema(user_schema, sample_user)
        # Cache still says False because it wasn't invalidated
        # Manually invalidate and recheck
        backend._invalidate_schema_cache(user_schema)
        assert backend.schema_exists(user_schema)
        assert backend._schema_cache[user_schema]

    def test_get_table_info(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify table info returns correct metadata."""
        backend.create_schema(user_schema, sample_user)
        info = backend.get_table_info(user_schema)

        assert info["exists"]
        assert info["schema"] == user_schema
        assert info["row_count"] == 0
        assert len(info["columns"]) > 0
        assert any(col["name"] == "id" for col in info["columns"])
        assert any(col["name"] == "_record_state" for col in info["columns"])


# ============================================================
# INSERT TESTS
# ============================================================

class TestInsert:
    """Test record insertion operations."""

    def test_insert_single_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify a single record can be inserted."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)
        assert backend.exists(user_schema, record_id)

    def test_insert_creates_schema_automatically(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify insert auto-creates the schema if it doesn't exist."""
        assert not backend.schema_exists(user_schema)
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)
        assert backend.schema_exists(user_schema)

    def test_insert_duplicate_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify inserting a duplicate key raises ObjectExistError."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        with pytest.raises(ObjectExistError):
            backend.insert(user_schema, record_id, sample_user)

    def test_insert_preserves_field_values(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify inserted record preserves all field values."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.name == sample_user.name
        assert retrieved.email == sample_user.email
        assert retrieved.age == sample_user.age
        assert retrieved.score == sample_user.score
        assert retrieved.status == sample_user.status
        assert retrieved.tags == sample_user.tags
        assert retrieved.metadata == sample_user.metadata

    def test_insert_with_optional_fields_null(
        self, backend: PostgresStoreBackend, workflow_schema: str
    ):
        """Verify optional fields can be NULL."""
        workflow = TestWorkflow(
            name="Minimal Workflow",
            organization_id="org-001",
            team_id="team-001",
            description=None,
            priority=None,
            updated_by=None,
        )
        record_id = str(uuid4())
        backend.insert(workflow_schema, record_id, workflow)

        retrieved = backend.get(workflow_schema, record_id, TestWorkflow)
        assert retrieved.description is None
        assert retrieved.priority is None
        assert retrieved.updated_by is None

    def test_insert_with_json_fields(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify complex JSON fields are preserved."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert isinstance(retrieved.metadata, dict)
        assert retrieved.metadata["department"] == "engineering"
        assert isinstance(retrieved.tags, list)
        assert "developer" in retrieved.tags

    def test_insert_with_enum_fields(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify Enum fields are stored and retrieved correctly."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.status == TestStatus.ACTIVE
        assert isinstance(retrieved.status, TestStatus)


# ============================================================
# UPDATE TESTS
# ============================================================

class TestUpdate:
    """Test record update operations."""

    def test_update_existing_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify an existing record can be updated."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        # Modify and update
        retrieved = backend.get(user_schema, record_id, TestUser)
        retrieved.name = "Alice Updated"
        retrieved.age = 31
        retrieved.status = TestStatus.INACTIVE
        backend.update(user_schema, record_id, retrieved)

        # Verify changes persisted
        updated = backend.get(user_schema, record_id, TestUser)
        assert updated.name == "Alice Updated"
        assert updated.age == 31
        assert updated.status == TestStatus.INACTIVE

    def test_update_nonexistent_record_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify updating a nonexistent record raises ObjectDoesNotExist."""
        backend.create_schema(user_schema, sample_user)
        with pytest.raises(ObjectDoesNotExist):
            backend.update(user_schema, "nonexistent_id", sample_user)

    def test_update_preserves_unchanged_fields(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify unchanged fields are preserved after update."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        original_email = retrieved.email
        original_tags = retrieved.tags.copy()

        retrieved.name = "New Name Only"
        backend.update(user_schema, record_id, retrieved)

        updated = backend.get(user_schema, record_id, TestUser)
        assert updated.name == "New Name Only"
        assert updated.email == original_email
        assert updated.tags == original_tags


# ============================================================
# UPSERT TESTS
# ============================================================

class TestUpsert:
    """Test upsert (insert-or-update) operations."""

    def test_upsert_inserts_new_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify upsert inserts when record doesn't exist."""
        record_id = str(uuid4())
        backend.upsert(user_schema, record_id, sample_user)
        assert backend.exists(user_schema, record_id)

    def test_upsert_updates_existing_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify upsert updates when record already exists."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        # Modify and upsert
        sample_user.name = "Upserted Name"
        sample_user.age = 99
        backend.upsert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.name == "Upserted Name"
        assert retrieved.age == 99

    def test_upsert_idempotent(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify upsert with same data is idempotent."""
        record_id = str(uuid4())
        backend.upsert(user_schema, record_id, sample_user)

        # Upsert again with same data
        backend.upsert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.name == sample_user.name


# ============================================================
# DELETE TESTS
# ============================================================

class TestDelete:
    """Test record deletion operations."""

    def test_delete_existing_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify an existing record can be deleted."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)
        assert backend.exists(user_schema, record_id)

        backend.delete(user_schema, record_id)
        assert not backend.exists(user_schema, record_id)

    def test_delete_nonexistent_record_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify deleting a nonexistent record raises ObjectDoesNotExist."""
        backend.create_schema(user_schema, sample_user)
        with pytest.raises(ObjectDoesNotExist):
            backend.delete(user_schema, "nonexistent_id")

    def test_delete_then_get_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify getting a deleted record raises ObjectDoesNotExist."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)
        backend.delete(user_schema, record_id)

        with pytest.raises(ObjectDoesNotExist):
            backend.get(user_schema, record_id, TestUser)


# ============================================================
# GET AND FILTER TESTS
# ============================================================

class TestGetAndFilter:
    """Test record retrieval and filtering operations."""

    def test_get_existing_record(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify getting an existing record returns the correct data."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved is not None
        assert retrieved.name == sample_user.name
        assert retrieved.id == record_id

    def test_get_nonexistent_record_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify getting a nonexistent record raises ObjectDoesNotExist."""
        backend.create_schema(user_schema, sample_user)
        with pytest.raises(ObjectDoesNotExist):
            backend.get(user_schema, "nonexistent_id", TestUser)

    def test_filter_exact_match(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify filtering by exact field value."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        results = backend.filter(user_schema, TestUser, name="Alice Test")
        assert len(results) == 1
        assert results[0].id == record_id

    def test_filter_no_match(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify filtering returns empty list when no records match."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        results = backend.filter(user_schema, TestUser, name="Nonexistent")
        assert len(results) == 0

    def test_filter_multiple_conditions(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify filtering with multiple conditions."""
        # Insert multiple users
        for i in range(5):
            user = TestUser(
                name=f"User {i}",
                email=f"user{i}@test.com",
                age=20 + i,
                is_active=(i % 2 == 0),
                status=TestStatus.ACTIVE if i < 3 else TestStatus.INACTIVE,
            )
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(
            user_schema, TestUser, status=TestStatus.ACTIVE, is_active=True
        )
        # Users with i=0,2 are active AND have is_active=True
        assert len(results) >= 1

    def test_filter_with_limit_and_offset(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify limit and offset parameters work correctly."""
        for i in range(10):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, limit=3, offset=5)
        assert len(results) == 3

    def test_filter_with_order_by_ascending(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify order_by ascending works."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=30 - i)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, order_by="age")
        ages = [r.age for r in results]
        assert ages == sorted(ages)

    def test_filter_with_order_by_descending(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify order_by descending works."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, order_by="-age")
        ages = [r.age for r in results]
        assert ages == sorted(ages, reverse=True)

    def test_filter_operator_gt(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __gt operator."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i * 5)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, age__gt=30)
        assert all(r.age > 30 for r in results)

    def test_filter_operator_gte(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __gte operator."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i * 5)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, age__gte=20)
        assert all(r.age >= 20 for r in results)

    def test_filter_operator_lt(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __lt operator."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i * 5)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, age__lt=40)
        assert all(r.age < 40 for r in results)

    def test_filter_operator_lte(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __lte operator."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i * 5)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, age__lte=40)
        assert all(r.age <= 40 for r in results)

    def test_filter_operator_ne(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify __ne operator."""
        backend.insert(user_schema, str(uuid4()), sample_user)

        results = backend.filter(user_schema, TestUser, name__ne="Alice Test")
        assert all(r.name != "Alice Test" for r in results)

    def test_filter_operator_in(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __in operator."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com", age=20 + i)
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(
            user_schema, TestUser, name__in=["User 0", "User 2", "User 4"]
        )
        assert len(results) == 3
        names = {r.name for r in results}
        assert names == {"User 0", "User 2", "User 4"}

    def test_filter_operator_contains(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __contains operator."""
        for name in ["Alice Johnson", "Bob Aliceberg", "Charlie Brown", "Alice Smith"]:
            user = TestUser(name=name, email=f"{name}@test.com".lower().replace(" ", "."))
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, name__contains="Alice")
        assert len(results) == 3
        assert all("Alice" in r.name for r in results)

    def test_filter_operator_startswith(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __startswith operator."""
        for name in ["Alice Johnson", "Bob Marley", "Alice Cooper", "Charlie"]:
            user = TestUser(name=name, email=f"{name}@test.com".lower().replace(" ", "."))
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, name__startswith="Alice")
        assert len(results) == 2

    def test_filter_operator_isnull(
        self, backend: PostgresStoreBackend, workflow_schema: str
    ):
        """Verify __isnull operator."""
        # Insert workflows with and without descriptions
        wf1 = TestWorkflow(
            name="With Description", organization_id="org-1", team_id="team-1",
            description="Has a description"
        )
        wf2 = TestWorkflow(
            name="Without Description", organization_id="org-1", team_id="team-1",
            description=None
        )
        backend.insert(workflow_schema, str(uuid4()), wf1)
        backend.insert(workflow_schema, str(uuid4()), wf2)

        null_results = backend.filter(workflow_schema, TestWorkflow, description__isnull=True)
        not_null_results = backend.filter(workflow_schema, TestWorkflow, description__isnull=False)

        assert len(null_results) == 1
        assert null_results[0].name == "Without Description"
        assert len(not_null_results) == 1
        assert not_null_results[0].name == "With Description"

    def test_filter_operator_icontains(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify __icontains operator (case-insensitive)."""
        for name in ["ALICE JOHNSON", "bob marley", "alice cooper"]:
            user = TestUser(name=name, email=f"{name}@test.com".lower().replace(" ", "."))
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, name__icontains="alice")
        assert len(results) == 2

    def test_filter_with_enum_field(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify filtering by Enum field works."""
        for status in TestStatus:
            user = TestUser(
                name=f"User {status.value}", email=f"{status.value}@test.com", status=status
            )
            backend.insert(user_schema, str(uuid4()), user)

        results = backend.filter(user_schema, TestUser, status=TestStatus.ACTIVE)
        assert len(results) == 1
        assert results[0].status == TestStatus.ACTIVE

    def test_filter_nonexistent_schema_raises(
        self, backend: PostgresStoreBackend
    ):
        """Verify filtering a nonexistent schema raises ObjectDoesNotExist."""
        with pytest.raises(ObjectDoesNotExist):
            backend.filter("nonexistent_schema", TestUser)


# ============================================================
# COUNT TESTS
# ============================================================

class TestCount:
    """Test count operations."""

    def test_count_all_records(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify count returns total number of records."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com")
            backend.insert(user_schema, str(uuid4()), user)

        assert backend.count(user_schema) == 5

    def test_count_with_filter(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify count with filter conditions."""
        for i in range(10):
            user = TestUser(
                name=f"User {i}", email=f"user{i}@test.com",
                is_active=(i % 2 == 0)
            )
            backend.insert(user_schema, str(uuid4()), user)

        active_count = backend.count(user_schema, is_active=True)
        assert active_count == 5

    def test_count_empty_schema(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify count returns 0 for empty schema."""
        backend.create_schema(user_schema, sample_user)
        assert backend.count(user_schema) == 0

    def test_count_nonexistent_schema_raises(
        self, backend: PostgresStoreBackend
    ):
        """Verify counting a nonexistent schema raises ObjectDoesNotExist."""
        with pytest.raises(ObjectDoesNotExist):
            backend.count("nonexistent_schema")


# ============================================================
# BULK OPERATION TESTS
# ============================================================

class TestBulkOperations:
    """Test bulk insert and delete operations."""

    def test_bulk_insert(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify bulk insert creates multiple records."""
        records = {}
        for i in range(10):
            record_id = str(uuid4())
            records[record_id] = TestUser(
                name=f"Bulk User {i}", email=f"bulk{i}@test.com", age=25 + i
            )

        count = backend.bulk_insert(user_schema, records)
        assert count == 10
        assert backend.count(user_schema) == 10

    def test_bulk_insert_empty(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify bulk insert with empty dict returns 0."""
        count = backend.bulk_insert(user_schema, {})
        assert count == 0

    def test_bulk_delete(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify bulk delete removes specified records."""
        ids = []
        for i in range(10):
            record_id = str(uuid4())
            ids.append(record_id)
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com")
            backend.insert(user_schema, record_id, user)

        # Delete first 5
        deleted = backend.bulk_delete(user_schema, ids[:5])
        assert deleted == 5
        assert backend.count(user_schema) == 5

        # Verify deleted records are gone
        for record_id in ids[:5]:
            assert not backend.exists(user_schema, record_id)

        # Verify remaining records exist
        for record_id in ids[5:]:
            assert backend.exists(user_schema, record_id)

    def test_bulk_delete_empty_list(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify bulk delete with empty list returns 0."""
        deleted = backend.bulk_delete(user_schema, [])
        assert deleted == 0

    def test_clear_schema(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify clear_schema removes all records but keeps the table."""
        for i in range(5):
            user = TestUser(name=f"User {i}", email=f"user{i}@test.com")
            backend.insert(user_schema, str(uuid4()), user)

        assert backend.count(user_schema) == 5
        assert backend.schema_exists(user_schema)

        deleted = backend.clear_schema(user_schema)
        assert deleted == 5
        assert backend.count(user_schema) == 0
        assert backend.schema_exists(user_schema)  # Table still exists


# ============================================================
# RELOAD TESTS
# ============================================================

class TestReload:
    """Test record reload operations."""

    def test_reload_refreshes_from_backend(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify reload refreshes the record with current backend state."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        # Get two references to the same record
        instance1 = backend.get(user_schema, record_id, TestUser)
        instance2 = backend.get(user_schema, record_id, TestUser)

        # Update via instance1
        instance1.name = "Updated via Instance 1"
        backend.update(user_schema, record_id, instance1)

        # Reload instance2
        backend.reload(user_schema, instance2)

        assert instance2.name == "Updated via Instance 1"

    def test_reload_nonexistent_record_raises(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify reloading a deleted record raises ObjectDoesNotExist."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        instance = backend.get(user_schema, record_id, TestUser)
        backend.delete(user_schema, record_id)

        with pytest.raises(ObjectDoesNotExist):
            backend.reload(user_schema, instance)

    def test_reload_without_id_raises(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify reloading a record without an id raises ValueError."""
        user = TestUser(name="No ID", email="noid@test.com")
        with pytest.raises(ValueError, match="must have an 'id' attribute"):
            backend.reload(user_schema, user)


# ============================================================
# SERIALIZATION TESTS
# ============================================================

class TestSerialization:
    """Test serialization round-trip fidelity."""

    def test_roundtrip_all_types(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify all field types survive a round-trip."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.name == sample_user.name
        assert isinstance(retrieved.name, str)
        assert retrieved.age == sample_user.age
        assert isinstance(retrieved.age, int)
        assert retrieved.score == sample_user.score
        assert isinstance(retrieved.score, float)
        assert retrieved.is_active == sample_user.is_active
        assert isinstance(retrieved.is_active, bool)
        assert retrieved.tags == sample_user.tags
        assert isinstance(retrieved.tags, list)
        assert retrieved.metadata == sample_user.metadata
        assert isinstance(retrieved.metadata, dict)
        assert retrieved.status == sample_user.status
        assert isinstance(retrieved.status, TestStatus)

    def test_roundtrip_nested_json(
        self, backend: PostgresStoreBackend, workflow_schema: str
    ):
        """Verify deeply nested JSON structures survive round-trip."""
        workflow = TestWorkflow(
            name="Nested Config Test",
            organization_id="org-001",
            team_id="team-001",
            config={
                "sources": [
                    {"name": "source1", "config": {"host": "a", "port": 1}},
                    {"name": "source2", "config": {"host": "b", "port": 2}},
                ],
                "transformations": {
                    "step1": {"type": "filter", "params": {"field": "status", "value": "active"}},
                },
            },
        )
        record_id = str(uuid4())
        backend.insert(workflow_schema, record_id, workflow)

        retrieved = backend.get(workflow_schema, record_id, TestWorkflow)
        assert retrieved.config == workflow.config
        assert len(retrieved.config["sources"]) == 2
        assert retrieved.config["sources"][0]["config"]["host"] == "a"

    def test_roundtrip_null_fields(
        self, backend: PostgresStoreBackend, workflow_schema: str
    ):
        """Verify NULL fields are preserved correctly."""
        workflow = TestWorkflow(
            name="Null Fields",
            organization_id="org-001",
            team_id="team-001",
            description=None,
            priority=None,
            updated_by=None,
        )
        record_id = str(uuid4())
        backend.insert(workflow_schema, record_id, workflow)

        retrieved = backend.get(workflow_schema, record_id, TestWorkflow)
        assert retrieved.description is None
        assert retrieved.priority is None
        assert retrieved.updated_by is None

    def test_serialize_with_orjson_if_available(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify serialization works with orjson if installed."""
        try:
            import orjson  # noqa: F401
            orjson_available = True
        except ImportError:
            orjson_available = False

        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)
        retrieved = backend.get(user_schema, record_id, TestUser)

        # The data should be correct regardless of serializer
        assert retrieved.name == sample_user.name
        assert retrieved.email == sample_user.email

    def test_load_record_static_method(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify the static load_record method works correctly."""
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, sample_user)

        # Get the raw serialized state
        raw = backend.get(user_schema, record_id, TestUser)
        serialized = backend._serialize_record(raw)

        # Load it back using the static method
        loaded = PostgresStoreBackend.load_record(serialized, TestUser)
        assert loaded.name == sample_user.name
        assert loaded.email == sample_user.email


# ============================================================
# IMMUTABLE AUDIT ENTRY TESTS
# ============================================================

class TestAuditPattern:
    """Test patterns for immutable/append-only records."""

    def test_audit_entry_insert_and_retrieve(
        self, backend: PostgresStoreBackend, audit_schema: str, sample_audit_entry: TestAuditEntry
    ):
        """Verify audit entries can be inserted and retrieved."""
        record_id = str(uuid4())
        backend.insert(audit_schema, record_id, sample_audit_entry)

        retrieved = backend.get(audit_schema, record_id, TestAuditEntry)
        assert retrieved.event_type == "workflow_created"
        assert retrieved.actor_id == "user-001"
        assert retrieved.metadata["version"] == "1.0.0"

    def test_audit_entry_hash_chain(
        self, backend: PostgresStoreBackend, audit_schema: str
    ):
        """Verify hash chaining for audit entries works."""
        import hashlib

        previous_hash = None
        entries = []

        for i in range(5):
            entry = TestAuditEntry(
                event_type=f"event_{i}",
                actor_id="user-001",
                actor_role="workflow_author",
                target_type="workflow",
                target_id=f"wf-{i:03d}",
                action="create",
                metadata={"sequence": i},
                previous_hash=previous_hash,
            )
            # Compute hash
            content = json.dumps({
                "event_type": entry.event_type,
                "actor_id": entry.actor_id,
                "target_id": entry.target_id,
                "sequence": i,
                "previous_hash": previous_hash,
            }, sort_keys=True)
            entry.hash = hashlib.sha256(content.encode()).hexdigest()

            backend.insert(audit_schema, str(uuid4()), entry)
            entries.append(entry)
            previous_hash = entry.hash

        # Verify chain integrity
        all_entries = backend.filter(audit_schema, TestAuditEntry, order_by="created_at")
        assert len(all_entries) == 5
        for i in range(1, len(all_entries)):
            assert all_entries[i].previous_hash == all_entries[i - 1].hash


# ============================================================
# EDGE CASE TESTS
# ============================================================

class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_insert_with_special_characters(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify records with special characters in strings work correctly."""
        special_name = "User with 'quotes' and \"double quotes\" and emoji 🚀"
        user = TestUser(
            name=special_name,
            email="special@test.com",
            metadata={"note": "Line1\nLine2\tTabbed"},
        )
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.name == special_name
        assert "\n" in retrieved.metadata["note"]

    def test_insert_with_very_long_string(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify records with very long string fields."""
        long_name = "A" * 10000
        user = TestUser(name=long_name, email="long@test.com")
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert len(retrieved.name) == 10000
        assert retrieved.name == long_name

    def test_insert_with_empty_collections(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify records with empty lists and dicts."""
        user = TestUser(
            name="Empty Collections",
            email="empty@test.com",
            tags=[],
            metadata={},
        )
        record_id = str(uuid4())
        backend.insert(user_schema, record_id, user)

        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved.tags == []
        assert retrieved.metadata == {}

    def test_filter_with_empty_result(
        self, backend: PostgresStoreBackend, user_schema: str, sample_user: TestUser
    ):
        """Verify filtering returns empty list for no matches."""
        backend.insert(user_schema, str(uuid4()), sample_user)
        results = backend.filter(user_schema, TestUser, name="Definitely Nonexistent")
        assert isinstance(results, list)
        assert len(results) == 0

    def test_get_table_info_nonexistent_schema(
        self, backend: PostgresStoreBackend
    ):
        """Verify get_table_info for nonexistent schema returns correct info."""
        info = backend.get_table_info("nonexistent_schema_xyz")
        assert not info["exists"]
        assert info["schema"] == "nonexistent_schema_xyz"


# ============================================================
# CONCURRENCY TESTS
# ============================================================

class TestConcurrency:
    """Test basic concurrent operation safety."""

    def test_concurrent_inserts(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify multiple inserts from different threads don't conflict."""
        import concurrent.futures

        def insert_user(i: int) -> str:
            record_id = str(uuid4())
            user = TestUser(
                name=f"Concurrent User {i}",
                email=f"concurrent{i}@test.com",
                age=25 + i,
            )
            # Each thread needs its own backend instance
            be = PostgresStoreBackend(**backend.connector.config.__dict__)
            try:
                be.insert(user_schema, record_id, user)
                return record_id
            finally:
                be.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(insert_user, i) for i in range(20)]
            ids = [f.result() for f in futures]

        # All records should exist
        for record_id in ids:
            assert backend.exists(user_schema, record_id)

        assert backend.count(user_schema) == 20

    def test_concurrent_upserts(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify concurrent upserts to the same key are safe."""
        import concurrent.futures

        record_id = str(uuid4())

        # First, insert the record
        user = TestUser(name="Original", email="original@test.com", age=1)
        backend.insert(user_schema, record_id, user)

        def upsert_with_value(value: int):
            be = PostgresStoreBackend(**backend.connector.config.__dict__)
            try:
                user = TestUser(
                    name=f"Updated {value}",
                    email=f"updated{value}@test.com",
                    age=value,
                )
                be.upsert(user_schema, record_id, user)
            finally:
                be.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(upsert_with_value, range(10)))

        # Record should exist with some value (race condition on which wins)
        retrieved = backend.get(user_schema, record_id, TestUser)
        assert retrieved is not None
        assert retrieved.id == record_id


# ============================================================
# PERFORMANCE BASELINE TESTS
# ============================================================

class TestPerformance:
    """Baseline performance tests (not strict assertions)."""

    def test_bulk_insert_performance(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify bulk insert completes within a reasonable time."""
        records = {}
        for i in range(100):
            record_id = str(uuid4())
            records[record_id] = TestUser(
                name=f"Perf User {i}", email=f"perf{i}@test.com", age=25 + i
            )

        start = time.time()
        count = backend.bulk_insert(user_schema, records)
        elapsed = time.time() - start

        assert count == 100
        # 100 inserts should complete in under 5 seconds
        assert elapsed < 5.0, f"Bulk insert took {elapsed:.2f}s"

    def test_filter_performance(
        self, backend: PostgresStoreBackend, user_schema: str
    ):
        """Verify filtered queries complete quickly."""
        # Insert test data
        records = {}
        for i in range(200):
            record_id = str(uuid4())
            records[record_id] = TestUser(
                name=f"Filter User {i}",
                email=f"filter{i}@test.com",
                age=20 + (i % 50),
                is_active=(i % 2 == 0),
                status=TestStatus.ACTIVE if i < 100 else TestStatus.INACTIVE,
            )
        backend.bulk_insert(user_schema, records)

        # Time a filtered query
        start = time.time()
        results = backend.filter(
            user_schema, TestUser,
            status=TestStatus.ACTIVE,
            is_active=True,
            age__gt=30,
        )
        elapsed = time.time() - start

        # Query should complete in under 1 second
        assert elapsed < 1.0, f"Filter query took {elapsed:.2f}s"