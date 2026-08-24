import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Optional,
    Type,
    TYPE_CHECKING,
    Union,
    ForwardRef,
    cast,
    Callable,
    Coroutine,
    List,
    Tuple,
)
from formax import Attrib, MiniAnnotated, MISSING
from formax.typing import evaluate_forward_ref

from volnux.import_utils import import_string
from volnux.utils import get_obj_klass_import_str
from volnux.exceptions import ObjectProtectedError
from .utils import formax_null_validator

if TYPE_CHECKING:
    from volnux.mixins import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


_MISSING = object()

SagaAction = Callable[[], Coroutine[Any, Any, None]]
SagaCompensation = Callable[[], Coroutine[Any, Any, None]]


def _build_cascade_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    """Build action/compensation pair for CASCADE delete."""
    snapshots: Dict[str, Dict[str, Any]] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            snapshots[obj.id] = obj.__getstate__()  # type: ignore

        if hasattr(referencing_model, "bulk_delete"):
            await referencing_model.bulk_delete(record_ids)
        else:
            await asyncio.gather(*(obj.delete() async for obj in objects))

        logger.info(
            "Saga CASCADE: deleted %d %s (%s)",
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        for record_id, state in snapshots.items():
            try:
                obj = referencing_model.__new__(referencing_model)
                obj.__setstate__(state)
                await obj.save(force_insert=True)
            except Exception as e:
                logger.error(
                    "Saga CASCADE compensate failed %s(%s): %s",
                    referencing_model.__name__,
                    record_id,
                    e,
                )
        logger.info(
            "Saga CASCADE compensated: restored %d %s",
            len(snapshots),
            referencing_model.__name__,
        )

    return action, compensation


def _build_set_null_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    field_name: str,
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    """Build action/compensation pair for SET_NULL."""
    original_values: Dict[str, Any] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            original_values[obj.id] = getattr(obj, field_name)
            setattr(obj, field_name, None)

        await asyncio.gather(*(obj.save() async for obj in objects))

        logger.info(
            "Saga SET_NULL: cleared %s on %d %s (%s)",
            field_name,
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        objects = await referencing_model.filter(id__in=list(original_values.keys()))

        async for obj in objects:
            original = original_values.get(obj.id)
            if original is not None:
                setattr(obj, field_name, original)

        if objects:
            await asyncio.gather(*(obj.save() async for obj in objects))

        logger.info(
            "Saga SET_NULL compensated: restored %s on %d %s",
            field_name,
            len(original_values),
            referencing_model.__name__,
        )

    return action, compensation


def _build_protect_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    field_name: str,
    obj_id: str,
    obj_class_name: str,
) -> Tuple[SagaAction, SagaCompensation]:
    """Build action/compensation pair for PROTECT check."""

    async def action() -> None:
        filter_key = f"{field_name}__object_id"
        referring = await referencing_model.filter(**{filter_key: obj_id})
        if referring:
            raise ObjectProtectedError(
                f"Cannot delete {obj_class_name}({obj_id}): "
                f"referenced by {len(referring)} {referencing_model.__name__} "
                f"objects via '{field_name}'."
            )

    async def compensation() -> None:
        pass  # Check-only step; nothing to undo

    return action, compensation


def _build_set_default_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    field_name: str,
    default_value: Any,
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    """Build action/compensation pair for SET_DEFAULT."""
    original_values: Dict[str, Any] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            original_values[obj.id] = getattr(obj, field_name)
            setattr(obj, field_name, default_value)

        await asyncio.gather(*(obj.save() async for obj in objects))

        logger.info(
            "Saga SET_DEFAULT: set %s on %d %s (%s)",
            field_name,
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        objects = await referencing_model.filter(id__in=list(original_values.keys()))
        async for obj in objects:
            original = original_values.get(obj.id)
            setattr(obj, field_name, original)

        if objects:
            await asyncio.gather(*(obj.save() async for obj in objects))

        logger.info(
            "Saga SET_DEFAULT compensated: restored %s on %d %s",
            field_name,
            len(original_values),
            referencing_model.__name__,
        )

    return action, compensation


class OnDelete(str, Enum):
    """Behavior when a referenced object is deleted."""

    CASCADE = "cascade"  # Delete all referring objects
    SET_NULL = "set_null"  # Set the foreign key to None on referring objects
    PROTECT = "protect"  # Raise an error if any referring objects exist
    SET_DEFAULT = "set_default"  # Set to a default value
    DO_NOTHING = "do_nothing"  # Leave the reference dangling (not recommended)

    async def operation_handler(
        self,
        obj_id: str,
        obj_class_name: str,
        referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str],
        field_name: str,
        attrib: Attrib,
    ):
        """
        Handles operations related to object deletion, cascade, or updates based on the
        defined `on_delete` behavior for referencing model objects.

        :param obj_id: The identifier of the object for which the operation is being
            handled.
        :type obj_id: str
        :param obj_class_name: The class name of the referenced object being handled.
        :type obj_class_name: str
        :param referencing_model: The model or model class name that references the
            object.
        :type referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str]
        :param field_name: The name of the field in the referencing model that refers
            to the object.
        :type field_name: str
        :param attrib: Field metadata containing field behavior or settings.
        :type attrib: Attrib
        :return: None if no action is taken, or if operation completes without errors.
        :rtype: None
        :raises ValueError: If the `referencing_model` cannot be imported.
        :raises ObjectProtectedError: If the `on_delete` behavior is set to PROTECT and
            the referenced object cannot be deleted due to active references.
        """
        if isinstance(referencing_model, str):
            try:
                referencing_model: Type["KeyValueStoreIntegrationMixin"] = import_string(referencing_model)  # type: ignore
            except ImportError as e:
                raise ValueError(
                    f"Failed to import model class '{referencing_model}'"
                ) from e

        # Find all objects referencing this instance
        filter_key = f"{field_name}__object_id"
        referring_objects = await referencing_model.filter(**{filter_key: obj_id})

        if referring_objects.exists():
            return

        if self == OnDelete.PROTECT:
            raise ObjectProtectedError(
                f"Cannot delete {self.__class__.__name__}({obj_id}): "
                f"referenced by {len(referring_objects)} "
                f"{referencing_model.__name__} objects via '{field_name}'. "
                f"Use on_delete=CASCADE or SET_NULL on the ForeignKeyField."
            )

        elif self == OnDelete.CASCADE:
            try:
                await referring_objects.bulk_delete()
            except Exception:
                async for obj in referring_objects:
                    await obj.delete()
            logger.info(
                "Cascaded delete: %d %s objects deleted " "due to %s(%s) deletion",
                len(referring_objects),
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.SET_NULL:
            try:
                await referring_objects.bulk_update(**{field_name: None})
            except Exception:
                async for obj in referring_objects:
                    setattr(obj, field_name, None)
                    await obj.save()
            logger.info(
                "Set NULL on %d %s objects due to %s(%s) deletion",
                len(referring_objects),
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.SET_DEFAULT:
            # Get the default value from the field definition
            # This requires storing the default on the Attrib

            def get_default(attrib: Attrib) -> Any:
                if attrib.has_default():
                    if attrib.default is MISSING:
                        return attrib.default_factory()
                    return attrib.default

                return MISSING

            async for obj in referring_objects:
                default = attrib.get_default()  # get_default(attrib)
                if default is MISSING:
                    logger.warning(
                        "Cannot set default value for %s: %s",
                        field_name,
                        default,
                    )
                    continue
                setattr(obj, field_name, default)
                await obj.save()

        elif self == OnDelete.DO_NOTHING:
            logger.warning(
                "Dangling reference: %d %s objects still reference "
                "deleted %s(%s) via '%s'",
                len(referring_objects),
                referencing_model.__name__,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
                field_name,
            )


class FKConstraint(str, Enum):
    """Where the foreign key constraint is enforced."""

    AUTO = "auto"  # Native if the same database, software otherwise
    NATIVE = "native"  # Database-level constraint only
    SOFTWARE = "software"  # Application-level constraint only
    BOTH = "both"


@dataclass(frozen=True)
class FKConfig:
    """Configuration for a ForeignKey field."""

    reverse_name: Optional[str] = None
    on_delete: OnDelete = OnDelete.PROTECT
    nullable: bool = True
    constraint: FKConstraint = FKConstraint.AUTO


class ForeignKey:
    """Serializable foreign key reference to any KeyValueStoreIntegrationMixin model.

    Stored format:
        {
            "object_id": "uuid-string",
            "model_path": "volnux.models.governance.Workflow",
            "backend_alias": "default"
        }

    On access, the referenced object is lazy-loaded from the backend.
    On serialization, the object is reduced to its reference tuple.
    """

    __slots__ = (
        "object_id",
        "model_path",
        "backend_alias",
        "_resolved",
        "_instance",
        "_is_native_fk",
    )

    def __init__(
        self,
        object_id: str,
        model_path: str,
        backend_alias: str = "default",
        is_native_fk: bool = False,
    ):
        self.object_id = object_id
        self.model_path = model_path
        self.backend_alias = backend_alias
        self._resolved = False
        self._instance: Optional[KeyValueStoreIntegrationMixin] = None
        self._is_native_fk = is_native_fk

    @property
    def id(self) -> str:
        return self.object_id

    @classmethod
    def serialize(cls, value: Any) -> Optional[Dict[str, str]]:
        """Convert a model instance or ForeignKey to its serializable dict form.

        Args:
            value: A KeyValueStoreIntegrationMixin instance, a ForeignKey
                   instance, or None.

        Returns:
            Dict with object_id, model_path, backend_alias, or None.
        """
        if value is None:
            return None

        # Already a ForeignKey reference
        if isinstance(value, ForeignKey):
            return {
                "type": "ForeignKey",
                "object_id": value.object_id,
                "model_path": value.model_path,
                "backend_alias": value.backend_alias,
            }

        # A model instance — extract reference
        if hasattr(value, "id") and hasattr(value, "__class__"):
            return {
                "type": "ForeignKey",
                "object_id": str(value.id),
                "model_path": get_obj_klass_import_str(value),
                "backend_alias": getattr(value, "_backend_alias", "default"),
            }

        raise TypeError(
            f"Cannot create ForeignKey reference from {type(value).__name__}. "
            f"Expected a KeyValueStoreIntegrationMixin instance or ForeignKey."
        )

    @classmethod
    def deserialize(cls, value: Any) -> Any:
        """Convert a serialized dict to a ForeignKey reference.

        The actual model instance is NOT loaded at deserialization time.
        It is lazy-loaded on first attribute access via __getattr__.

        Args:
            value: A dict with object_id, model_path, backend_alias,
                   or a ForeignKey instance, or None.

        Returns:
            A ForeignKey instance (lazy), the original value, or None.
        """
        if value is None:
            return None

        # Already deserialized
        if isinstance(value, ForeignKey):
            return value

        # Already a resolved model instance (e.g., from previous access)
        if not isinstance(value, dict):
            return value

        # Must have object_id
        if "object_id" not in value:
            logger.warning("ForeignKey dict missing 'object_id': %s", value)
            return None

        return ForeignKey(
            object_id=value["object_id"],
            model_path=value.get("model_path", ""),
            backend_alias=value.get("backend_alias", "default"),
        )

    def resolve(self) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Resolve and return the referenced model instance.

        The instance is cached after the first resolution.

        Returns:
            The resolved model instance, or None if not found.

        Raises:
            ImportError: If the model class cannot be imported.
            AttributeError: If the model class doesn't have a 'get' method.
        """
        if self._resolved:
            return self._instance

        if not self.model_path:
            logger.warning("Cannot resolve ForeignKey with empty model_path")
            self._resolved = True
            return None

        try:
            model_class: Type["KeyValueStoreIntegrationMixin"] = import_string(self.model_path)  # type: ignore[assignment]
        except (ImportError, AttributeError) as e:
            logger.error("Failed to import model class '%s': %s", self.model_path, e)
            self._resolved = True
            return None

        try:
            self._instance = model_class.get(self.object_id)
        except Exception as e:
            logger.warning(
                "Failed to resolve ForeignKey %s -> %s: %s",
                self.model_path,
                self.object_id,
                e,
            )
            self._instance = None

        self._resolved = True
        return self._instance

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the resolved instance.

        This enables transparent access: workflow.created_by.name
        """
        # Prevent infinite recursion during __setstate__ and internal attrs
        if name.startswith("_"):
            raise AttributeError(name)

        instance = self.resolve()
        if instance is None:
            raise ValueError(
                f"Cannot access '{name}' on unresolved ForeignKey "
                f"({self.model_path}#{self.object_id})"
            )
        return getattr(instance, name)

    def __repr__(self) -> str:
        if self._resolved and self._instance is not None:
            return repr(self._instance)
        return f"<ForeignKey: {self.model_path}#{self.object_id}>"

    def __str__(self) -> str:
        if self._resolved and self._instance is not None:
            return str(self._instance)
        return f"{self.model_path}#{self.object_id}"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ForeignKey):
            return (
                self.object_id == other.object_id
                and self.model_path == other.model_path
            )
        if hasattr(other, "id"):
            return self.object_id == str(other.id)
        return False

    def __hash__(self) -> int:
        return hash((self.object_id, self.model_path))

    def __bool__(self) -> bool:
        return self.object_id is not None and len(self.object_id) > 0


class ForeignKeyField:
    """Create a MiniAnnotated field definition for a foreign key reference.

    Args:
        target_model: Optional model class for type hinting and validation.
        nullable: Whether the foreign key can be None.
        reverse_name: Optional name for the reverse relation.
        on_delete: Action to perform when the referenced object is deleted.
        constraint: Constraint type for the foreign key relationship.
                    - AUTO: Native if the same database, software otherwise
                    - NATIVE: Database-level constraint only
                    - SOFTWARE: Software-level constraint only
                    - BOTH: Both native and software constraints

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Workflow(GovernanceModel):
        ...     created_by: ForeignKeyField[User]
        ...     approved_by: ForeignKeyField[User, FKConfig(nullable=True, reverse_name='approved_workflows')]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError(f"Cannot subclass ForeignKeyField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ForeignKeyField cannot be instantiated")

    def __class_getitem__(cls, params) -> MiniAnnotated:
        if not isinstance(params, tuple):
            params = (params, FKConfig())

        if len(params) == 1:
            target_model = params[0]
            config = FKConfig()
        elif len(params) == 2:
            target_model, config = params
            if not isinstance(config, FKConfig):
                raise TypeError(
                    f"Second argument must be FKConfig, got {type(config).__name__}"
                )
        else:
            raise TypeError(
                f"ForeignKeyField[...] expects 1 or 2 arguments, got {len(params)}"
            )

        validators = []

        if config.on_delete == OnDelete.SET_NULL and not config.nullable:
            raise ValueError("on_delete=SET_NULL requires nullable=True.")

        if isinstance(target_model, str):
            try:
                module_path, class_name = target_model.rsplit(".", 1)
            except ValueError as err:
                # raise ImportError(
                #     f"{target_model} doesn't look like a module path"
                # ) from err
                module_path, class_name = None, target_model

            # Create a forward reference
            target_model = ForwardRef(class_name, module=module_path, is_class=True)

        if target_model is None:
            pre_fmt = lambda instance, value: ForeignKey.serialize(value)
            post_fmt = lambda instance, value: ForeignKey.deserialize(value)
        else:
            # Typed foreign key — validates model class on serialization
            def pre_fmt(instance, value):
                nonlocal target_model
                model_class = target_model

                if isinstance(model_class, ForwardRef):
                    model_class = evaluate_forward_ref(model_class, None, None)

                model_class = cast(Type["KeyValueStoreIntegrationMixin"], model_class)

                if value is not None and not isinstance(value, model_class):
                    raise TypeError(
                        f"Expected {target_model.__name__} instance, "
                        f"got {type(value).__name__}"
                    )
                return ForeignKey.serialize(value)

            post_fmt = lambda instance, object_id: ForeignKey.deserialize(object_id)

        if not config.nullable:
            validators.append(formax_null_validator)

        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                post_formatter=post_fmt,
                validators=validators,
                metadata={
                    "type": "foreignkey",
                    "target_model": target_model,
                    "reverse_name": config.reverse_name,
                    "on_delete": config.on_delete or OnDelete.PROTECT,
                    "nullable": config.nullable,
                    "constraint": config.constraint,
                },
            ),
        ]
