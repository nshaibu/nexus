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


# ---------------------------------------------------------------------------
# Saga Step Builders (Unchanged — Already Correct)
# ---------------------------------------------------------------------------


def _build_cascade_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    snapshots: Dict[str, Dict[str, Any]] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            snapshots[obj.id] = obj.__getstate__()
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
    async def action() -> None:
        filter_key = f"{field_name}__object_id"
        referring = await referencing_model.filter(**{filter_key: obj_id})
        count = await referring.count()
        if count > 0:
            raise ObjectProtectedError(
                f"Cannot delete {obj_class_name}({obj_id}): "
                f"referenced by {count} {referencing_model.__name__} objects via '{field_name}'."
            )

    async def compensation() -> None:
        pass

    return action, compensation


def _build_set_default_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    field_name: str,
    default_value: Any,
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
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


# ---------------------------------------------------------------------------
# OnDelete (Fixed Inverted Logic + Saga Integration)
# ---------------------------------------------------------------------------


class OnDelete(str, Enum):
    CASCADE = "cascade"
    SET_NULL = "set_null"
    PROTECT = "protect"
    SET_DEFAULT = "set_default"
    DO_NOTHING = "do_nothing"

    async def operation_handler(
        self,
        obj_id: str,
        obj_class_name: str,
        referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str],
        field_name: str,
        attrib: Attrib,
    ) -> None:
        if isinstance(referencing_model, str):
            try:
                referencing_model = import_string(referencing_model)  # type: ignore[assignment]
            except ImportError as e:
                raise ValueError(
                    f"Failed to import model class '{referencing_model}'"
                ) from e

        filter_key = f"{field_name}__object_id"
        referring_objects = await referencing_model.filter(**{filter_key: obj_id})
        count = await referring_objects.count()

        # FIXED: proceed only when references EXIST (was inverted)
        if count == 0:
            return

        if self == OnDelete.PROTECT:
            raise ObjectProtectedError(
                f"Cannot delete {obj_class_name}({obj_id}): "
                f"referenced by {count} {referencing_model.__name__} objects via '{field_name}'. "
                f"Use on_delete=CASCADE or SET_NULL on the ForeignKeyField."
            )

        elif self == OnDelete.CASCADE:
            try:
                await referring_objects.bulk_delete()
            except Exception:
                async for obj in referring_objects:
                    await obj.delete()
            logger.info(
                "Cascaded delete: %d %s objects due to %s(%s)",
                count,
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
                "Set NULL on %d %s objects due to %s(%s)",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.SET_DEFAULT:
            default = (
                attrib.get_default() if hasattr(attrib, "get_default") else _MISSING
            )
            if default is _MISSING:
                logger.warning(
                    "Cannot SET_DEFAULT for %s.%s: no default defined",
                    referencing_model.__name__,
                    field_name,
                )
                return
            async for obj in referring_objects:
                setattr(obj, field_name, default)
                await obj.save()
            logger.info(
                "Set default on %d %s objects due to %s(%s)",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.DO_NOTHING:
            logger.warning(
                "Dangling reference: %d %s objects still reference deleted %s(%s) via '%s'",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
                field_name,
            )


# ---------------------------------------------------------------------------
# FKConstraint & FKConfig (Unchanged)
# ---------------------------------------------------------------------------


class FKConstraint(str, Enum):
    AUTO = "auto"
    NATIVE = "native"
    SOFTWARE = "software"
    BOTH = "both"


@dataclass(frozen=True)
class FKConfig:
    reverse_name: Optional[str] = None
    on_delete: OnDelete = OnDelete.PROTECT
    nullable: bool = True
    constraint: FKConstraint = FKConstraint.AUTO


# ---------------------------------------------------------------------------
# ForeignKey (Async Resolve + Self-Describing Serialization)
# ---------------------------------------------------------------------------


class ForeignKey:
    """Serializable foreign key reference.

    Stored format (self-describing dict):
        {"type": "foreignkey", "data": "uuid-string", "native": false}

    The descriptor owns model_path and backend_alias. Only object_id
    and enforcement mode travel through the serialization boundary.
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
        self._instance: Optional["KeyValueStoreIntegrationMixin"] = None
        self._is_native_fk = is_native_fk

    @property
    def id(self) -> str:
        return self.object_id

    @classmethod
    def serialize(
        cls, value: Any, is_native_fk: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Convert to self-describing dict for backend storage.

        Produces: {"type": "foreignkey", "data": object_id, "native": bool}
        Backend annotates authoritative native flag at write time.
        """
        if value is None:
            return None

        if isinstance(value, ForeignKey):
            return {
                "type": "foreignkey",
                "data": value.object_id,
                "native": value._is_native_fk,
            }

        if hasattr(value, "id") and hasattr(value, "__class__"):
            return {
                "type": "foreignkey",
                "data": str(value.id),
                "native": is_native_fk,
            }

        raise TypeError(
            f"Cannot create ForeignKey reference from {type(value).__name__}. "
            f"Expected a KeyValueStoreIntegrationMixin instance or ForeignKey."
        )

    @classmethod
    def deserialize(
        cls, value: Any, model_path: str = "", backend_alias: str = "default"
    ) -> Any:
        """Convert stored value to ForeignKey reference.

        Accepts self-describing dict, plain string ID, or existing ForeignKey.
        Model resolution is deferred to first attribute access.
        """
        if value is None:
            return None

        if isinstance(value, ForeignKey):
            return value

        # Self-describing dict from backend
        if isinstance(value, dict) and value.get("type") == "foreignkey":
            return ForeignKey(
                object_id=value.get("data", ""),
                model_path=model_path,
                backend_alias=backend_alias,
                is_native_fk=value.get("native", False),
            )

        # Backward compat: old-format dict with object_id/model_path
        if isinstance(value, dict) and "object_id" in value:
            return ForeignKey(
                object_id=value["object_id"],
                model_path=value.get("model_path", model_path),
                backend_alias=value.get("backend_alias", backend_alias),
                is_native_fk=value.get("native", False),
            )

        # Plain string ID (from column-only storage)
        if isinstance(value, str):
            return ForeignKey(
                object_id=value,
                model_path=model_path,
                backend_alias=backend_alias,
                is_native_fk=False,  # Assume software-enforced for plain IDs
            )

        logger.warning("Unexpected ForeignKey value type: %s", type(value).__name__)
        return None

    async def resolve_async(self) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Async resolution of the referenced model instance."""
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
            self._instance = await model_class.get(self.object_id)
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
        if name.startswith("_"):
            raise AttributeError(name)

        # NOTE: For sync contexts, callers should await resolve_async() first.
        # This proxy raises if not yet resolved to prevent silent coroutine storage.
        if not self._resolved:
            raise AttributeError(
                f"ForeignKey not yet resolved. Call 'await fk.resolve_async()' before "
                f"accessing attributes, or use the async iteration protocol."
            )

        if self._instance is None:
            raise ValueError(
                f"Cannot access '{name}' on unresolved ForeignKey ({self.model_path}#{self.object_id})"
            )
        return getattr(self._instance, name)

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
        return bool(self.object_id)


# ---------------------------------------------------------------------------
# ForeignKeyField (Self-Describing pre_formatter + Descriptor-Owned Metadata)
# ---------------------------------------------------------------------------


class ForeignKeyField:
    """Create a MiniAnnotated field definition for a foreign key reference.

    The pre_formatter produces self-describing dicts:
        {"type": "foreignkey", "data": object_id, "native": requested_native}

    The backend annotates the authoritative native flag at write time.
    The post_formatter reconstructs ForeignKey using descriptor-owned metadata.
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError("Cannot subclass ForeignKeyField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ForeignKeyField cannot be instantiated")

    @staticmethod
    def _resolve_target_model(
        target_model: Any,
    ) -> Type["KeyValueStoreIntegrationMixin"]:
        if isinstance(target_model, ForwardRef):
            return cast(
                Type["KeyValueStoreIntegrationMixin"],
                evaluate_forward_ref(target_model, None, None),
            )
        return cast(Type["KeyValueStoreIntegrationMixin"], target_model)

    @classmethod
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

        if config.on_delete == OnDelete.SET_NULL and not config.nullable:
            raise ValueError("on_delete=SET_NULL requires nullable=True.")

        validators = []
        if not config.nullable:
            validators.append(formax_null_validator)

        # Capture descriptor-owned metadata for post_formatter
        if isinstance(target_model, str):
            try:
                module_path, class_name = target_model.rsplit(".", 1)
            except ValueError as err:
                raise ImportError(
                    f"{target_model} doesn't look like a module path"
                ) from err
            target_model = ForwardRef(class_name, module=module_path, is_class=True)

        # Determine model_path string for post_formatter
        if isinstance(target_model, ForwardRef):
            _model_path = (
                f"{target_model.__forward_module__}.{target_model.__forward_arg__}"
            )
        elif hasattr(target_model, "__module__"):
            _model_path = get_obj_klass_import_str(target_model)
        else:
            _model_path = ""

        # Pre-formatter: produces self-describing dict
        def pre_fmt(instance: Any, value: Any) -> Optional[Dict[str, Any]]:
            if value is None:
                return None

            # Validate type if target_model is resolvable
            if target_model is not None:
                try:
                    model_class = cls._resolve_target_model(target_model)
                    if not isinstance(value, (model_class, ForeignKey)):
                        raise TypeError(
                            f"Expected {model_class.__name__} or ForeignKey, got {type(value).__name__}"
                        )
                except Exception:
                    pass  # ForwardRef not yet resolvable; skip validation

            # Requested native based on FKConfig constraint setting
            requested_native = config.constraint in (
                FKConstraint.NATIVE,
                FKConstraint.BOTH,
                FKConstraint.AUTO,
            )
            return ForeignKey.serialize(value, is_native_fk=requested_native)

        # Post-formatter: reconstructs ForeignKey from stored value using descriptor metadata
        def post_fmt(instance: Any, value: Any) -> Any:
            return ForeignKey.deserialize(
                value, model_path=_model_path, backend_alias="default"
            )

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
                    "on_delete": config.on_delete,
                    "nullable": config.nullable,
                    "constraint": config.constraint,
                },
            ),
        ]
