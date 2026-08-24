"""Tests for volnux.execution.checkpoint_constants — FRAMEWORK_EXCLUDED_ATTRS.

Validates:
  - The set is a frozenset (immutable).
  - It contains all known framework-internal attribute names.
  - It does not inadvertently block common user attribute patterns.
"""

from __future__ import annotations

from volnux.execution.checkpoint_constants import FRAMEWORK_EXCLUDED_ATTRS


class TestFrameworkExcludedAttrs:
    # ------------------------------------------------------------------ type
    def test_is_frozenset(self) -> None:
        assert isinstance(FRAMEWORK_EXCLUDED_ATTRS, frozenset)

    # ------------------------------------------------- snapshot field names
    def test_contains_snapshot_fields(self) -> None:
        snapshot_fields = {
            "task_id",
            "class_path",
            "phase",
            "init_args",
            "call_args",
            "external_resources",
            "timestamp",
            "exec_status",
            "exec_result",
            "retry_count",
            "max_retry_attempts",
            "attribs",
        }
        assert snapshot_fields.issubset(FRAMEWORK_EXCLUDED_ATTRS), (
            "Missing snapshot field names in FRAMEWORK_EXCLUDED_ATTRS"
        )

    # --------------------------------------------------- init_arg key names
    def test_contains_init_arg_keys(self) -> None:
        init_arg_keys = {
            "execution_context",
            "previous_result",
            "stop_condition",
            "run_bypass_event_checks",
            "options",
            "sequence_number",
            "kwargs",
        }
        assert init_arg_keys.issubset(FRAMEWORK_EXCLUDED_ATTRS), (
            "Missing init_arg key names in FRAMEWORK_EXCLUDED_ATTRS"
        )

    # ------------------------------------------------- framework internals
    def test_contains_framework_internals(self) -> None:
        internal_attrs = {
            "_pipeline",
            "_trigger",
            "_step_runner",
            "_logger",
            "_options",
            "_event_graph",
            "_parent_context",
        }
        assert internal_attrs.issubset(FRAMEWORK_EXCLUDED_ATTRS), (
            "Missing framework internal attrs in FRAMEWORK_EXCLUDED_ATTRS"
        )

    # -------------------------------------- common user attrs are NOT excluded
    @pytest.mark.parametrize("attr", [
        "_page_number",
        "_offset",
        "_current_item",
        "_accumulator",
        "_total_count",
        "_cache_key",
        "_result_buffer",
    ])
    def test_user_attrs_not_excluded(self, attr: str) -> None:
        assert attr not in FRAMEWORK_EXCLUDED_ATTRS, (
            f"User attribute '{attr}' is incorrectly in FRAMEWORK_EXCLUDED_ATTRS"
        )

    # -------------------------------------------------- no empty strings etc
    def test_no_empty_strings(self) -> None:
        assert "" not in FRAMEWORK_EXCLUDED_ATTRS
