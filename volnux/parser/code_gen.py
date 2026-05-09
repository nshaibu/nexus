import logging
import typing

from . import ast
from .conditional import StandardDescriptor
from .exceptions import PointyParseError
from .operator import PipeType
from .options import Options
from .protocols import TaskGroupingProtocol, TaskProtocol
from .visitor import ASTVisitorInterface

logger = logging.getLogger(__name__)


class ExecutableASTGenerator(ASTVisitorInterface):

    def __init__(
        self,
        task_template: typing.Type[TaskProtocol],
        grouping_template: typing.Type[TaskGroupingProtocol],
    ):
        self.task_template = task_template
        self.grouping_template = grouping_template
        self._generated_task_chain: typing.Optional[TaskProtocol] = None
        self._current_task: typing.Optional[TaskProtocol] = None

    def apply_directive(self, name: str, value: typing.Union[str, int]):
        """Apply configuration directive"""
        if name == "recursive-depth":
            from volnux.utils import _extend_recursion_depth

            result = _extend_recursion_depth(value)
            if isinstance(result, Exception):
                logger.warning(f"Failed to set recursive-depth: {result}")

    def visit_program(self, node: ast.ProgramNode):
        for directive_name, directive_value in node.directives.items():
            self.apply_directive(directive_name, directive_value.accept(self))

        chain = node.chain
        if chain is None:
            return
        self._generated_task_chain = None
        self._current_task = None
        chain.accept(self)

    def visit_binop(self, node: ast.BinOpNode) -> typing.Union[TaskProtocol, TaskGroupingProtocol]:
        left_instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
            node.left.accept(self)
        )
        right_instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
            node.right.accept(self)
        )

        if isinstance(
            left_instance, (TaskProtocol, TaskGroupingProtocol)
        ) and isinstance(right_instance, (TaskProtocol, TaskGroupingProtocol)):
            pipe_type = PipeType.get_pipe_type_enum(node.op)
            if pipe_type is None:
                logger.debug("No pipe type for %s", node.op)
                raise PointyParseError(
                    f"AST is malformed {ast}. No pipe type for {node.op} found."
                )

            if left_instance.is_conditional:
                left_instance.sink_node = right_instance
                left_instance.sink_pipe = pipe_type
            else:
                left_instance.condition_node.on_success_event = right_instance
                left_instance.condition_node.on_success_pipe = pipe_type

            right_instance.parent_node = left_instance
            return right_instance
        elif isinstance(left_instance, int) or isinstance(right_instance, int):
            descriptor_value = None
            node_instance = None

            if isinstance(left_instance, int):
                descriptor_value = left_instance
            else:
                node_instance = left_instance

            if isinstance(right_instance, int):
                descriptor_value = right_instance
            else:
                node_instance = right_instance

            if node_instance is None:
                logger.debug("No node instance for %s", left_instance)
                raise PointyParseError(
                    f"AST is malformed {ast}. Descriptor operation must have a valid task node"
                )

            # handle retry syntax
            if node.op == PipeType.RETRY.token():
                if node_instance.options is None:
                    try:
                        node_instance.options = Options.from_dict({"retry_attempts": 0})
                    except Exception:
                        class _FallbackOptions:
                            def __init__(self):
                                self.retry_attempts = 0
                                self.extras = {}

                        node_instance.options = _FallbackOptions()
                # override the retry_attempts since * has high precedence
                node_instance.options.retry_attempts = descriptor_value
                return node_instance

            node_instance = node_instance.get_root()
            node_instance.descriptor = descriptor_value
            node_instance.descriptor_pipe = node.op
            return node_instance
        else:
            return left_instance or right_instance

    def visit_descriptor(self, node: ast.DescriptorNode):
        return int(node.value)

    def visit_task(self, node: ast.TaskNode):
        instance = self.task_template(event=node.task)
        self._current_task = instance
        if node.options:
            options_dict = dict(attr.accept(self) for attr in node.options)
            instance.options = Options.from_dict(options_dict)
        return instance

    def visit_literal(self, node: ast.LiteralNode) -> typing.Union[int, str, float]:
        return node.value

    def visit_pipeline_grouping(self, node: ast.PipelineGroupingNode) -> TaskGroupingProtocol:
        chains = []
        for expr in node.expressions:
            result = expr.accept(self)
            if result is not None:
                chains.append(result.get_root())

        instance = self.grouping_template(chains)
        self._current_task = instance
        if node.options:
            options_dict = dict(attr.accept(self) for attr in node.options)
            instance.options = Options.from_dict(options_dict)
        return instance

    def visit_directive(self, node: ast.DirectiveNode):
        """Visit individual directive node — resolves the value only.
        The name is handled upstream by visit_program via ProgramNode.directives."""
        return node.value.accept(self)

    def visit_conditional(self, node: ast.ConditionalNode):
        parent = node.task.accept(self)

        for branch in node.branches:
            instance: typing.Union[TaskProtocol, TaskGroupingProtocol] = (
                branch.accept(self)
            )
            if instance:
                self._current_task = instance

                instance = instance.get_root()
                instance.parent_node = parent

                if instance.descriptor == StandardDescriptor.FAILURE:
                    parent.condition_node.on_failure_event = instance
                    parent.condition_node.on_failure_pipe = PipeType.get_pipe_type_enum(
                        instance.descriptor_pipe
                    )
                elif instance.descriptor == StandardDescriptor.SUCCESS:
                    parent.condition_node.on_success_event = instance
                    parent.condition_node.on_success_pipe = PipeType.get_pipe_type_enum(
                        instance.descriptor_pipe
                    )
                else:
                    is_added = parent.condition_node.add_descriptor(
                        instance.descriptor,
                        PipeType.get_pipe_type_enum(instance.descriptor_pipe),
                        instance,
                    )
                    if not is_added:
                        logger.warning(
                            f"Failed to add descriptor {instance.descriptor} for event {node}"
                        )
            else:
                logger.warning(
                    f"Failed to add descriptor for conditional event {branch}"
                )

        return parent

    def visit_variable_access(self, node: ast.VariableAccessNode):
        return node.resolve()

    def visit_unaryop(self, node: ast.UnaryOpNode):
        # Evaluate the right-hand expression first
        right_value = node.right.accept(self)

        op = node.op

        # Logical NOT: return boolean inversion based on truthiness
        if op == "!":
            return not self._is_truthy(right_value)

        # Unary minus: numeric negation
        if op == "-":
            if isinstance(right_value, (int, float, bool)):
                return -right_value
            raise PointyParseError(f"Unary '-' applied to non-numeric value: {right_value!r}")

        # Bitwise NOT: only valid for integers (booleans are ints in Python but warn elsewhere)
        if op == "~":
            if isinstance(right_value, int) and not isinstance(right_value, bool):
                return ~right_value
            # allow bool too (it's an int subclass) to maintain intuitive behaviour
            if isinstance(right_value, bool):
                return ~int(right_value)
            raise PointyParseError(f"Bitwise NOT '~' applied to non-integer value: {right_value!r}")

        # Unknown unary operator: raise
        raise PointyParseError(f"Unknown unary operator '{op}'")

    def visit_list(self, node: ast.ListNode) -> typing.List[typing.Any]:
        """Resolve a ListNode into a plain Python list by visiting each element.

        Each element may be a LiteralNode, ListNode, MapNode, VariableAccessNode,
        or any expression — we return whatever its visitor resolves to.
        """
        resolved = []
        for item in node.value:
            # Each item is an AST node; dispatch to its visitor
            resolved.append(item.accept(self))
        return resolved

    def visit_map(self, node: ast.MapNode) -> typing.Dict[str, typing.Any]:
        """Resolve a MapNode into a plain Python dict by visiting each value.

        Keys are strings per the grammar; values are resolved via visitor dispatch.
        """
        resolved: typing.Dict[str, typing.Any] = {}
        for key, value in node.value.items():
            resolved[key] = value.accept(self)
        return resolved

    def visit_meta_task(self, node: ast.MetaTaskNode):
        # Create a pipeline task whose event is the meta-event mode (e.g. MAP, FILTER)
        # The template task (the inner template event) is passed via options.extras
        instance = self.task_template(event=node.mode)
        self._current_task = instance

        # Resolve attribute list into a plain dict (key -> resolved value)
        options_dict: typing.Dict[str, typing.Any] = {}
        if node.options:
            options_dict = dict(attr.accept(self) for attr in node.options)

        # Try to build a real Options object; fall back to a minimal shim when
        # Options cannot be instantiated (tests/environments may lack deps or
        # the Options model may raise during preformat hooks).
        try:
            opts = Options.from_dict(options_dict) if options_dict else Options.from_dict({})
        except Exception:
            class _FallbackOptions:
                def __init__(self):
                    # Extras is the only piece the meta-events consume at generator time
                    self.extras: typing.Dict[str, typing.Any] = {}

                # Minimal compatibility surface used elsewhere (merge_with is used by meta flow)
                def merge_with(self, other: "_FallbackOptions") -> None:
                    try:
                        self.extras.update(getattr(other, "extras", {}) or {})
                    except Exception:
                        pass

            opts = _FallbackOptions()

        # Store the template event reference in extras for runtime resolution.
        # Use a namespaced string when the template namespace is provided.
        if getattr(opts, "extras", None) is None:
            # Some Options implementations may not expose extras; create it defensively
            try:
                opts.extras = {}
            except Exception:
                # If we cannot set extras, fail early with a parse error
                raise PointyParseError("Failed to attach options extras for meta-task")

        template_ref = (
            f"{node.template_event_namespace}::{node.template_task}"
            if node.template_event_namespace and node.template_event_namespace != "local"
            else node.template_task
        )

        # Put template_class into extras so ControlFlowEvent can resolve it at runtime
        opts.extras["template_class"] = template_ref

        instance.options = opts
        return instance

    def visit_variable_declaration(self, node: ast.VariableDeclNode):
        # Resolve the variable's value expression and return it.
        # The parser already wires VariableAccessNode.value to the AST node
        # representing the declared value, so resolving here simply dispatches
        # to the appropriate visitor for that expression and returns the
        # resulting Python value (or AST-derived structure).
        return node.value.accept(self)

    def visit_null_coalesce(self, node: ast.NullCoalesceExprNode):
        # Evaluate left operand first. If it's a runtime 'null' representation
        # we should return the right-hand value, otherwise return left.
        left_value = node.left.accept(self)

        # In the parser/LiteralNode representation, a null literal is stored
        # as the string 'null' (LiteralType.NULL). Treat both Python None and
        # the literal string 'null' as null values here.
        if left_value is None or left_value == "null":
            return node.right.accept(self)

        return left_value

    def visit_comparison_expr(self, node: ast.ComparisonExprNode):
        # Evaluate both sides first
        left_value = node.left.accept(self)
        right_value = node.right.accept(self)

        return self._compare(node.operator, left_value, right_value)

    def visit_branch(self, node: ast.BranchNode):
        instance = node.task.accept(self)

        if instance is None:
            logger.warning(f"visit_branch: task sub-tree returned None for {node}")
            return None

        root = instance.get_root()
        root.descriptor = node.condition.accept(self)
        root.descriptor_pipe = node.operator
        return root

    def visit_index_expr(self, node: ast.IndexExprNode):
        # Evaluate the collection and the index expression
        collection_value = node.collection.accept(self)
        index_value = node.index.accept(self)

        # Use the visitor's safe indexing helper to attempt the access.
        result = self._safe_index(collection_value, index_value)

        # If safe indexing returns None it's either out-of-bounds or an
        # unsupported indexing operation — raise a parse error to signal
        # the problem to the caller.
        if result is None:
            raise PointyParseError(
                f"Indexing error: cannot index {collection_value!r} with {index_value!r}"
            )

        return result

    def visit_retry(self, node: ast.RetryNode):
        # Visit the job sub-node to construct the task/grouping instance
        node_instance = node.job.accept(self)

        if node_instance is None:
            logger.warning(f"visit_retry: job sub-tree returned None for {node}")
            return None

        # Resolve the attempts literal
        attempts_value = node.attempts.accept(self)

        # Validate attempts is an integer
        if not isinstance(attempts_value, int):
            raise PointyParseError(
                f"Retry attempts must be an integer literal, got: {attempts_value!r}"
            )

        # Ensure options object exists on the target instance
        if getattr(node_instance, "options", None) is None:
            # Create an Options instance with a safe minimal payload that
            # avoids triggering a known preformat bug in Options.preformat_*
            # which can cause tuple coercion when called with None.
            # Providing a concrete retry_attempts key avoids calling the
            # problematic preformat for result_evaluation_strategy.
            try:
                node_instance.options = Options.from_dict({"retry_attempts": 0})
            except Exception:
                # Fallback: create a minimal options-like object to hold
                # retry_attempts in environments where Options cannot be
                # instantiated (e.g., missing dependencies during tests).
                class _FallbackOptions:
                    def __init__(self):
                        self.retry_attempts = 0
                        self.extras = {}

                node_instance.options = _FallbackOptions()

        # Apply the retry attempts
        node_instance.options.retry_attempts = attempts_value

        return node_instance

    def visit_attribute(self, node: ast.AttributeNode) -> typing.Tuple[str, typing.Any]:
        """Returns a (key, resolved_value) pair; callers build a dict via dict(attr.accept(self) for attr in options)."""
        return node.attr, node.value.accept(self)

    def visit_ternary_expr(self, node: ast.TernaryExprNode):
        # Evaluate the condition and choose which branch to evaluate.
        cond_value = node.condition.accept(self)

        if self._is_truthy(cond_value):
            return node.true_expr.accept(self)
        return node.false_expr.accept(self)

    def visit_access_environment_variable(self, node: ast.EnvironmentVariableAccessNode):
        # Resolve environment variable via the AST helper. This returns the
        # environment value or None if not set. Keep behaviour consistent with
        # visit_variable_access which delegates to the node's resolve().
        return node.resolve()

    def generate(self) -> typing.Optional[TaskProtocol]:
        if self._current_task is None:
            return None
        return self._current_task.get_root()
