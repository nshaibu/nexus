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
                    node_instance.options = Options()
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

    def visit_meta_event(self, node: ast.MetaTaskNode):
        pass


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
        pass

    def visit_variable_declaration(self, node: ast.VariableDeclNode):
        pass

    def visit_null_coalesce(self, node: ast.NullCoalesceExprNode):
        pass

    def visit_comparison_expr(self, node: ast.ComparisonExprNode):
        pass

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
        pass

    def visit_retry(self, node: ast.RetryNode):
        pass

    def visit_attribute(self, node: ast.AttributeNode) -> typing.Tuple[str, typing.Any]:
        """Returns a (key, resolved_value) pair; callers build a dict via dict(attr.accept(self) for attr in options)."""
        return node.attr, node.value.accept(self)

    def visit_ternary_expr(self, node: ast.TernaryExprNode):
        pass

    def visit_access_environment_variable(self, node: ast.EnvironmentVariableAccessNode):
        pass

    def generate(self) -> typing.Optional[TaskProtocol]:
        if self._current_task is None:
            return None
        return self._current_task.get_root()
