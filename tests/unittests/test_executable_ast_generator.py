import unittest

from volnux import EventBase
from volnux.parser import pointy_parser
from volnux.parser.ast import BranchNode, DescriptorNode, TaskNode
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.conditional import StandardDescriptor
from volnux.parser.operator import PipeType
from volnux.task import PipelineTask
from volnux.task.group import PipelineTaskGrouping


def _build(code: str):
    """Helper: parse Pointy code and run it through ExecutableASTGenerator."""
    ast = pointy_parser(code)
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(ast)
    return generator.generate()


class TestExecutableASTGeneratorSetup(unittest.TestCase):
    """Tests for ExecutableASTGenerator construction and basic behaviour."""

    def test_generator_initial_state(self):
        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        self.assertIs(generator.task_template, PipelineTask)
        self.assertIs(generator.grouping_template, PipelineTaskGrouping)
        self.assertIsNone(generator._generated_task_chain)
        self.assertIsNone(generator._current_task)

    def test_generate_returns_none_for_empty_program(self):
        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        generator.visit_program(pointy_parser(""))
        self.assertIsNone(generator.generate())

    def test_generate_returns_none_for_directives_only(self):
        generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
        generator.visit_program(pointy_parser('@mode:"CFG"'))
        self.assertIsNone(generator.generate())


class TestExecutableASTGeneratorEvents(unittest.TestCase):
    """Integration tests that mirror build_pipeline_flow_from_pointy_code."""

    @classmethod
    def setUpClass(cls):
        class Alpha(EventBase):
            def process(self, *args, **kwargs):
                return True, "alpha"

        class Beta(EventBase):
            def process(self, *args, **kwargs):
                return True, "beta"

        class Gamma(EventBase):
            def process(self, *args, **kwargs):
                return True, "gamma"

        cls.Alpha = Alpha
        cls.Beta = Beta
        cls.Gamma = Gamma

    # ------------------------------------------------------------------
    # Single task
    # ------------------------------------------------------------------

    def test_single_task_returns_pipeline_task(self):
        root = _build("Alpha")
        self.assertIsInstance(root, PipelineTask)
        self.assertEqual(root.get_event_name(), "Alpha")

    def test_single_task_has_no_successor(self):
        root = _build("Alpha")
        self.assertIsNone(root.condition_node.on_success_event)
        self.assertIsNone(root.condition_node.on_success_pipe)

    # ------------------------------------------------------------------
    # Linear chain  A -> B -> C
    # ------------------------------------------------------------------

    def test_linear_chain_root_is_first_task(self):
        root = _build("Alpha->Beta->Gamma")
        self.assertIsInstance(root, PipelineTask)
        self.assertEqual(root.get_event_name(), "Alpha")

    def test_linear_chain_pipe_types(self):
        root = _build("Alpha->Beta->Gamma")
        self.assertEqual(root.condition_node.on_success_pipe, PipeType.POINTER)
        self.assertEqual(
            root.condition_node.on_success_event.condition_node.on_success_pipe,
            PipeType.POINTER,
        )

    def test_linear_chain_last_task_has_no_successor(self):
        root = _build("Alpha->Beta->Gamma")
        last = root.condition_node.on_success_event.condition_node.on_success_event
        self.assertIsInstance(last, PipelineTask)
        self.assertEqual(last.get_event_name(), "Gamma")
        self.assertIsNone(last.condition_node.on_success_pipe)

    def test_linear_chain_parent_links(self):
        root = _build("Alpha->Beta->Gamma")
        beta = root.condition_node.on_success_event
        gamma = beta.condition_node.on_success_event
        self.assertIsNone(root.parent_node)
        self.assertIs(beta.parent_node, root)
        self.assertIs(gamma.parent_node, beta)

    # ------------------------------------------------------------------
    # Parallel execution  A || B
    # ------------------------------------------------------------------

    def test_parallel_execution_pipe_type(self):
        root = _build("Alpha||Beta")
        self.assertEqual(root.condition_node.on_success_pipe, PipeType.PARALLELISM)

    # ------------------------------------------------------------------
    # Result-pipe  A |-> B
    # ------------------------------------------------------------------

    def test_result_pipe_type(self):
        root = _build("Alpha|->Beta")
        self.assertEqual(root.condition_node.on_success_pipe, PipeType.PIPE_POINTER)

    # ------------------------------------------------------------------
    # Parallel then result-piped  A || B |-> C
    # ------------------------------------------------------------------

    def test_parallel_then_result_pipe_chain(self):
        root = _build("Alpha||Beta|->Gamma")
        self.assertEqual(root.condition_node.on_success_pipe, PipeType.PARALLELISM)
        parallel_node = root.condition_node.on_success_event
        self.assertEqual(
            parallel_node.condition_node.on_success_pipe, PipeType.PIPE_POINTER
        )

    # ------------------------------------------------------------------
    # Directive is processed without error
    # ------------------------------------------------------------------

    def test_directive_with_chain_does_not_raise(self):
        root = _build('@mode:"CFG" Alpha->Beta')
        self.assertIsInstance(root, PipelineTask)
        self.assertEqual(root.get_event_name(), "Alpha")

    # ------------------------------------------------------------------
    # generate() always returns root of the chain
    # ------------------------------------------------------------------

    def test_generate_returns_root_not_tail(self):
        root = _build("Alpha->Beta->Gamma")
        self.assertEqual(root.get_event_name(), "Alpha")
        self.assertIsNone(root.parent_node)

    # ------------------------------------------------------------------
    # Conditional branching  A(0->B, 1->C) -> S
    # ------------------------------------------------------------------

    def test_conditional_task_is_conditional(self):
        root = _build("Alpha(0->Beta,1->Gamma)")
        self.assertIsInstance(root, PipelineTask)
        self.assertTrue(root.is_conditional)

    def test_conditional_branches_are_descriptor_tasks(self):
        root = _build("Alpha(0->Beta,1->Gamma)")
        self.assertTrue(root.condition_node.on_success_event.is_descriptor_task)
        self.assertTrue(root.condition_node.on_failure_event.is_descriptor_task)

    def test_conditional_branches_have_correct_event_names(self):
        root = _build("Alpha(0->Beta,1->Gamma)")
        # descriptor 0 (FAILURE) → Beta, descriptor 1 (SUCCESS) → Gamma
        success = root.condition_node.on_success_event
        failure = root.condition_node.on_failure_event
        self.assertEqual(success.get_event_name(), "Gamma")
        self.assertEqual(failure.get_event_name(), "Beta")

    def test_conditional_with_sink(self):
        root = _build("Alpha(0->Beta,1->Gamma)->Alpha")
        self.assertIsNotNone(root.sink_node)
        self.assertEqual(root.sink_pipe, PipeType.POINTER)
        self.assertTrue(root.sink_node.is_sink)

    def test_conditional_sink_event_name(self):
        root = _build("Alpha(0->Beta,1->Gamma)->Alpha")
        self.assertEqual(root.sink_node.get_event_name(), "Alpha")

    def test_conditional_branches_parent_is_conditional_task(self):
        root = _build("Alpha(0->Beta,1->Gamma)")
        success = root.condition_node.on_success_event
        failure = root.condition_node.on_failure_event
        self.assertIs(success.parent_node, root)
        self.assertIs(failure.parent_node, root)

    def test_conditional_preceded_by_linear_chain(self):
        root = _build("Alpha->Beta(0->Gamma,1->Alpha)")
        self.assertIsInstance(root, PipelineTask)
        self.assertEqual(root.get_event_name(), "Alpha")
        branch_task = root.condition_node.on_success_event
        self.assertIsInstance(branch_task, PipelineTask)
        self.assertTrue(branch_task.is_conditional)

    def test_conditional_followed_by_chain(self):
        # A(0->B,1->C)->S  — sink is chained after the conditional
        root = _build("Alpha(0->Beta,1->Gamma)->Alpha")
        self.assertIsNotNone(root.sink_node)
        sink = root.sink_node
        self.assertIsInstance(sink, PipelineTask)
        self.assertEqual(sink.get_event_name(), "Alpha")

    # ------------------------------------------------------------------
    # Multiple independent calls produce independent graphs
    # ------------------------------------------------------------------

    def test_independent_generators_produce_independent_graphs(self):
        root1 = _build("Alpha->Beta")
        root2 = _build("Alpha->Gamma")
        # Changing one graph must not affect the other
        self.assertIsNot(root1, root2)
        self.assertEqual(
            root1.condition_node.on_success_event.get_event_name(), "Beta"
        )
        self.assertEqual(
            root2.condition_node.on_success_event.get_event_name(), "Gamma"
        )


# ------------------------------------------------------------------
# Direct unit tests for visit_branch
# ------------------------------------------------------------------

class TestVisitBranch(unittest.TestCase):
    """Unit tests for ExecutableASTGenerator.visit_branch."""

    def setUp(self):
        class Alpha(EventBase):
            def process(self, *args, **kwargs):
                return True, "alpha"

        class Beta(EventBase):
            def process(self, *args, **kwargs):
                return True, "beta"

        self.Alpha = Alpha
        self.Beta = Beta
        self.generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)

    def _branch(self, descriptor: int, operator: str, task_name: str):
        return BranchNode(
            condition=DescriptorNode(descriptor),
            operator=operator,
            task=TaskNode(task=task_name, options=None),
        )

    def test_visit_branch_returns_pipeline_task(self):
        branch = self._branch(1, "->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertIsInstance(result, PipelineTask)

    def test_visit_branch_stamps_descriptor(self):
        branch = self._branch(0, "->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.descriptor, 0)

    def test_visit_branch_stamps_descriptor_success(self):
        branch = self._branch(StandardDescriptor.SUCCESS, "->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.descriptor, StandardDescriptor.SUCCESS)

    def test_visit_branch_stamps_descriptor_failure(self):
        branch = self._branch(StandardDescriptor.FAILURE, "->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.descriptor, StandardDescriptor.FAILURE)

    def test_visit_branch_stamps_pointer_operator(self):
        branch = self._branch(1, "->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.descriptor_pipe, "->")

    def test_visit_branch_stamps_pipe_pointer_operator(self):
        branch = self._branch(1, "|->", "Alpha")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.descriptor_pipe, "|->")

    def test_visit_branch_task_name(self):
        branch = self._branch(0, "->", "Beta")
        result = self.generator.visit_branch(branch)
        self.assertEqual(result.get_event_name(), "Beta")

    def test_visit_branch_returns_root_of_sub_chain(self):
        """For a multi-task sub-chain the descriptor is stamped on the root (first task)."""
        root = _build("Alpha(0->Beta->Gamma,1->Alpha)")
        failure_branch = root.condition_node.on_failure_event  # descriptor 0
        self.assertEqual(failure_branch.get_event_name(), "Beta")
        self.assertEqual(failure_branch.descriptor, StandardDescriptor.FAILURE)


# ------------------------------------------------------------------
# Additional integration tests for conditional branching
# ------------------------------------------------------------------

class TestConditionalBranchIntegration(unittest.TestCase):
    """Extended integration tests for conditional branches via visit_branch."""

    @classmethod
    def setUpClass(cls):
        class Alpha(EventBase):
            def process(self, *args, **kwargs):
                return True, "alpha"

        class Beta(EventBase):
            def process(self, *args, **kwargs):
                return True, "beta"

        class Gamma(EventBase):
            def process(self, *args, **kwargs):
                return True, "gamma"

        cls.Alpha = Alpha
        cls.Beta = Beta
        cls.Gamma = Gamma

    # Three-branch conditional
    def test_three_branch_conditional_descriptor_config(self):
        root = _build("Alpha(0->Beta,1->Gamma,2->Alpha)")
        self.assertTrue(root.is_conditional)
        config = root.condition_node.get_descriptor_config(2)
        self.assertIsNotNone(config)
        self.assertEqual(config.task.get_event_name(), "Alpha")

    def test_three_branch_conditional_failure_branch(self):
        root = _build("Alpha(0->Beta,1->Gamma,2->Alpha)")
        failure = root.condition_node.on_failure_event
        self.assertEqual(failure.get_event_name(), "Beta")

    def test_three_branch_conditional_success_branch(self):
        root = _build("Alpha(0->Beta,1->Gamma,2->Alpha)")
        success = root.condition_node.on_success_event
        self.assertEqual(success.get_event_name(), "Gamma")

    # Branch whose task is a sub-chain
    def test_branch_with_sub_chain_descriptor_on_root(self):
        root = _build("Alpha(0->Beta->Gamma,1->Alpha)")
        failure = root.condition_node.on_failure_event  # Beta is the root
        self.assertEqual(failure.get_event_name(), "Beta")
        self.assertEqual(failure.descriptor, StandardDescriptor.FAILURE)
        # Gamma is the tail — it must NOT carry the descriptor
        tail = failure.condition_node.on_success_event
        self.assertEqual(tail.get_event_name(), "Gamma")
        self.assertIsNone(tail.descriptor)

    def test_branch_with_sub_chain_tail_connected(self):
        root = _build("Alpha(0->Beta->Gamma,1->Alpha)")
        failure = root.condition_node.on_failure_event
        tail = failure.condition_node.on_success_event
        self.assertEqual(tail.get_event_name(), "Gamma")

    # |-> pipe type on a branch
    def test_branch_pipe_pointer_operator(self):
        root = _build("Alpha(0->Beta,1|->Gamma)")
        success_pipe = root.condition_node.on_success_pipe
        self.assertEqual(success_pipe, PipeType.PIPE_POINTER)

    # Nested conditional
    def test_nested_conditional_outer_is_conditional(self):
        root = _build("Alpha(0->Beta(0->Gamma,1->Alpha),1->Gamma)")
        self.assertTrue(root.is_conditional)

    def test_nested_conditional_inner_is_conditional(self):
        root = _build("Alpha(0->Beta(0->Gamma,1->Alpha),1->Gamma)")
        failure = root.condition_node.on_failure_event
        self.assertTrue(failure.is_conditional)
        self.assertEqual(failure.get_event_name(), "Beta")

    def test_nested_conditional_inner_branches(self):
        root = _build("Alpha(0->Beta(0->Gamma,1->Alpha),1->Gamma)")
        beta = root.condition_node.on_failure_event
        self.assertEqual(beta.condition_node.on_failure_event.get_event_name(), "Gamma")
        self.assertEqual(beta.condition_node.on_success_event.get_event_name(), "Alpha")

    # Parent links
    def test_branch_parent_links(self):
        root = _build("Alpha(0->Beta,1->Gamma)")
        self.assertIs(root.condition_node.on_failure_event.parent_node, root)
        self.assertIs(root.condition_node.on_success_event.parent_node, root)


