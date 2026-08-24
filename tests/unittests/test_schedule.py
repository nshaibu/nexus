import pytest
from unittest.mock import MagicMock, patch
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from volnux.mixins.schedule import ScheduleMixin, _PipeLineJob
from volnux.exceptions import ValidationError


class MockPipeline:
    def __init__(self, pipeline_id):
        self.id = pipeline_id

    def start(self, force_rerun=False):
        pass

    def execute(self):
        pass


@pytest.fixture
def mock_scheduler():
    return BackgroundScheduler()


@pytest.fixture
def mock_pipeline():
    return MockPipeline(pipeline_id="test_pipeline")


@pytest.fixture
def schedule_mixin_instance():
    class TestScheduleMixin(ScheduleMixin):
        pass

    return TestScheduleMixin()


def test_pipeline_job_initialization(mock_pipeline, mock_scheduler):
    job = _PipeLineJob(mock_pipeline, mock_scheduler)
    assert job.id == "test_pipeline"
    assert job._pipeline == mock_pipeline
    assert job._sched == mock_scheduler


def test_pipeline_job_run(mock_pipeline, mock_scheduler):
    job = _PipeLineJob(mock_pipeline, mock_scheduler)

    with patch.object(mock_pipeline, "start") as mock_start:
        job.run()
        mock_start.assert_called_once_with(force_rerun=True)


def test_pipeline_job_run_batch(mock_scheduler):
    mock_batch_pipeline = MagicMock()
    job = _PipeLineJob(mock_batch_pipeline, mock_scheduler)
    job._is_batch = True

    with patch.object(mock_batch_pipeline, "execute") as mock_execute:
        job.run()
        mock_execute.assert_called_once()


def test_schedule_trigger_enum():
    assert ScheduleMixin.ScheduleTrigger.DATE.tigger_klass() == DateTrigger
    assert ScheduleMixin.ScheduleTrigger.INTERVAL.tigger_klass() == IntervalTrigger
    assert ScheduleMixin.ScheduleTrigger.CRON.tigger_klass() == CronTrigger


def test_validate_trigger_args_valid(schedule_mixin_instance):
    trigger = ScheduleMixin.ScheduleTrigger.DATE
    trigger_args = {"run_date": "2023-01-01"}

    with patch("volnux.utils.get_function_call_args", return_value=trigger_args):
        with patch("volnux.utils.get_expected_args", return_value={"run_date": None}):
            # Should not raise ValidationError
            schedule_mixin_instance._validate_trigger_args(trigger, trigger_args)


def test_validate_trigger_args_invalid(schedule_mixin_instance):
    trigger = ScheduleMixin.ScheduleTrigger.DATE
    trigger_args = {}

    with patch("volnux.utils.get_function_call_args", return_value=trigger_args):
        with patch("volnux.utils.get_expected_args", return_value={"run_date": None}):
            with pytest.raises(ValidationError) as exc_info:
                schedule_mixin_instance._validate_trigger_args(trigger, trigger_args)
            assert "Invalid trigger arguments" in str(exc_info.value)


def test_schedule_job(schedule_mixin_instance, mock_pipeline, mock_scheduler):
    trigger = ScheduleMixin.ScheduleTrigger.DATE
    scheduler_kwargs = {"run_date": "2023-01-01"}

    schedule_mixin_instance.id = mock_pipeline.id

    with patch.object(
        ScheduleMixin, "get_pipeline_scheduler", return_value=mock_scheduler
    ):
        with patch.object(ScheduleMixin, "_validate_trigger_args") as mock_validate:
            with patch.object(mock_scheduler, "add_job") as mock_add_job:
                job = schedule_mixin_instance.schedule_job(trigger, **scheduler_kwargs)

                mock_validate.assert_called_once_with(trigger, scheduler_kwargs)
                mock_add_job.assert_called_once()
                assert job is not None
