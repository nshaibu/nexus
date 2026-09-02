import logging
import typing
from enum import Enum
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import STATE_RUNNING
from apscheduler.schedulers.background import BackgroundScheduler
from volnux.utils import get_function_call_args, get_expected_args
from volnux.exceptions import ValidationError

if typing.TYPE_CHECKING:
    from volnux.execution.pipeline import Pipeline, BatchPipeline

logger = logging.getLogger(__name__)

_PIPELINE_BACKGROUND_SCHEDULER = BackgroundScheduler()


class _PipeLineJob:
    def __init__(
        self,
        pipeline: typing.Union["Pipeline", "BatchPipeline"],
        scheduler: BackgroundScheduler,
    ):
        from volnux.execution.pipeline import BatchPipeline

        self._pipeline = pipeline
        self._is_batch = isinstance(pipeline, BatchPipeline)
        self._sched = scheduler

    @property
    def id(self) -> str:
        return self._pipeline.id

    def run(self, *args, **kwargs):
        if self._is_batch:
            self._pipeline.execute()
            return
        self._pipeline.start(force_rerun=True)

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)


class ScheduleMixin:

    class ScheduleTrigger(Enum):
        DATE = "date"
        INTERVAL = "interval"
        CRON = "cron"

        def tigger_klass(self):
            if self == self.DATE:
                return DateTrigger
            elif self == self.INTERVAL:
                return IntervalTrigger
            else:
                return CronTrigger

    @classmethod
    def get_pipeline_scheduler(cls):
        return _PIPELINE_BACKGROUND_SCHEDULER

    @staticmethod
    def _validate_trigger_args(
        trigger: ScheduleTrigger, trigger_args: typing.Dict[str, typing.Any]
    ):
        klass = trigger.tigger_klass()
        params = get_function_call_args(klass.__init__, trigger_args)
        if not params or not any([params[key] for key in params]):
            expected_args = list(get_expected_args(klass.__init__).keys())
            raise ValidationError(
                message=f"Invalid trigger arguments. Expected argument(s) {expected_args}",
                code="invalid-args",
                params={"trigger_args": trigger_args},
            )

    def schedule_job(self, trigger: ScheduleTrigger, **scheduler_kwargs):
        """
        Schedule a pipeline job. There are three triggers used for scheduling a job: cron, date, and interval.

        - CronTrigger: Triggers when current time matches all specified time constraints, similarly to how the UNIX cron scheduler works.
            - int|str year: 4-digit year
            - int|str month: month (1-12)
            - int|str day: day of month (1-31)
            - int|str week: ISO week (1-53)
            - int|str day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
            - int|str hour: hour (0-23)
            - int|str minute: minute (0-59)
            - int|str second: second (0-59)
            - datetime|str start_date: earliest possible date/time to trigger on (inclusive)
            - datetime|str end_date: latest possible date/time to trigger on (inclusive)
            - datetime.tzinfo|str timezone: time zone to use for the date/time calculations (defaults to scheduler timezone)
            - int|None jitter: delay the job execution by ``jitter`` seconds at most

        - DateTrigger: Triggers once on the given datetime. If ``run_date`` is left empty, current time is used.
            - datetime|str run_date: the date/time to run the job at
            - datetime.tzinfo|str timezone: time zone for ``run_date`` if it doesn't have one already

        - IntervalTrigger: Triggers on specified intervals, starting on ``start_date`` if specified, ``datetime.now()`` + interval otherwise.
            - int weeks: number of weeks to wait
            - int days: number of days to wait
            - int hours: number of hours to wait
            - int minutes: number of minutes to wait
            - int seconds: number of seconds to wait
            - datetime|str start_date: starting point for the interval calculation
            - datetime|str end_date: latest possible date/time to trigger on
            - datetime.tzinfo|str timezone: time zone to use for the date/time calculations
            - int|None jitter: delay the job execution by ``jitter`` seconds at most

        :param trigger: Trigger to execute
        :param scheduler_kwargs: Keyword arguments to pass to the scheduler
        :return: Job instance
        """
        sched = self.get_pipeline_scheduler()
        _job_op = _PipeLineJob(self, sched)

        self._validate_trigger_args(trigger, scheduler_kwargs)

        job = sched.add_job(
            _job_op,
            trigger.value,
            id=_job_op.id,
            name=self.__class__.__name__,
            **scheduler_kwargs,
        )

        if sched.state != STATE_RUNNING:
            sched.start()
        return job
