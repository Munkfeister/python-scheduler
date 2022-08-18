import sys
sys.path.append("./dependencies")

from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

class Scheduler(object):
    jobstores = {
        'redis': RedisJobStore()
    }

    executors = {
        'default': ThreadPoolExecutor(20)
    }

    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }

    def __init__(self) -> None:
        self.scheduler = BackgroundScheduler(
            jobstores=self.jobstores,
            executors=self.executors,
            job_defaults=self.job_defaults,
            timezone=utc
        )

    def start(self):
        self.scheduler.start()