import copy
import functools
import random
import time

random.seed()

__all__ = [
            "TaskLoop",
            "Task",
            "task",
]

class TaskLoop(object):
    timeout = 0.5
    def __init__(self):
        self.tasks = {}
        self.stopped = False

    def start(self):
        while not self.stopped:
            if not self.process_tasks():
                time.sleep(self.timeout)

    def stop(self):
        self.stopped = True

    def process_tasks(self):
        now = time.time()
        done = []
        for task_id, task in self.tasks.iteritems():
            if task['timeout'] <= now:
                task['callback']()
                if task['interval'] is not None:
                    task['timeout'] = now + task['interval']
                else:
                    done.append(task_id)

        result = bool(done)
        for task_id in done:
            del self.tasks[task_id]

        return result

    def insert_task(self, task, delay=0, interval=None):
        task_id = self.getid()
        timeout = time.time() + delay
        self.tasks[task_id] = {'callback':task,
                                'timeout':timeout,
                                'interval':interval}
        return task_id

    def remove_task(self, task_id):
        del self.tasks[task_id]

    def getid(self):
        return random.getrandbits(32), time.time()


class Task(object):
    def __init__(self, func, delay=None, interval=None, auto=False, **kwargs):
        self._decorated_func = func
        self.kwargs = kwargs
        self.delay = delay
        self.interval = interval
        self.auto = auto
        if delay is None:
            self.delay = interval or 0

    def __get__(self, instance, cls):
        @functools.wraps(self._decorated_func)
        def schedule(client, *args, **kwargs):
            kwargs_ = copy.copy(self.kwargs)
            kwargs_.update(kwargs)
            delay = kwargs_.pop('delay', self.delay)
            interval = kwargs_.pop('interval', self.interval)
            def task_func():
                self._decorated_func(client, *args, **kwargs_)
            client.insert_task(task_func, delay, interval)
        return schedule.__get__(instance, cls)

def task(func=None, **kwargs):
    if callable(func) and kwargs == {}:
        return Task(func)
    else:
        def decorator(func):
            return Task(func, **kwargs)
        return decorator

