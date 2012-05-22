import random
import time

random.seed()

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


