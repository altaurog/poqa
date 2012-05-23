import random
import sys
from poqa import client

random.seed()

class TaskFactory(client.AsyncClient):
    task_queue = client.Queue(durable=True)

    @task_queue.publisher
    def run(self):
        if len(sys.argv) > 1:
            messages = sys.argv[1:]
        else:
            messages = ['%0.2f' % random.random() for i in range(6)]

        for m in messages:
            yield m

if __name__ == "__main__":
    c = TaskFactory()
    c.start()
