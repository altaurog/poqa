from random import choice
from string import ascii_letters
import sys
import time

from poqa import client

class Worker(client.AsyncClient):
    task_queue = client.Queue(durable=True)

    def __init__(self, name):
        self.name = name

    @task_queue.consumer(no_ack=True)
    def print_message(self, message):
        tag = message.delivery_info['delivery_tag']
        payload = message.payload
        print ("%10s starting %d %s" % (self.name, tag, payload))
        try:
            duration = float(payload)
        except ValueError:
            pass
        else:
            time.sleep(duration)
        print ("%10s finished %d" % (self.name, tag))

if __name__ == '__main__':
    if len(sys.argv) > 1:
        name = sys.argv[1]
    else:
        name = ''.join([choice(ascii_letters) for i in range(8)])
    c = Worker(name)
    c.start()

