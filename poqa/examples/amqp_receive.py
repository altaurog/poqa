"""
Asyncronous amqp consumer; do our processing via an ioloop timeout
"""
import time

from poqa import client

class TimingClient(client.AsyncClient):
    def __init__(self):
        self.start_time = time.time()
        super(TimingClient, self).__init__()

    test = client.Queue(durable=True, exclusive=False, auto_delete=False,)

    count = 0
    last_count = 0

    @client.BasicConsumer(queue='test', no_ack=True)
    def timing_handler(self, channel, method, header, body):
        self.count += 1
        if not self.count % 1000:
            now = time.time()
            duration = now - self.start_time
            sent = self.count - self.last_count
            rate = sent / duration
            self.last_count = self.count
            self.start_time = now
            print "timed_receive: %i Messages Received, %.4f per second" %\
                  (self.count, rate)

if __name__ == '__main__':
    my_client = TimingClient()
    my_client.start()
