import sys
import time

from . import client

class LogClient(client.AsyncClient):
    log_direct = client.Exchange(type='direct')
    logq_ = client.Queue()
    for severity in sys.argv[1:]:
        logq_.bind(log_direct, routing_key=severity)

    @logq_.basic_consumer(no_ack=True)
    def print_message(self, channel, method, props, body):
        print ("-> %s: %s" % (method.routing_key, body))

if __name__ == '__main__':
    c = LogClient()
    c.start()

