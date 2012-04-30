import sys

from poqa import client

class LogClient(client.AsyncClient):
    log_topic = client.Exchange(type='topic')
    logq_ = client.Queue(exclusive=True)
    for binding in sys.argv[1:]:
        logq_.bind(log_topic, routing_key=binding)

    @logq_.basic_consumer(no_ack=True)
    def print_message(self, channel, method, props, body):
        print ("-> %s: %s" % (method.routing_key, body))

if __name__ == '__main__':
    c = LogClient()
    c.start()

