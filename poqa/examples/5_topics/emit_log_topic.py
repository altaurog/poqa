import sys
from . import client

class LogClient(client.AsyncClient):
    log_topic = client.Exchange(type='topic')

    @client.task
    def send_message(self, topics):
        message = 'Lorem ipsum'
        for t in topics:
            self.log_topic.basic_publish(message, routing_key=t)
        exit()

if __name__ == '__main__':
    c = LogClient()
    c.send_message(sys.argv[1:])
    c.start()

