import sys
from . import client

class LogClient(client.AsyncClient):
    log_direct = client.Exchange(type='direct')

    @client.task
    def send_message(self, message, severity='info'):
        self.log_direct.basic_publish(message,
                                routing_key=severity)
        exit()

if __name__ == '__main__':
    c = LogClient()
    c.send_message(*sys.argv[1:])
    c.start()

