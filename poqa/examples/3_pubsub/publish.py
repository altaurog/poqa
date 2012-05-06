import sys
from . import client

class LogClient(client.AsyncClient):
    nlogs = client.Exchange(type='fanout')

    @client.task
    def send_message(self, message):
        self.nlogs.basic_publish(message)
        exit()

if __name__ == '__main__':
    c = LogClient()
    c.send_message(sys.argv[1])
    c.start()

