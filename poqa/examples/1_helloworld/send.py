from poqa import client

class HelloClient(client.AsyncClient):
    hello = client.Queue()

    @client.task
    def send_message(self, message):
        self.hello.basic_publish(message)
        exit()

if __name__ == '__main__':
    c = HelloClient()
    c.send_message('Hello world')
    c.start()

