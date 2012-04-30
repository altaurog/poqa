from poqa import client

class HelloClient(client.AsyncClient):
    hello = client.Queue()
    count = 0

    @hello.basic_consumer(no_ack=True)
    def print_message(self, channel, method, props, body):
        print ("-> %s" % body)


if __name__ == '__main__':
    c = HelloClient()
    c.start()
