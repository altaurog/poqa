from poqa import client
from poqa.serializers.yaml import YamlSerializer

class HelloClient(client.AsyncClient):
    hello = client.Queue()
    count = 0

    @hello.basic_consumer(no_ack=True)
    def print_message(self, channel, method, props, body):
        print(props.__dict__)
        print ("-> %r" % body)

    print_message.add_serializer(YamlSerializer)

if __name__ == '__main__':
    c = HelloClient()
    c.start()
