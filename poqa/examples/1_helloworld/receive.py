from poqa import client
from poqa.serializers.yaml import YamlSerializer

class HelloClient(client.AsyncClient):
    hello = client.Queue()

    def __init__(self):
        pass

    @hello.consumer(no_ack=True)
    def print_message(self, message):
        print ("-> %s" % message.payload)

    #print_message.add_serializer(YamlSerializer)

if __name__ == '__main__':
    c = HelloClient()
    c.start()
