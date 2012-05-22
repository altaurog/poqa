from poqa import client
from poqa.serializers.yaml import YamlSerializer

class HelloClient(client.AsyncClient):
    hello = client.Queue()

    @hello.publisher
    def run(self):
        print "sending hello"
        yield ['hello world', 2]
        exit()

    run.serializer = YamlSerializer

if __name__ == '__main__':
    c = HelloClient()
    c.start()

