from poqa import client
from poqa.serializers.yaml import YamlSerializer

class HelloClient(client.AsyncClient):
    hello = client.Queue()
    next_id = 0

    @hello.basic_publisher
    def run(self):
        yield ['hello world', 2]
        exit()

    run.serializer = YamlSerializer
    run.properties = client.Properties(delivery_mode=2)

if __name__ == '__main__':
    HelloClient().start()

