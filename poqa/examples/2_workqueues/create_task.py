from poqa import client

class TaskFactory(client.AsyncClient):
    task_queue = client.Queue(durable=True)

    @task_queue.basic_publisher
    def run(self):
        for i in range(6):
            yield 'task'

    def on_queue_declare(self, frame):
        print "queue consumers: %d" % frame.method.consumer_count
        print "queue messages: %d" % frame.method.message_count

    @client.task(interval=1, auto=True)
    def check_queue(self):
        self.task_queue.declare(callback=self.on_queue_declare)


if __name__ == "__main__":
    c = TaskFactory()
    c.start()
