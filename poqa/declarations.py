import functools
import types

import haigha.message

from .serializers import Message, Serializers

__all__ = [
            "Declaration",
            "Queue",
            "Exchange",
            "Decorator",
            "Consumer",
            "Publisher",
]

def publish(channel, message, exchange, routing_key, serializer, **kwargs):
    if serializer:
        if not isinstance(message, (Message, haigha.message.Message)):
            message = Message(message)
        message = serializer.serialize(message)
    else:
        if not isinstance(message, haigha.message.Message):
            message = haigha.message.Message(message)
    channel.basic.publish(message, exchange, routing_key, **kwargs)


class Declaration(object):
    "Base class for declarative style"

class Queue(Declaration):
    def __init__(self, **kwargs):
        super(Queue, self).__init__()
        self.kwargs = kwargs
        self._bindings = []

    def declare(self):
        if not self.name.endswith('_'):
            name = self.kwargs.setdefault('queue', self.name)
        else:
            name = self.kwargs.get('queue')

        if name:
            print "declaring queue %r" % name
        else:
            print "declaring temorary queue for %s" % self.name
            self.kwargs.setdefault('exclusive', True)
        
        channel = self.client().channel
        channel.queue.declare(cb=self.on_declare, **self.kwargs)

    def on_declare(self, queue, *args):
        self.queue_name = queue
        for exchange, kwargs in self._bindings:
            self.bind(exchange, **kwargs)

    def bind(self, exchange, **kwargs):
        channel = self.client().channel
        queue_name = getattr(self, 'queue_name', False)
        if queue_name:
            if isinstance(exchange, Exchange):
                exchange = exchange.exchange_name
            channel.queue.bind( queue=self.queue_name,
                                exchange=exchange,
                                **kwargs)
        else:
            self._bindings.append((exchange, kwargs))

    def consumer(self, consumer_callback=None, **kwargs):
        kwargs['queue'] = self
        decorator = Consumer(**kwargs)
        if callable(consumer_callback) and kwargs.keys() == ['queue']:
            return decorator(consumer_callback)
        else:
            return decorator

    def publisher(self, publisher_func=None, **kwargs):
        decorator = Publisher(self, **kwargs)
        if callable(publisher_func) and len(kwargs) == 0:
            return decorator(publisher_func)
        else:
            return decorator

    def publish(self, message, serializer=None, **kwargs):
        channel = self.client().channel
        exchange = ''
        routing_key = self.queue_name
        publish(channel, message, exchange, routing_key, serializer, **kwargs)

class Exchange(Declaration):
    def __init__(self, **kwargs):
        super(Exchange, self).__init__()
        self.kwargs = kwargs

    def declare(self):
        self.exchange_name = self.kwargs.setdefault('exchange', self.name)
        print "declaring exchange '%s'" % self.exchange_name
        channel = self.client().channel
        channel.exchange.declare(**self.kwargs)

    def publisher(self, publisher_func=None, **kwargs):
        decorator = Publisher(self, **kwargs)
        if callable(publisher_func) and len(kwargs) == 0:
            return decorator(publisher_func)
        else:
            return decorator

    def publish(self, message, serializer=None, **kwargs):
        channel = self.client().channel
        exchange = self.exchange_name
        routing_key = message.routing_key or ''
        publish(channel, message, exchange, routing_key, serializer, **kwargs)


class Decorator(Declaration):
    _decorated_func = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, func):
        self._decorated_func = func
        return functools.wraps(func, updated=())(self)

    def on_class_create(self, cls):
        pass


class Consumer(Decorator):
    def __init__(self, queue='', serializers=None, auto_ack=False, **kwargs):
        self.queue = queue
        self.serializers = serializers or Serializers()
        self.auto_ack = auto_ack
        if self.auto_ack:
            if kwargs.get('no_ack', False):
                msg = "A consumer may not have both auto_ack and no_ack set."
                raise ValueError(msg)
        super(Consumer, self).__init__(**kwargs)

    def on_class_create(self, cls):
        setattr(cls, self.name, self._decorated_func)

    def declare(self):
        channel = self.client().channel
        queue = self.queue
        if isinstance(queue, Queue):
            queue = queue.queue_name
        print "declaring consumer %r on queue %r" % (self.name, queue)
        channel.basic.consume(queue=queue, consumer=self.handler, **self.kwargs)

    def handler(self, message):
        if self.serializers:
             message = self.serializers.deserialize(message)
        try:
            self._decorated_func(self.client(), message)
        except Exception, e:
            print e
        else:
            if self.auto_ack:
                channel = message.delivery_info['channel']
                delivery_tag = message.delivery_info['delivery_tag']
                channel.basic.ack(delivery_tag)

    def add_serializer(self, serializer):
        self.serializers.append(serializer)

class Publisher(Decorator):
    def __init__(self, parent, serializer=None, **kwargs):
        self.parent = parent
        self.kwargs = kwargs
        if serializer:
            self.serializer = serializer

    def on_class_create(self, cls):
        if isinstance(self.serializer, type):
            self.serializer = self.serializer()

    def __get__(self, instance, cls):
        if self._decorated_func is None:
            return functools.partial(self.publish, instance)
        @functools.wraps(self._decorated_func)
        def publish_wrapper(client, *args, **kwargs):
            result = self._decorated_func(client, *args, **kwargs)
            if not isinstance(result, types.GeneratorType):
                result = (result,)
            for message in result:
                self.parent.publish(message, self.serializer, **self.kwargs)
        return publish_wrapper.__get__(instance, cls)

