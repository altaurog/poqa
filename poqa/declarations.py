import copy
import functools
import types

from .serializers import Serializers

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
        is_empty_kwargs = not len(kwargs)
        kwargs['routing_key'] = self
        decorator = Publisher(**kwargs)
        if callable(publisher_func) and is_empty_kwargs:
            return decorator(publisher_func)
        else:
            return decorator

    def publish(self, message, serializer=None, **kwargs):
        channel = self.client().channel
        exchange = ''
        routing_key = self.queue_name
        if serializer:
            serializer.serialize(message)
        channel.basic.publish(message, exchange, routing_key, **kwargs)

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
        is_empty_kwargs = not len(kwargs)
        kwargs['exchange'] = self
        decorator = Publisher(**kwargs)
        if callable(publisher_func) and is_empty_kwargs:
            return decorator(publisher_func)
        else:
            return decorator

    def publish(self, message, routing_key='', serializer=None, **kwargs):
        channel = self.client().channel
        exchange = self.exchange_name
        if serializer:
            serializer.serialize(message)
        channel.basic.publish(message, exchange, routing_key, **kwargs)


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
        channel.basic.consume(self.handler, queue=queue, **self.kwargs)

    def handler(self, message):
        if self.serializers:
             self.serializers.deserialize(message)
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
    def __init__(self, exchange='', routing_key='', serializer=None, properties=None, **kwargs):
        """
        Store the arguments as attributes.
        Preserve the class attributes, which are decorators
        that will allow us to update the publisher parameters.
        """
        self.args = {'exchange': exchange,
                     'routing_key': routing_key,
                     'serializer': serializer,
                     'properties': properties}
        self.kwargs = kwargs

    def on_class_create(self, cls):
        """
        Now apply args we got when created, but don't overwrite
        changes that might have been made in class body
        (i.e. with decorators below)
        """
        for key, value in self.args.iteritems():
            self.__dict__.setdefault(key, value)
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
                self.publish(client, message)
        return publish_wrapper.__get__(instance, cls)

    def publish(self, client, message):
        exchange = self.exchange
        if callable(exchange):
            exchange = exchange(client, message)
        if isinstance(exchange, Exchange):
            exchange = exchange.exchange_name

        routing_key = self.routing_key
        if callable(routing_key):
            routing_key = routing_key(client, message)
        if isinstance(routing_key, Queue):
            routing_key = routing_key.queue_name

        if self.serializer:
            self.serializer.serialize(message)
        client.channel.basic.publish(message, exchange, routing_key, **self.kwargs)

    # decorators
    def serializer(self, obj):
        self.args['serializer'] = obj
        return obj

    def exchange(self, func):
        self.args['exchange'] = func
        return func

    def routing_key(self, func):
        self.args['routing_key'] = func
        return func

class Task(object):
    def __init__(self, func, delay=None, interval=None, auto=False, **kwargs):
        self._decorated_func = func
        self.kwargs = kwargs
        self.delay = delay
        self.interval = interval
        self.auto = auto
        if delay is None:
            self.delay = interval or 0

    def __get__(self, instance, cls):
        @functools.wraps(self._decorated_func)
        def schedule(client, *args, **kwargs):
            kwargs_ = copy.copy(self.kwargs)
            kwargs_.update(kwargs)
            delay = kwargs_.pop('delay', self.delay)
            interval = kwargs_.pop('interval', self.interval)
            def task_func():
                self._decorated_func(client, *args, **kwargs_)
            client.insert_task(task_func, delay, interval)
        return schedule.__get__(instance, cls)

def task(func=None, **kwargs):
    if callable(func) and kwargs == {}:
        return Task(func)
    else:
        def decorator(func):
            return Task(func, **kwargs)
        return decorator

