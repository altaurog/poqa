import copy
import functools
import types

from pika import spec

from .serializers import Serializers

class Declaration(object):
    "Base class for declarative style"

class Queue(Declaration):
    def __init__(self, **kwargs):
        super(Queue, self).__init__()
        self.kwargs = kwargs
        self._bindings = []

    def declare(self, callback):
        if not self.name.endswith('_'):
            name = self.kwargs.setdefault('queue', self.name)
        else:
            name = self.kwargs.get('queue')

        if name:
            print "declaring queue %r" % name
        else:
            print "declaring temorary queue for %s" % self.name
            self.kwargs.setdefault('exclusive', True)
        
        self.client_callback = callback
        channel = self.client.channel
        channel.queue_declare(callback=self.on_declare, **self.kwargs)

    def on_declare(self, frame):
        if frame.method.NAME != 'Queue.DeclareOk':
            raise RuntimeError('failed to declare queue: %r' % self.name)
        self.queue_name = frame.method.queue
        self.declare_bindings(None)

    def declare_bindings(self, frame):
        if frame is not None and frame.method.NAME != 'Queue.BindOk':
            raise RuntimeError('failed to bind queue %s' % self.queue_name)

        try:
            bindings_iter = self._bindings_iter
        except AttributeError:
            bindings_iter = iter(self._bindings)
            self._bindings_iter = bindings_iter

        try:
            exchange, kwargs = bindings_iter.next()
        except StopIteration:
            self.client_callback()
        else:
            channel = self.client.channel
            if isinstance(exchange, Exchange):
                exchange = exchange.exchange_name
            channel.queue_bind(callback=self.declare_bindings,
                                queue=self.queue_name,
                                exchange=exchange,
                                **kwargs)

    def bind(self, exchange, **kwargs):
        self._bindings.append((exchange, kwargs))

    def basic_consumer(self, consumer_callback=None, **kwargs):
        kwargs['queue'] = self
        decorator = BasicConsumer(**kwargs)
        if callable(consumer_callback) and kwargs.keys() == ['queue']:
            return decorator(consumer_callback)
        else:
            return decorator

    def basic_publisher(self, publisher_func=None, **kwargs):
        is_empty_kwargs = not len(kwargs)
        kwargs['routing_key'] = self
        decorator = BasicPublisher(**kwargs)
        if callable(publisher_func) and is_empty_kwargs:
            return decorator(publisher_func)
        else:
            return decorator

    def basic_publish(self, body, serializer=None, properties=None, **kwargs):
        channel = self.client.channel
        exchange = ''
        routing_key = self.queue_name
        if serializer:
            if properties is None:
                properties = spec.BasicProperties()
            body = serializer.serialize(body, properties)
        channel.basic_publish(exchange, routing_key, body, **kwargs)

class Exchange(Declaration):
    def __init__(self, **kwargs):
        super(Exchange, self).__init__()
        self.kwargs = kwargs

    def declare(self, callback):
        if not self.name.endswith('_'):
            name = self.kwargs.setdefault('exchange', self.name)
        else:
            name = self.kwargs.get('exchange')
        print "declaring exchange '%s'" % name
        self.callback = callback
        channel = self.client.channel
        channel.exchange_declare(callback=self.on_declare, **self.kwargs)

    def on_declare(self, frame):
        if frame.method.NAME != 'Exchange.DeclareOk':
            raise RuntimeError('failed to declare exchange: %r' % self.name)
        self.exchange_name = self.name
        self.callback()

    def basic_publisher(self, publisher_func=None, **kwargs):
        is_empty_kwargs = not len(kwargs)
        kwargs['exchange'] = self
        decorator = BasicPublisher(**kwargs)
        if callable(publisher_func) and is_empty_kwargs:
            return decorator(publisher_func)
        else:
            return decorator

    def basic_publish(self, body, routing_key='', serializer=None,
                                   properties=None, **kwargs):
        channel = self.client.channel
        exchange = self.exchange_name
        if serializer:
            if properties is None:
                properties = spec.BasicProperties()
            body = serializer.serialize(body, properties)
        channel.basic_publish(exchange, routing_key, body, **kwargs)


class Decorator(Declaration):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, func):
        self._decorated_func = func
        return functools.wraps(func, updated=())(self)

    def on_class_create(self, cls):
        pass

class BasicConsumer(Decorator):
    def __init__(self, queue='', serializers=None, auto_ack=False, **kwargs):
        self.queue = queue
        self.serializers = serializers or Serializers()
        self.auto_ack = auto_ack
        if self.auto_ack:
            if kwargs.get('no_ack', False):
                msg = "A consumer may not have both auto_ack and no_ack set."
                raise ValueError(msg)
        super(BasicConsumer, self).__init__(**kwargs)

    def on_class_create(self, cls):
        setattr(cls, self.name, self._decorated_func)

    def declare(self, channel):
        queue = self.queue
        if isinstance(queue, Queue):
            queue = queue.queue_name
        print "declaring consumer %r on queue %r" % (self.name, queue)
        channel.basic_consume(self.handler, queue=queue, **self.kwargs)

    def handler(self, channel, method, props, body):
        if self.serializers:
            body = self.serializers.deserialize(props, body)
        try:
            self._decorated_func(self.client, channel, method, props, body)
        except Exception, e:
            print e
        else:
            if self.auto_ack:
                channel.basic_ack(delivery_tag=method.delivery_tag)

    def add_serializer(self, serializer):
        self.serializers.append(serializer)

class BasicPublisher(Decorator):
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

        properties = self.properties
        if callable(properties):
            properties = properties(client, message)
        if properties is None:
            properties = spec.BasicProperties()

        if self.serializer:
            message = self.serializer.serialize(message, properties)
        client.channel.basic_publish(exchange, routing_key, message, properties, **self.kwargs)

    # decorators
    def serializer(self, obj):
        self.args['serializer'] = obj
        return obj

    def properties(self, func):
        self.args['properties'] = func
        return func

    def exchange(self, func):
        self.args['exchange'] = func
        return func

    def routing_key(self, func):
        self.args['routing_key'] = func
        return func

class Task(object):
    error_msg = "Specifying both timeout and interval for task is not allowed"
    def __init__(self, func, timeout=None, interval=None, **kwargs):
        self._decorated_func = func
        self.kwargs = kwargs
        self.timeout = timeout
        self.interval = interval
        if interval is not None and timeout is not None:
            raise ValueError(self.error_msg)
        elif interval is None and timeout is None:
            self.timeout = 0

    def __get__(self, instance, cls):
        @functools.wraps(self._decorated_func)
        def schedule(client, *args, **kwargs):
            kwargs_ = copy.copy(self.kwargs)
            kwargs_.update(kwargs)
            timeout = kwargs_.pop('timeout', self.timeout)
            interval = kwargs_.pop('interval', self.interval)
            if interval is not None and timeout is not None:
                raise ValueError(self.error_msg)
            if timeout is not None:
                def task_func():
                    self._decorated_func(client, *args, **kwargs_)
                client.add_timeout(timeout, task_func)
            else:
                def task_func():
                    self._decorated_func(client, *args, **kwargs_)
                    client.add_timeout(interval, task_func)
                client.add_timeout(interval, task_func)
        return schedule.__get__(instance, cls)

def task(func=None, **kwargs):
    if callable(func) and kwargs == {}:
        return Task(func)
    else:
        def decorator(func):
            return Task(func, **kwargs)
        return decorator

