import copy
import functools

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

    def basic_publish(self, body, **kwargs):
        channel = self.client.channel
        exchange = ''
        routing_key = self.queue_name
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

    def basic_publish(self, body, **kwargs):
        channel = self.client.channel
        exchange = self.exchange_name
        routing_key = kwargs.pop('routing_key', '')
        channel.basic_publish(exchange, routing_key, body, **kwargs)

class Consumer(Declaration):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, handler):
        self.handler = handler
        return self

class BasicConsumer(Consumer):
    def declare(self, channel):
        queue = self.kwargs.setdefault('queue', '')
        if isinstance(queue, Queue):
            queue = self.kwargs['queue'] = queue.queue_name
        print "declaring consumer %r on queue %r" % (self.name, queue)
        if self.kwargs.get('auto_ack', False):
            if self.kwargs.get('no_ack', False):
                msg = "A consumer may not have both auto_ack and no_ack set."
                raise ValueError(msg)
            def handler(channel, method, props, body):
                try:
                    self.handler(channel, method, props, body)
                except Exception, e:
                    print e
                else:
                    channel.basic_ack(delivery_tag = method.delivery_tag)
        else:
            try:
                handler = self.handler
            except Exception, e:
                print e
        channel.basic_consume(handler, **self.kwargs)

class Task(object):
    error_msg = "Specifying both timeout and interval for task is not allowed"
    def __init__(self, func, timeout=None, interval=None, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.timeout = timeout
        self.interval = interval
        if interval is not None and timeout is not None:
            raise ValueError(self.error_msg)
        elif interval is None and timeout is None:
            self.timeout = 0

    def __get__(self, instance, cls):
        @functools.wraps(self.func)
        def schedule(client, *args, **kwargs):
            kwargs_ = copy.copy(self.kwargs)
            kwargs_.update(kwargs)
            timeout = kwargs_.pop('timeout', self.timeout)
            interval = kwargs_.pop('interval', self.interval)
            if interval is not None and timeout is not None:
                raise ValueError(self.error_msg)
            if timeout is not None:
                def task_func():
                    self.func(client, *args, **kwargs_)
                client.add_timeout(timeout, task_func)
            else:
                def task_func():
                    self.func(client, *args, **kwargs_)
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

