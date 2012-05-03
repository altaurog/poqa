"""
AMQP client wrapper
"""
from __future__ import absolute_import

import itertools
import weakref

from pika.adapters import SelectConnection
from pika.connection import ConnectionParameters
from pika.spec import BasicProperties as Properties

from .declarations import *

class ClientMeta(type):
    def __new__(cls, name, bases, attrs):
        """
        Create a new class, with special handling for amqp objects
        inspired by django models
        """
        super_new = super(ClientMeta, cls).__new__

        # Create the class.
        module = attrs.pop('__module__')
        base_attrs = {'__module__': module,
                        '_exchanges':[],
                        '_queues':[],
                        '_consumers':[],
                        '_tasks':[],
                        }
        new_class = super_new(cls, name, bases, base_attrs)

        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        return new_class

    def add_to_class(cls, name, value):
        setattr(cls, name, value)
        if isinstance(value, Declaration):
            value.name = name
            if isinstance(value, Exchange):
                cls._exchanges.append(value)
            elif isinstance(value, Queue):
                cls._queues.append(value)
            elif isinstance(value, Consumer):
                cls._consumers.append(value)
                setattr(cls, name, value.handler)

class AsyncClient(object):
    __metaclass__ = ClientMeta

    def _on_connected(self, connection):
        connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self.channel = channel
        self._declare_topology()

    def _declare_topology(self):
        try:
            decl_iter = self._decl_iter
        except AttributeError:
            decl_iter = itertools.chain(self._exchanges, self._queues)
            self._decl_iter = decl_iter

        try:
            declaration = decl_iter.next()
        except StopIteration:
            self._declare_consumers()
        else:
            declaration.client = weakref.proxy(self)
            declaration.declare(self._declare_topology)

    def _declare_consumers(self):
        for c in self._consumers:
            c.handler = getattr(self, c.name)
            c.declare(self.channel)
        self._start_tasks()

    def _start_tasks(self):
        for deadline, handler in getattr(self, '_pending_timeouts', []):
            self.connection.add_timeout(deadline, handler)

    def start(self, host='127.0.0.1'):
        connection_params = ConnectionParameters(host)
        self.connection = SelectConnection(connection_params, self._on_connected)

        try:
            print "starting io loop"
            self.connection.ioloop.start()
        except KeyboardInterrupt:
            self.connection.close()

            # Loop until the connection is closed
            self.connection.ioloop.start()

    def add_timeout(self, timeout, handler):
        connection = getattr(self, 'connection', None)
        pending = getattr(self, '_pending_timeouts', [])
        if connection is None:
            pending.append((timeout, handler))
            self._pending_timeouts = pending
        else:
            self.connection.add_timeout(timeout, handler)

    @task
    def basic_qos(self, **kwargs):
        self.channel.basic_qos(**kwargs)

