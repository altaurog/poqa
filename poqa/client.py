"""
AMQP client wrapper
"""
from __future__ import absolute_import

import itertools
import weakref

from haigha.connection import Connection

from .declarations import *
from .taskloop import *
from .serializers import Message

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
        if isinstance(value, Decorator):
            value.on_class_create(cls)

class AsyncClient(object):
    __metaclass__ = ClientMeta

    def start(self, **kwargs):
        self.connection = Connection(**kwargs)
        self.channel = self.connection.channel()
        for declaration in itertools.chain(self._exchanges, self._queues, self._consumers):
            declaration.client = weakref.ref(self)
            declaration.declare()
        self.loop = TaskLoop()
        self.insert_task(self.read_frames, interval=0.1)

        # if there's a run function which isn't already a task, queue it
        class_run = getattr(self.__class__, 'run', None)
        self_run = getattr(self, 'run', None)
        if callable(self_run) and not isinstance(class_run, Task):
            self.insert_task(self.run, 0)

        # start 'auto' tasks
        for name, attr in self.__class__.__dict__.iteritems():
            if isinstance(attr, Task) and attr.auto:
                getattr(self, name)()

        # I don't think this does anything
        for task, args, kwargs in self._tasks:
            print 'scheduling %s from _tasks' % task
            self.insert_task(task, *args, **kwargs)

        self.loop.start()

    def stop(self):
        self.connection.close()

    def insert_task(self, task, *args, **kwargs):
        loop = getattr(self, 'loop', None)
        if isinstance(loop, TaskLoop):
            loop.insert_task(task, *args, **kwargs)
        else:
            self._tasks.append((task, args, kwargs))

    def read_frames(self):
        if self.connection.close_info:
            self.loop.stop()
        else:
            self.connection.read_frames()

    @task
    def basic_qos(self, **kwargs):
        self.channel.basic.qos(**kwargs)

