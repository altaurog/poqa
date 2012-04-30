"""
poqa - python objective queueing assistant

A relatively thin wrapper around pika amqp library
providing declarative-style pythonic interface to
core asyncronous amqp methods, inspired by django's
ORM models.
"""

VERSION = (0,9)
__version__ = '.'.join(map(str, VERSION))
