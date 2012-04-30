from __future__ import absolute_import
# otherwise 'yaml' gets confused with present module, apparently

import warnings
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

try:
    from django.core import serializers
    from django.db.models import Model
    from django.db.models.query import QuerySet
except ImportError:
    orm_serialize = None
    orm_deserialize = None
else:
    orm_serialize = serializers.get_serializer('yaml')().serialize
    orm_deserialize = serializers.get_serializer('python')().deserialize

from .base import SerializerBase, SerializationError

class YamlSerializer(SerializerBase):
    content_type = 'application/x-yaml'
    content_encoding = 'utf-8'

    def __init__(self, django_orm=False):
        if django_orm and not orm_serialize:
            warnings.warn("Django ORM serialization not available", RuntimeWarning)
            self.django = False
        else:
            self.django = django_orm
            
    def dumps(self, obj):
        if self.django:
            if isinstance(obj, QuerySet):
                return orm_serialize('yaml', obj)
            if isinstance(obj, Model):
                return orm_serialize('yaml', (obj,))
            try:
                i = tuple(obj)
                if isinstance(i[0], Model):
                    return orm_serialize('yaml', i)
            except TypeError:
                pass
        try:
            return yaml.dump(obj, Dumper=Dumper)
        except Exception, e:
            raise SerializationError(e)


    def loads(self, yaml_str):
        try:
            py_obj = yaml.load(yaml_str)
        except yaml.YAMLError, e:
            raise SerializationError(e)

        if self.django:
            try:
                model_instances = orm_deserialize('python', py_obj)
                return model_instances
            except (KeyError, TypeError):
                pass
        return py_obj

