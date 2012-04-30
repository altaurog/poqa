import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

try:
    import cPickle as pickle
except ImportError:
    import pickle

from django.core import serializers
from django.db.models import Model
from django.db.models.query import QuerySet

orm_serialize = serializers.get_serializer('yaml')().serialize
orm_deserialize = serializers.get_serializer('python')().deserialize

def to_yaml(obj):
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
    return yaml.dump(obj, Dumper=Dumper)

def from_yaml(yaml_str):
    try:
        py_obj = yaml.load(yaml_str)
    except yaml.YAMLError:
        return None

    try:
        model_instances = orm_deserialize('python', py_obj)
        return model_instances
    except (KeyError, TypeError):
        return py_obj

def to_pickle(obj):
    try:
        return pickle.dumps(obj)
    except (TypeError, pickle.PicklingError):
        return None

def from_pickle(pickle_str):
    try:
        return pickle.loads(pickle_str)
    except (TypeError, AttributeError, ImportError, IndexError, pickle.UnpicklingError):
        return None

