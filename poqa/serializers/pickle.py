from .base import SerializerBase, SerializationError

try:
    import cPickle as pickle
except ImportError:
    import pickle

class PickleSerializer(SerializerBase):
    content_type = 'application/x-python.pickle'
    content_encoding = 'binary'

    def dumps(self, obj):
        try:
            return pickle.dumps(obj)
        except (TypeError, pickle.PicklingError), e:
            raise SerializationError(e)

    def loads(self, pickle_str):
        try:
            return pickle.loads(pickle_str)
        except (TypeError, AttributeError, ImportError, IndexError, pickle.UnpicklingError):
            return None

