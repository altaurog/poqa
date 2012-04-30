from .base import SerializerBase

class Serializers(dict):
    def __init__(self, *serializers):
        for s in serializers:
            if issubclass(s, SerializerBase):
                s = s()
            self[s.content_type] = s

    def deserialize(self, props, body):
        serializer = self.get(props.content_type)
        if serializer:
            return serializer.loads(body)
        else:
            return body

