from .base import SerializerBase

class Serializers(dict):
    def __init__(self, *serializers):
        for s in serializers:
            self.append(s)

    def append(self, serializer):
        if issubclass(serializer, SerializerBase):
            serializer = serializer()
        if isinstance(serializer, SerializerBase):
            self[serializer.content_type] = serializer

    def deserialize(self, props, body):
        serializer = self.get(props.content_type)
        if serializer:
            return serializer.loads(body)
        else:
            return body

