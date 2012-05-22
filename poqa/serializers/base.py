import haigha.message

class Message(object):
    def __init__(self, payload, routing_key=None, delivery_info=None, **properties):
        self.payload = payload
        self.routing_key = routing_key
        self.properties = properties
        self.delivery_info = delivery_info

class SerializationError(Exception):
    "Unable to serialize a value"

class SerializerBase(object):
    "Base class for serializers"

    def serialize(self, message):
        body = self.dumps(message.payload)
        properties = message.properties
        properties['content_type'] = self.content_type
        properties['content_encoding'] = self.content_encoding
        return haigha.message.Message(body, **properties)

    def deserialize(self, message):
        content_type = message.properties.get('content_type')
        if self.content_type == content_type:
            payload = self.loads(message.body)
            return Message(payload,
                        delivery_info=message.delivery_info,
                        **message.properties)
        else:
            return message

class Serializers(dict):
    default_encoding = 'utf-8'

    def __init__(self, *serializers):
        for s in serializers:
            self.append(s)

    def append(self, serializer):
        if issubclass(serializer, SerializerBase):
            serializer = serializer()
        if isinstance(serializer, SerializerBase):
            self[serializer.content_type] = serializer

    def deserialize(self, message):
        properties = message.properties
        content_type = properties.get('content_type')
        serializer = self.get(content_type)
        if serializer:
            return serializer.deserialize(message)
        else:
            encoding = properties.get('content_encoding',
                                        self.default_encoding)
            message.payload = message.body.decode(encoding)
            return message

