
class SerializationError(Exception):
    "Unable to serialize a value"

class SerializerBase(object):
    "Base class for serializers"

    def serialize(self, body, properties):
        body = self.dumps(body)
        properties.content_type = self.content_type
        properties.content_encoding = self.content_encoding
        return body

    def deserialize(self, props, body):
        if self.content_type == props.content_type:
            return self.loads(body)
        else:
            return body

