from pika import spec

class SerializationError(Exception):
    "Unable to serialize a value"

class SerializerBase(object):
    "Base class for serializers"

    def serialize(self, body, kwargs):
        body = self.dumps(body)
        props = kwargs.pop('properties', None)
        if props is None:
            props = spec.BasicProperties()
        props.content_type = self.content_type
        props.content_encoding = self.content_encoding
        kwargs['properties'] = props
        return body

    def deserialize(self, props, body):
        if self.content_type == props.content_type:
            return self.loads(body)
        else:
            return body
