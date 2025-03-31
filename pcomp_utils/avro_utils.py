from io import BytesIO
from fastavro import schemaless_writer, schemaless_reader
from pcomp.avro_layer_schema import layer_message_schema
from pcomp.avro_batch_schema import batch_message_schema

def avro_serialize(message_dict, schema=layer_message_schema):
    bytes_writer = BytesIO()
    schemaless_writer(bytes_writer, schema, message_dict)
    return bytes_writer.getvalue()

def avro_deserialize(bytes_data, schema=layer_message_schema):
    bytes_reader = BytesIO(bytes_data)
    return schemaless_reader(bytes_reader, schema)

def avro_serialize_batch(message_dict, schema=batch_message_schema):
    bytes_writer = BytesIO()
    schemaless_writer(bytes_writer, schema, message_dict)
    return bytes_writer.getvalue()

def avro_deserialize_batch(bytes_data, schema=batch_message_schema):
    bytes_reader = BytesIO(bytes_data)
    return schemaless_reader(bytes_reader, schema)
