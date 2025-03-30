layer_message_schema = {
    "type": "record",
    "name": "LayerMessage",
    "fields": [
        {"name": "layer_id", "type": "string"},
        {"name": "image_id", "type": "int"},
        {"name": "data", "type": "bytes"}
    ]
}
