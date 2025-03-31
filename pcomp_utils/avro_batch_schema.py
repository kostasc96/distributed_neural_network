batch_message_schema = {
    "type": "record",
    "name": "BatchMessage",
    "fields": [
        {"name": "neuron_id", "type": "int"},
        {"name": "batch_id", "type": "int"},
        {"name": "batch_size", "type": "int"},
        {"name": "columns_size", "type": "int"},
        {"name": "data", "type": "bytes"}
    ]
}
