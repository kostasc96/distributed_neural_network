import json

# Load the original model
with open('node_based_model.json', 'r') as f:
    model = json.load(f)

# Determine which layer is the final one
layer_ids = list(model.keys())
final_layer_id = layer_ids[-1]

neurons = []

for layer_id, layer_content in model.items():
    # Extract numeric index from layer_id, e.g. 'layer_0' → 0
    layer_num = int(layer_id.split('_')[1])
    is_final = 1 if layer_id == final_layer_id else 0

    # Reset per-layer neuron counter
    neuron_counter = 0

    for node in layer_content.get('nodes', []):
        neurons.append({
            "layer_id": layer_id,
            "layer_id_num": str(layer_num),
            "is_final_layer": str(is_final),
            "neuron_id": str(neuron_counter),
            "weights": str(node.get("weights")),
            "bias": str(node.get("biases")),
            "activation": node.get("activation")
        })
        neuron_counter += 1

# Write out the new JSON
with open('neurons.json', 'w') as f:
    json.dump(neurons, f, indent=4)

print(f"Extracted {len(neurons)} neurons into neurons.json")
