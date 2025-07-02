import os
import json
from pathlib import Path
from string import Template

# Διαδρομή του αρχικού yml αρχείου (πρότυπο)
input_file = "neuron_function.yml"

# Ανάγνωση περιεχομένου από το template
with open(input_file, "r", encoding="utf-8") as file:
    content = file.read()

# Δημιουργία του φακέλου αν δεν υπάρχει ήδη
folder_path = "neuronYmlFolder"
os.makedirs(folder_path, exist_ok=True)


# Ανάγνωση neurons από το json
with open("neurons.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# Δημιουργία αρχείων .yml για κάθε neuron
for params_json in data:
    
    function_id = f"neuron-{str(params_json.get('layer_id_num', 'unknown'))}-{str(params_json.get('neuron_id', 'unknown'))}"
    mock_topic = None
    autoscaler_id = None
    if int(params_json.get('layer_id_num')) == 0:
        mock_topic = "layer-0-mock"
        autoscaler_id = "autoscale-layer-0"
    else:
        mock_topic = "layer-1-mock"
        autoscaler_id = "autoscale-layer-1"
    # Αντικατάσταση μεταβλητών
    template = Template(content)
    newContent = template.substitute(FUNCTION_ID=function_id, PARAMS_JSON=json.dumps(params_json, indent=2), MOCK_TOPIC=mock_topic, AUTOSCALER_ID=autoscaler_id)
    output_file = function_id+".yml"
    # Αποθήκευση του νέου αρχείου
    with open((Path(folder_path) / output_file), "w", encoding="utf-8") as file:
        file.write(newContent)

print(f"Τα αρχεία δημιουργήθηκαν")