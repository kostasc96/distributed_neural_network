from google.colab import files
import json
import os

uploaded = files.upload()  # Θα εμφανιστεί επιλογή για ανέβασμα αρχείου


# Όνομα αρχείου (ανέβασε το αρχείο πριν)
filename = list(uploaded.keys())[0] # άλλαξε το όνομα αν είναι διαφορετικό

with open(filename, "r", encoding="utf-8") as file:
  data = json.load(file)  # Φόρτωση JSON δεδομένων

if os.path.exists(filename):  # Ελέγχει αν υπάρχει το αρχείο
  os.remove(filename)

version=data["version"]

list_of_items = []
newData = {}
for item in data:
  if item == "version":
    continue
  newData[item] = { "nodes": [ ] }
  list_of_items.append(item)

#    Σε σχόλιο: Κώδικας για previous_node ή και next_node
#lastItem = None
layerCount = 0;
#lastNodesIDs = []
for item in list_of_items:
  num=len(data[item]["biases"])
#  newNodesIDs = []
  for i in range(0,num):
    id = version+"_"+str(layerCount)+str(i)
#    newData[item]["nodes"].append({"id": id, "weights": data[item]["weights"][i], "biases": data[item]["biases"][i], "activation": data[item]["activation"], "next_nodes": []})
#    newData[item]["nodes"].append({"id": id, "weights": data[item]["weights"][i], "biases": data[item]["biases"][i], "activation": data[item]["activation"], "previous_nodes": lastNodesIDs, "next_nodes": []})
    newData[item]["nodes"].append({"id": id, "weights": data[item]["weights"][i], "biases": data[item]["biases"][i], "activation": data[item]["activation"]})
#    newNodesIDs.append(id)
#  if lastItem != None:
#    oldNum=len(newData[lastItem]["nodes"])
#    for i in range(0, oldNum):
#      newData[lastItem]["nodes"][i]["next_nodes"] = newNodesIDs
#  lastNodesIDs = newNodesIDs
#  lastItem = item
  layerCount+=1

newFileName = "node_based_model.json"
counter=0
while os.path.exists(newFileName):
  newFileName = "node_based_model"+str(counter)+".json"
  counter+=1

with open(newFileName, "w", encoding="utf-8") as file:
  json.dump(newData, file, ensure_ascii=False, indent=4)

files.download(newFileName)
