import pickle
import json

def save_obj_pickle(obj, filepath):
    with open(filepath, "wb+") as f:
        pickle.dump(obj, f)

def load_obj_pickle(filepath):
    with open(filepath, "rb") as f:
        obj = pickle.load(f)
    return obj

def save_obj_json(obj, filepath):
    with open(filepath, "w") as f:
        json.dump(obj, f)

def load_obj_json(filepath):
    with open(filepath, "r") as f:
        obj = json.load(f)
    return obj

