import glob
import json
import os
from typing import Dict
from dataset.feature.feature import *
from dataset import const

path = 'data/feature_selection.json'
with open(path, 'r') as f:
    feature_selection = json.load(f)

FEATURE_DEF_PATH = 'data/feature_definition'
new_feature_def = dict()
for file in glob.glob(f'{FEATURE_DEF_PATH}/*.json'):
    with open(file, 'r') as f:
        feature_def = json.load(f)
    new_feature_def = {
        feature_name: feature 
        for feature_name, feature in feature_def.items() 
        if feature_name in feature_selection}

    file = file.replace('feature_definition', 'feature_definition_new')
    with open(file, 'w') as f:
        json.dump(new_feature_def, f)
