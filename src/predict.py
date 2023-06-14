# Dont forget conda activate opensoundscape
# Dont forget to modify file names and glob pattern
# Run script in Pomona-2, hard code trip date in the glob
# python /media/david/USB/Skraak/src/predict.py

from opensoundscape.torch.models.cnn import load_model
import opensoundscape

import torch
from pathlib import Path
import numpy as np
import pandas as pd

from glob import glob
import os
from datetime import datetime

model = load_model('/home/david/best.model')

# folders =  glob('./*/2023-?????/')
folders =  glob('./*/2023-06-10/')
for folder in folders:
    os.chdir(folder)
    print(folder, ' start: ', datetime.now())
    # Beware, secretary island files are .wav
    field_recordings = glob('./*.WAV')	
    scores, preds, unsafe = model.predict(
            field_recordings, 
            binary_preds = 'single_target', 
            overlap_fraction = 0.5, 
            batch_size =  128, 
            num_workers = 12)
    scores.to_csv("scores-2023-06-12.csv")
    preds.to_csv("preds-2023-06-12.csv")
    os.chdir('../..')
    print(folder, ' done: ', datetime.now())
    print()
    print()