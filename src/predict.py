# Run script in Pomona-2, hard code trip date in the glob
# python ~/.julia/dev/Skraak/src/predict.py

from opensoundscape.torch.models.cnn import load_model
import opensoundscape

import torch
from pathlib import Path
import numpy as np
import pandas as pd

from glob import glob
import os
from datetime import datetime

model = load_model('/media/david/72CADE2ECADDEDFB/Pomona/TrainingSets/K-Set/K-Set_Models/2022-12-13/binary_train/best.model')

folders =  glob('./*/2022-12-17/')
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
    scores.to_csv("scores-2022-12-21.csv")
    preds.to_csv("preds-2022-12-21.csv")
    os.chdir('../..')
    print(folder, ' done: ', datetime.now())
    print()
    print()