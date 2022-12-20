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
	field_recordings = glob('./*.WAV')	
	scores, preds, unsafe = model.predict(
		field_recordings, 
		binary_preds = 'single_target', 
		overlap_fraction = 0.5, 
		batch_size =  64, 
		num_workers = 12)
	scores.to_csv("scores.csv")
	preds.to_csv("preds.csv")
	if len(unsafe) > 0:
		with open('unsafe.txt', 'w') as file:
    		for item in unsafe:
        		file.write("%s\n" % item)

    print('Done')
	os.chdir('../..')
	print(folder, ' done: ', datetime.now())
	print()
	print()