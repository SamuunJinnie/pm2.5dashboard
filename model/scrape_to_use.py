from os import listdir
from os.path import join
import pandas as pd

data_base_path = 'prepared_data/station'
to_use_base_path = 'prepared_data/to_uses'
for station in listdir(data_base_path):
    df = pd.read_csv(join(data_base_path, station))
    to_use = pd.read_csv(join(to_use_base_path, station))
