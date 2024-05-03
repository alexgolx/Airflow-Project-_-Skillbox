# <YOUR_IMPORTS>
import logging
from datetime import datetime

import dill
import pandas as pd
import os
from glob import glob
import json
from pydantic import BaseModel


#path = os.environ.get('PROJECT_PATH', '..')


def predict():
    #path = os.path.expanduser(os.environ.get('PROJECT_PATH', '~/airflow_hw'))
    #path = os.environ.get('PROJECT_PATH', '..')
    full_path = f'{path}/data/models/cars_pipe_*.pkl'
    latest_model = max(glob(full_path), key=os.path.getctime)
    test_path = f'{path}/data/test'

    with open(latest_model, 'rb') as f:
        model = dill.load(f)

    predictions = []
    json_files = glob(os.path.join(test_path, "*.json"))
    for file in json_files:
        with open(file, "r") as f:
            # Load JSON data from the file
            json_data = json.load(f)
            # Convert JSON data to DataFrame
            df = pd.DataFrame([json_data])
            predicting = model.predict(df)
            predictions.append({'id': df["id"].iloc[0], 'prediction': predicting[0]})
            logging.info(f'id : {df["id"].iloc[0]}, prediction : {predicting[0]}')

    predictions_df = pd.DataFrame(predictions)

    print(predictions_df)


    output_path = f'{path}/data/predictions'
    csv_file_path = os.path.join(output_path, f'predictions_{datetime.now().strftime("%Y%m%d%H%M")}.csv')
    predictions_df.to_csv(csv_file_path, index = False)


if __name__ == '__main__':
    predict()
