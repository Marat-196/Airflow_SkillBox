import dill
import json
import os

import pandas as pds

path = os.environ.get('PROJECT_PATH', '/home/airflow/airflow_hw')
models = path + '/data/models'


# def last_model():
#     dates = []
#     for files in os.listdir(models):
#         dates = dates + [files.split('_')[-1].split('.')[0]]
#     return os.listdir(models)[dates.index(max(dates))]


def predict():
    val = os.listdir(models)
    model_filename = models + '/' + val[-1]
    with open(model_filename, 'rb') as file:
        model = dill.load(file)
    df_pred = pds.DataFrame(columns=["car_id", "pred"])
    for file_json in os.listdir(f'{path}/data/test'):
        with open(f'{path}/data/test/{file_json}', 'rb') as fin:
            form = json.load(fin)
            df = pds.DataFrame.from_dict([form])
            y = model.predict(df)
            df_pred.loc[len(df_pred.index)] = [form["id"], y[0]]
    df_pred.to_csv(f'{path}/data/predictions/car_price_prediction.csv', encoding='utf-8',
                   index=False)


if __name__ == '__main__':
    predict()
