import pandas as pd
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.statespace.sarimax import SARIMAX
import math
import warnings
import pickle
warnings.filterwarnings("ignore")

def train_and_save():
    traffic_2018 = pd.read_csv('../basedata/Traffics/trafficindex2018.csv')
    traffic_2018['datetime'] = pd.to_datetime(traffic_2018['datetime'] + ' +07:00', format='%Y-%m-%dT%H:%M %z')
    traffic_2019 = pd.read_csv('../basedata/Traffics/trafficindex2019.csv')
    traffic_2019['datetime'] = pd.to_datetime(traffic_2019['datetime'] + ' +07:00', format='%Y-%m-%dT%H:%M %z')
    traffic_2020 = pd.read_csv('../basedata/Traffics/trafficindex2020.csv')
    traffic_2020['datetime'] = pd.to_datetime(traffic_2020['datetime'] + ' +07:00', format='%Y-%m-%dT%H:%M %z')
    traffic_2021 = pd.read_csv('../basedata/Traffics/trafficindex2021.csv')
    traffic_2021['datetime'] = pd.to_datetime(traffic_2021['datetime'] + ' +07:00', format='%Y-%m-%dT%H:%M %z')

    all_traffic = pd.concat([traffic_2018, traffic_2019, traffic_2020, traffic_2021])
    all_traffic['datetime'] = all_traffic['datetime'].dt.round('H')
    all_traffic = all_traffic.groupby('datetime').mean()[['index']]
    all_traffic.reset_index(inplace=True)
    all_traffic['hour'] = all_traffic['datetime'].dt.hour
    all_traffic['dayofweek'] = all_traffic['datetime'].dt.dayofweek
    all_traffic['year'] = all_traffic['datetime'].dt.year
    dummies_hour = pd.get_dummies(all_traffic['hour'], prefix='hour')
    # dummies_year = pd.get_dummies(all_traffic['year'], prefix='year')
    dummies_dayofweek = pd.get_dummies(all_traffic['dayofweek'], prefix='dayofweek')
    all_traffic = pd.concat([all_traffic, dummies_hour, dummies_dayofweek], axis=1)
    # all_traffic = pd.concat([all_traffic, dummies_hour, dummies_year, dummies_dayofweek], axis=1)
    all_traffic.drop(columns=['hour', 'dayofweek', 'year'], inplace=True)
    train_size = 24454
    train = all_traffic[:train_size]
    test = all_traffic[train_size:]

    min_rmse = 200
    order = (2, 1, 1)
    best_s = 0
    for s in [3, 4, 12]:
        ss_order = (1, 0, 1, s)
        mod = SARIMAX(train['index'],
        exog=train.drop(columns=['index', 'datetime']),
        order=order,
        seasonal_order=ss_order,
        enforce_stationarity=False,
        enforce_invertibility=False)
        results = mod.fit()
        pred = results.get_prediction(start=test.index[0], end=test.index[-1], exog=test.drop(columns=['index', 'datetime']), dynamic=False)
        rmse = math.sqrt(mean_squared_error(test['index'], pred.predicted_mean))
        if rmse < min_rmse:
            min_rmse = rmse
            best_s = s

        print(s, rmse)
    pred = results.get_prediction(start=test.index[0], end=test.index[-1], exog=test.drop(columns=['index', 'datetime']), dynamic=False)
    rmse = math.sqrt(mean_squared_error(test['index'], pred.predicted_mean))

    print('min_rmse :', min_rmse)
    print('best_s :', best_s)
    order = (0, 1, 1)
    ss_order = (1, 0, 1, best_s)
    ss_order = (1, 0, 1, s)
    mod = SARIMAX(all_traffic['index'],
    exog=all_traffic.drop(columns=['index', 'datetime']),
    order=order,
    seasonal_order=ss_order,
    enforce_stationarity=False,
    enforce_invertibility=False)
    final_model = mod.fit()

    model_file = open('sarimax.model', 'wb')
    pickle.dump(final_model, model_file)
    model_file.close()


    
try:
    model_file = open('sarimax.model', 'rb')
    loaded = pickle.load(model_file)
    model_file.close()
except:
    train_and_save()
