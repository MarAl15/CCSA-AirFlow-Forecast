"""
    Microservice to predict the humidity and temperature using ARIMA.

    @author: Mar Alguacil
"""
import json
import pickle
import pandas as pd
import pmdarima
from pathlib import Path
from datetime import datetime
from flask import Flask, Response
app = Flask(__name__)

# Get ARIMA models from the pickle files
path = str(Path.home())+'/.models/'
file_hum = open(path+'arima_humidity.p', 'rb')
model_hum  = pickle.load(file_hum)
file_hum.close()

file_temp = open(path+'arima_temperature.p', 'rb')
model_temp = pickle.load(file_temp)
file_temp.close()

# Define routes
@app.route("/")
def welcome():
    """
        Displays the welcome message.
    """
    return "<h1> PREDICCIÓN CON ARIMA </h1> \
            ¡Bienvenido al sistema de predicción de la humedad y la temperatura para las proximas \
            <a href='/servicio/v1/prediccion/24horas'>24</a>, \
            <a href='/servicio/v1/prediccion/48horas'>48</a> y \
            <a href='/servicio/v1/prediccion/72horas'>72</a> horas!"

@app.route("/servicio/v1/prediccion/<int:interval>horas", methods=['GET'])
def forecast(interval):
    """
        Predicts temperature and humidity for the next 24, 48 or 72 hours.
    """
    if interval not in [24, 48, 72]:
        return Response("Lo siento, sólo trabajamos con predicciones para las próximas 24, 48 y 72 horas.",
                        status=400)

    # Predict temperature and humidity from ARIMA models
    forecast_temp = model_temp.predict(n_periods=interval)
    forecast_hum  = model_hum.predict(n_periods=interval)

    # Create a list with the next 'interval' hours
    initial_hour  = (int(datetime.now().strftime('%H')) + 1)%24
    timestamps = pd.date_range(str(initial_hour)+':00', periods=interval, freq='60min').strftime('%d/%m/%Y %H:%M')


    return  Response(json.dumps([{'hour': date,
                                  'temp': round(temperature,2),
                                  'hum' : round(humidity,2)
                                 }
                                 for date, temperature, humidity
                                     in zip(timestamps, forecast_temp, forecast_hum)]),
                     status=200, mimetype='application/json')


if __name__ == "__main__":
    app.run()
