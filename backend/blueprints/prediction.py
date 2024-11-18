from flask import Blueprint, request, jsonify
import pandas as pd
from backend.models.model import model
from backend.utils.cassandra_utils import get_yesterdays_weather_data

prediction_blueprint = Blueprint('prediction', __name__)


@prediction_blueprint.route('/predict_today', methods=['GET'])
def predict_for_tomorrow():
    city = request.args.get('city')
    country = request.args.get('country', 'Vietnam')

    yesterdays_data = get_yesterdays_weather_data(city, country)
    if not yesterdays_data:
        return jsonify({"error": "No data available for prediction"}), 404

    # Create DataFrame from yesterdays_data
    features = pd.DataFrame(yesterdays_data)

    # Use the model to predict future weather
    predictions = model.predict(features)

    predictions_list = [
        {'temperature': pred[0], 'humidity': pred[1], 'wind_speed': pred[2]}
        for pred in predictions
    ]
    return jsonify({'predictions': predictions_list})


