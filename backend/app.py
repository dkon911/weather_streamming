from flask import Flask
from flask_cors import CORS

from blueprints.historical import historical_blueprint
from blueprints.prediction import prediction_blueprint
from blueprints.weather import weather_blueprint

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})

# Register the blueprints
app.register_blueprint(weather_blueprint)
app.register_blueprint(prediction_blueprint)
app.register_blueprint(historical_blueprint)

if __name__ == '__main__':
    app.run(debug=True)
