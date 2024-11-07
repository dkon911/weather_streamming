from flask import Blueprint, request, jsonify
from backend.config import get_postgres_connection

historical_blueprint = Blueprint('historical', __name__)
conn = get_postgres_connection()

@historical_blueprint.route('/historical_weather', methods=['GET'])
def get_historical_weather():
    city = request.args.get('city')
    country = request.args.get('country')
    date = request.args.get('date')

    query = """
        SELECT * FROM historical_weather_
        WHERE 1=1
    """

    params = []
    if city:
        query += " AND name LIKE %s"
        params.append(city)
    if country:
        query += " AND country LIKE %s"
        params.append(country)
    if date:
        query += " AND date = %s"
        params.append(date)

    cur = conn.cursor()
    cur.execute(query, tuple(params))
    # print(params)
    # print(cur.query)
    rows = cur.fetchall()
    print(rows)
    historical_data = []
    for row in rows:
        historical_data.append({
            'avghumidity': row[0],
            'avgtemp_c': row[1],
            'cloud': row[2],
            'condition_code': row[3],
            'condition_icon': row[4],
            'condition_text': row[5],
            'country': row[6],
            'daily_chance_of_rain': row[7],
            'daily_chance_of_snow': row[8],
            'daily_will_it_rain': row[9],
            'daily_will_it_snow': row[10],
            'date': row[11],
            'date_epoch': row[12],
            'feelslike_c': row[13],
            'hour_uv': row[14],
            'humidity': row[15],
            'is_day': row[16],
            'lat': row[17],
            'localtime': row[18],
            'localtime_epoch': row[19],
            'lon': row[20],
            'maxtemp_c': row[21],
            'maxwind_kph': row[22],
            'mintemp_c': row[23],
            'name': row[24],
            'precip_in': row[25],
            'pressure_mm': row[26],
            'pressure_in': row[27],
            'pressure_mb': row[28],
            'temp_c': row[29],  # Corrected index
            'time': row[30],    # Corrected index
            'totalprecip_mm': row[31],  # Corrected index
            'uv': row[32],      # Corrected index
            'wind_degree': row[33],  # Corrected index
            'wind_kph': row[34],  # Corrected index
        })

    cur.close()

    return jsonify(historical_data)