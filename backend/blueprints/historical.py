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
    query += "ORDER BY date, time ASC;"
    cur = conn.cursor()
    cur.execute(query, tuple(params))
    # print(params)
    # print(cur.query)
    rows = cur.fetchall()
    # print(rows)
    historical_data = []
    for row in rows:
        historical_data.append({
            'avghumidity': row[15],
            'avgtemp_c': row[13],
            'cloud': row[8],
            'condition_code': row[3],
            'condition_icon': row[4],
            'condition_text': row[7],
            'country': row[0],
            'daily_chance_of_rain': row[9],
            'daily_chance_of_snow': row[10],
            'daily_will_it_rain': row[11],
            'daily_will_it_snow': row[12],
            'date': row[2],
            'date_epoch': row[28],
            'feelslike_c': row[14],
            'hour_uv': row[10],
            'humidity': row[16],
            'is_day': row[6],
            'lat': row[4],
            'localtime': row[18],
            'localtime_epoch': row[19],
            'lon': row[5],
            'maxtemp_c': row[15],
            'maxwind_kph': row[17],
            'mintemp_c': row[16],
            'name': row[1],
            'precip_in': row[25],
            'precip_mm': row[11],
            'pressure_in': row[26],
            'pressure_mb': row[27],
            'temp_c': row[12],
            'time': row[3],
            'season': row[29],
            'uv': row[9],
            'wind_degree': row[20],
            'wind_kph': row[18],
        })

    cur.close()

    return jsonify(historical_data)
