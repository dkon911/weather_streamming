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
            'country': row[0],
            'name': row[1],
            'date': row[2],
            'time': row[3],
            'lat': row[4],
            'lon': row[5],
            'is_day': row[6],
            'condition_text': row[7],
            'cloud': row[8],
            'uv': row[9],
            'hour_uv': row[10],
            'precip_mm': row[11],
            'temp_c': row[12],
            'feelslike_c': row[13],
            'maxtemp_c': row[14],
            'mintemp_c': row[15],
            'avgtemp_c': row[16],
            'humidity': row[17],
            'maxwind_kph': row[18],
            'wind_kph': row[19],
            'wind_degree': row[20],
            'day_of_week': row[21],
            'season': row[29],
        })

    cur.close()

    return jsonify(historical_data)
