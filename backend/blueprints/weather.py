from flask import Blueprint, request, jsonify
from backend.config import get_postgres_connection
from backend.utils.postgres_utils import calculate_time_diff

weather_blueprint = Blueprint('weather', __name__)
conn = get_postgres_connection()

@weather_blueprint.route('/current_weather', methods=['GET'])
def get_weather():
    city = request.args.get('city')
    country = request.args.get('country')

    query = f"""
        SELECT * FROM current_weather 
        WHERE last_updated = (SELECT max(last_updated) FROM current_weather WHERE name like '{city}') 
        AND 1=1
    """

    params = []
    if city:
        query += " AND name LIKE %s"
        params.append(city)
    if country:
        query += " AND country LIKE %s"
        params.append(country)

    cur = conn.cursor()
    cur.execute(query, tuple(params))
    print(params)
    print(cur.query)
    rows = cur.fetchall()

    weather_data = []
    for row in rows:
        weather_data.append({
            'name': row[0],
            'region': row[1],
            'country': row[2],
            'lat': row[3],
            'lon': row[4],
            'tz_id': row[5],
            'localtime': row[6],
            "time_diff": calculate_time_diff(row[6]),
            'last_updated': row[8],
            'temp_c': row[9],
            'is_day': row[10],
            'condition_text': row[11],
            'condition_icon': row[12],  # This will return the icon URL as expected
            'condition_code': row[13],
            'wind_kph': row[14],
            'wind_degree': row[15],
            'pressure_mb': row[16],
            'precip_in': row[17],
            'humidity': row[18],
            'cloud': row[19],
            'feelslike_c': row[20],
            'windchill_c': row[21],
            'heatindex_c': row[22],
            'dewpoint_c': row[23],
            'vis_km': row[24],
            'uv': row[25],
            'gust_kph': row[26]
        })

    cur.close()

    return jsonify(weather_data)


