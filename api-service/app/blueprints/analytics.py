from flask import Blueprint, jsonify
from ..decorators import jwt_required
from ..db import get_db_connection

analytics_bp = Blueprint('analytics', __name__)

@analytics_bp.route('/cities', methods=['GET'])
@jwt_required
def get_cities():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT city FROM analytics.mart_city_summary ORDER BY city")
    cities = [row['city'] for row in cur.fetchall()]
    conn.close()
    return jsonify(cities)

@analytics_bp.route('/city/<city_name>', methods=['GET'])
@jwt_required
def get_city_metrics(city_name):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT review_month, monthly_reviews, review_growth_rate_pct 
        FROM analytics.mart_city_summary 
        WHERE city = %s 
        ORDER BY review_month
    """, (city_name,))
    data = cur.fetchall()
    conn.close()
    return jsonify(data)