from flask import Blueprint, request, jsonify
from ..db import get_db_connection
from ..decorators import jwt_required

businesses_bp = Blueprint('businesses', __name__)

@businesses_bp.route('/search', methods=['GET'])
@jwt_required
def search_businesses():
    city = request.args.get('city')
    min_stars = request.args.get('stars', 0)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    
    query = """
        SELECT business_name, city, total_avg_stars, total_reviews, business_direction
        FROM analytics.mart_business_performance
        WHERE city ILIKE %s AND total_avg_stars >= %s
        ORDER BY total_avg_stars DESC
        LIMIT 50
    """
    cur.execute(query, (f"%{city}%", min_stars))
    results = cur.fetchall()
    
    conn.close()
    return jsonify(results)

@businesses_bp.route('/top', methods=['GET'])
@jwt_required
def get_top_businesses():
    conn = get_db_connection()
    cur = conn.cursor()
    
    
    cur.execute("""
        SELECT business_name, total_avg_stars, elite_review_ratio
        FROM analytics.mart_business_performance
        WHERE total_reviews > 50
        ORDER BY total_avg_stars DESC, elite_review_ratio DESC
        LIMIT 10
    """)
    results = cur.fetchall()
    
    conn.close()
    return jsonify(results)