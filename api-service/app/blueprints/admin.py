from flask import Blueprint, jsonify
from ..decorators import jwt_required, admin_required
from ..db import get_db_connection

admin_bp = Blueprint('admin', __name__)

@admin_bp.route('/health', methods=['GET'])
@jwt_required
@admin_required
def system_health():
    conn = get_db_connection()
    cur = conn.cursor()
    
    
    cur.execute("SELECT COUNT(*) FROM app_users")
    user_count = cur.fetchone()['count']
    
    
    cur.execute("SELECT MAX(review_date) FROM analytics.stg_review")
    last_data = cur.fetchone()['max']
    
    conn.close()
    return jsonify({
        "status": "Healthy",
        "user_count": user_count,
        "data_freshness": str(last_data),
        "database": "PostgreSQL (Docker)"
    }), 200