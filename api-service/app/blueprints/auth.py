from flask import Blueprint, request, jsonify
from flask_bcrypt import Bcrypt
import jwt
import datetime
from ..db import get_db_connection
from ..config import Config

auth_bp = Blueprint('auth', __name__)
bcrypt = Bcrypt()

@auth_bp.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username, email, password = data.get('username'), data.get('email'), data.get('password')
    role = data.get('role', 'user') # Default to user

    if not all([username, email, password]):
        return jsonify({"message": "Missing fields"}), 400

    hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
    
    conn = get_db_connection()
    if not conn: return jsonify({"message": "DB Connection Error"}), 500
    
    try:
        cur = conn.cursor()
        # Change the INSERT line to this:
        cur.execute(
            "INSERT INTO public.app_users (username, email, password_hash, role) VALUES (%s, %s, %s, %s)",
            (username, email, hashed_pw, role)
        )
        conn.commit()
        return jsonify({"message": "User created"}), 201
    except Exception as e:
        return jsonify({"message": "User already exists or DB error"}), 409
    finally:
        conn.close()

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    email, password = data.get('email'), data.get('password')

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM app_users WHERE email = %s", (email,))
    user = cur.fetchone()
    conn.close()

    if user and bcrypt.check_password_hash(user['password_hash'], password):
        token = jwt.encode({
            'user_id': user['user_id'],
            'email': user['email'],
            'role': user['role'],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, Config.SECRET_KEY, algorithm="HS256")
        
        return jsonify({"token": token, "role": user['role'], "username": user['username']}), 200
    
    return jsonify({"message": "Invalid credentials"}), 401