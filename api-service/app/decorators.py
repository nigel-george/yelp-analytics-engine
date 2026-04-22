import jwt
from functools import wraps
from flask import request, jsonify, g
from .config import Config

def jwt_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token or "Bearer " not in token:
            return jsonify({'message': 'Authorization token is missing!'}), 401
        
        try:
            token_clean = token.split(" ")[1]
            data = jwt.decode(token_clean, Config.SECRET_KEY, algorithms=["HS256"])
            g.current_user = data
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired!'}), 401
        except Exception:
            return jsonify({'message': 'Token is invalid!'}), 401
            
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not hasattr(g, 'current_user') or g.current_user.get('role') != 'admin':
            return jsonify({'message': 'Forbidden: Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated