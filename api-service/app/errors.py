from flask import jsonify

def handle_error(e):
    message = str(e)
    status_code = 500
    return jsonify({"error": message}), status_code