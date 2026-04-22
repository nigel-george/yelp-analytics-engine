from flask import Flask
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app)

    
    from .blueprints.auth import auth_bp
    from .blueprints.admin import admin_bp
    from .blueprints.analytics import analytics_bp
    from .blueprints.businesses import businesses_bp

    # Registering the prefixes
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(analytics_bp, url_prefix='/analytics')
    app.register_blueprint(businesses_bp, url_prefix='/businesses')

    return app