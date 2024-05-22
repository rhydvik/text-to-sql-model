from flask import Flask

def create_app():
    app = Flask(__name__)
    
    with app.app_context():
        # Import parts of our application
        from .routes import main_routes
        from .models import db
        
        # Register Blueprints
        app.register_blueprint(main_routes.main_bp)
        
        # Initialize our database
        db.init_app(app)
        
    return app
