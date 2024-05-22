# This is where you'd set up your database models using an ORM like SQLAlchemy
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class ExampleModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    table_name = db.Column(db.String(50), nullable=False)
    columns = db.Column(db.String(200), nullable=False)
    database_type = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    
