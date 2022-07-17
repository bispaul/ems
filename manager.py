from . import manager
from models import db

@manager.command
def init_db():
    # Create the User Management Table if they do not exist
    db.create_all()