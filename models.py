from flask_sqlalchemy import SQLAlchemy
from flask_user import UserMixin
from flask_user.forms import RegisterForm
from flask_wtf import Form
from wtforms import StringField, SubmitField, validators


db = SQLAlchemy()


def init_app(app):
    db.init_app(app)


# Define the User data model. Make sure to add flask.ext.user UserMixin!!
# Define a base model for other database tables to inherit
class Base(db.Model):
    __abstract__ = True

    id = db.Column(db.Integer(), primary_key=True)
    date_created = db.Column(db.DateTime, default=db.func.current_timestamp())
    date_modified = db.Column(db.DateTime, default=db.func.current_timestamp(),
                              onupdate=db.func.current_timestamp())


class User(Base, UserMixin):
    __tablename__ = 'users_flask_user_mod'
    # id = db.Column(db.Integer, primary_key=True)

    # User authentication information
    username = db.Column(db.String(50), nullable=False, unique=True)
    password = db.Column(db.String(255), nullable=False, server_default='')
    reset_password_token = db.Column(db.String(100), nullable=False,
                                     server_default='')

    # User email information
    email = db.Column(db.String(255), nullable=False, unique=True)
    confirmed_at = db.Column(db.DateTime())

    # User information
    active = db.Column('is_active', db.Boolean(), nullable=False,
                       server_default='0')
    first_name = db.Column(db.String(100), nullable=False, server_default='')
    last_name = db.Column(db.String(100), nullable=False, server_default='')

    last_login_at = db.Column(db.DateTime())
    current_login_at = db.Column(db.DateTime())
    # Why 45 characters for IP Address ?
    # See http://stackoverflow.com/questions/166132/
    # maximum-length-of-the-textual-representation-of-an-ipv6-address/166157#166157
    last_login_ip = db.Column(db.String(45))
    current_login_ip = db.Column(db.String(45))
    login_count = db.Column(db.Integer)
    # organisation from organisation_master
    # organisation_master_fk = db.Column(db.Integer(),
    # db.ForeignKey('organisation_master.organisation_master_pk'))
    organisation_master_fk = db.Column(db.Integer())
    # Relationships
    roles = db.relationship('Role', secondary='users_roles_flask_user_mod',
                            backref=db.backref('users', lazy='dynamic'))


# Define the Role data model
class Role(Base):
    __tablename__ = 'roles_flask_user_mod'
    # id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(50), unique=True)


# Define the UserRoles data model
class UserRoles(Base):
    __tablename__ = 'users_roles_flask_user_mod'
    # id = db.Column(db.Integer(), primary_key=True)
    user_id = db.Column(db.Integer(),
                        db.ForeignKey('users_flask_user_mod.id',
                                      ondelete='CASCADE'))
    role_id = db.Column(db.Integer(),
                        db.ForeignKey('roles_flask_user_mod.id',
                                      ondelete='CASCADE'))


# Define the User registration form
# It augments the Flask-User RegisterForm with additional fields
class MyRegisterForm(RegisterForm):
    first_name = StringField('First name', validators=[
        validators.DataRequired('First name is required')])
    last_name = StringField('Last name', validators=[
        validators.DataRequired('Last name is required')])


# Define the User profile form
class UserProfileForm(Form):
    first_name = StringField('First name', validators=[
        validators.DataRequired('First name is required')])
    last_name = StringField('Last name', validators=[
        validators.DataRequired('Last name is required')])
    submit = SubmitField('Save')
