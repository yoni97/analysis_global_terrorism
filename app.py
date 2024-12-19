from flask import Flask

from configs.database_url import DATABASE_URL
from routes.analysis_terror_route import analysis_terror_bp

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.register_blueprint(analysis_terror_bp)

@app.route('/')
def hello_world():  # put application's code here
    return 'Welcome to Global Terrorism!'


if __name__ == '__main__':
    app.run(host='0.0.0.0',
            port=5000,
            debug=True)
