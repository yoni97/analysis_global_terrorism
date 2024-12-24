from flask import Flask, render_template

from configs.database_url import DATABASE_URL
from db_repo.upload_to_pandas import upload_to_pandas
from routes.analysis_terror_route import analysis_terror_bp

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.register_blueprint(analysis_terror_bp)

@app.route('/')
def home():
    return render_template('index.html')


if __name__ == '__main__':
    upload_to_pandas()
    app.run(host='0.0.0.0',
            port=5000,
            debug=True)