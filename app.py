from flask import Flask, render_template
from configs.mongodb import terrorism_actions, news
from db_repo.upload_to_pandas import upload_to_pandas
from elastic_search.load_to_elastic import load_to_elasticsearch
from routes.analysis_terror_route import analysis_terror_bp
from routes.elastic_search_route import elastic_bp

app = Flask(__name__)

app.register_blueprint(analysis_terror_bp)
app.register_blueprint(elastic_bp)

@app.route('/')
def home():
    return render_template('index.html')


if __name__ == '__main__':
    load_to_elasticsearch()
    upload_to_pandas(terrorism_actions)
    upload_to_pandas(news)
    app.run(debug=True)