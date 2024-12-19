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








# from flask import Flask, render_template, request, jsonify
# import pandas as pd
# import folium
# from folium.plugins import MarkerCluster
#
# app = Flask(__name__)
#
# @app.route('/')
# def home():
#     return render_template('index.html')
#
#
# @app.route('/run_query', methods=['POST'])
# def run_query():
#     try:
#         # קבלת הנתונים מהבקשה (פונקציה ואזור)
#         function_type = request.form.get('function')
#         region = request.form.get('region')
#
#         # אם הפונקציה היא אחוז שינוי במספר הפיגועים
#         if function_type == 'terror_incidents_change':
#             results = get_terror_incidents_change(region)
#         # אם הפונקציה היא ממוצע פצועים לפי אזור
#         elif function_type == 'average_wounded_by_region':
#             results = get_average_wounded_by_region(region)
#         else:
#             return jsonify({"status": "error", "message": "Invalid function selected"}), 400
#
#         return jsonify({"status": "success", "data": results}), 200
#
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500
#
#
# def get_terror_incidents_change(region):
#     # הפונקציה שלך של אחוז שינוי במספר הפיגועים
#     # החישוב בוצע במבנה הנתונים שסופק לך
#     # מבצע סינון לפי אזור ומחזיר תוצאות
#     pass
#
#
# def get_average_wounded_by_region(region):
#     # הפונקציה שלך של ממוצע פצועים לפי אזור
#     # החישוב בוצע במבנה הנתונים שסופק לך
#     # מבצע סינון לפי אזור ומחזיר תוצאות
#     pass
#
#
# if __name__ == '__main__':
#     app.run(debug=True)

