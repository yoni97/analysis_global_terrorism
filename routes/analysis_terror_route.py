from flask import Blueprint, jsonify, request
from db_service.analizy_data import get_average_wounded_by_region, get_top_attack_types

analysis_terror_bp = Blueprint('analysis_terror_bp', __name__)

@analysis_terror_bp.route('/api/top_attack_types', methods=['GET'])
def top_attack_types_route():
    """
    הפקת סוגי התקפה הקטלניים ביותר
    """
    try:
        # בדיקת פרמטר top_n מהבקשה
        top_n = request.args.get('top_n', type=int)  # ברירת מחדל: None אם לא נשלח
        results = get_top_attack_types(top_n)

        # המרת תוצאת הנתונים ל-JSON והחזרה
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/all_attack_types', methods=['GET'])
def all_attack_types_route():
    """
    הפקת כל סוגי ההתקפה
    """
    try:
        results = get_top_attack_types(1000)  # ללא מגבלת top_n

        # המרת תוצאת הנתונים ל-JSON והחזרה
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/average_wounded_by_region', methods=['GET'])
def average_wounded_by_region_route():
    """
    חישוב ממוצע נפגעים לפי אזור
    """
    try:
        results = get_average_wounded_by_region()

        # המרת התוצאה ל-JSON
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500