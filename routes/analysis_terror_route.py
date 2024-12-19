from flask import Blueprint, jsonify, request
from db_service.analizy_data import get_average_wounded_by_region, get_top_attack_types, get_top_groups_by_victims, \
     get_terror_incidents_change

analysis_terror_bp = Blueprint('analysis_terror_bp', __name__)

@analysis_terror_bp.route('/api/top_attack_types', methods=['GET'])
def top_attack_types_route():
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
    try:
        # קבלת פרמטרים מהבקשה
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'

        # חישוב ממוצע נפגעים לפי אזור
        results = get_average_wounded_by_region(show_top_5=show_top_5)

        # המרת התוצאה ל-JSON
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# @analysis_terror_bp.route('/api/average_wounded_by_region', methods=['GET'])
# def average_wounded_by_region_route():
#     """
#     חישוב ממוצע נפגעים לפי אזור
#     """
#     try:
#         results = get_average_wounded_by_region()
#
#         # המרת התוצאה ל-JSON
#         return jsonify({"status": "success", "data": results}), 200
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500




@analysis_terror_bp.route('/api/top_groups_by_victims', methods=['GET'])
def top_groups_by_victims_route():
    try:
        # קבלת חמש הקבוצות עם הכי הרבה נפגעים
        top_n = request.args.get('top_n', default=5, type=int)  # ברירת מחדל: 5
        results = get_top_groups_by_victims(top_n)

        # המרת התוצאה ל-JSON
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# @analysis_terror_bp.route('/api/attack_type_target_correlation', methods=['GET'])
# def attack_type_target_correlation_route():
#     """
#     חישוב קורלציה בין סוגי ההתקפה למטרות
#     """
#     try:
#         # חישוב הקורלציה בין סוגי ההתקפה למטרות
#         results = get_attack_type_target_correlation()
#
#         # המרת התוצאה ל-JSON
#         return jsonify({"status": "success", "data": results}), 200
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/terror_incidents_change', methods=['GET'])
def terror_incidents_change_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'

        # חישוב אחוז השינוי במספר הפיגועים בין השנים לפי אזור
        results = get_terror_incidents_change(show_top_5=show_top_5)

        # המרת התוצאה ל-JSON
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


