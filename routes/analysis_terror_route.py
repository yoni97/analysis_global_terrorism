from flask import Blueprint, jsonify, request
from db_service.analizy_data import get_average_wounded_by_region, get_top_attack_types, get_top_groups_by_victims, \
    get_terror_incidents_change, get_groups_with_common_targets, get_top_active_groups, track_group_expansion_over_time, \
    find_shared_attack_strategies, find_groups_in_same_attack, \
    find_groups_and_attack_count_by_target, find_areas_with_high_intergroup_activity, \
    find_groups_with_shared_targets_by_year, find_influential_groups

analysis_terror_bp = Blueprint('analysis_terror_bp', __name__)

@analysis_terror_bp.route('/api/top_attack_types', methods=['GET'])
def top_attack_types_route():
    try:
        top_n = request.args.get('top_n', type=int)  # ברירת מחדל: None אם לא נשלח
        results = get_top_attack_types(top_n)

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
        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/average_wounded_by_region', methods=['GET'])
def average_wounded_by_region_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'
        results = get_average_wounded_by_region(show_top_5=show_top_5)

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

        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/groups_with_common_targets', methods=['GET'])
def groups_with_common_targets_route():
    """
    זיהוי קבוצות עם מטרות משותפות באותו אזור.
    """
    try:
        # קבלת פרמטרים
        filter_by = request.args.get('filter_by', 'region')  # region או country
        filter_value = request.args.get('filter_value', None)  # ערך סינון

        # חישוב הנתונים
        results = get_groups_with_common_targets(filter_by=filter_by, filter_value=filter_value)

        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/top_active_groups', methods=['GET'])
def top_active_groups_route():
    """
    זיהוי הקבוצות הפעילות ביותר לפי אזור
    """
    try:
        filter_by = request.args.get('filter_by', 'region')
        filter_value = request.args.get('filter_value', None)  # ערך סינון
        top_n = int(request.args.get('top_n', 5))

        results = get_top_active_groups(filter_by=filter_by, filter_value=filter_value, top_n=top_n)
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/group_expansion', methods=['GET'])
def group_expansion_route():
    """
    מעקב אחר פעילות קבוצות במספר אזורים לאורך זמן
    """
    try:
        results = track_group_expansion_over_time()
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/groups_with_same_attacks', methods=['GET'])
def get_groups_in_same_attack():
    try:
        response = find_groups_in_same_attack()
        if response['status'] == 'success':
            return jsonify({"status": "success", "data": response['data']}), 200
        else:
            return jsonify({"status": "error", "message": response['message']}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

#   Question 14:
@analysis_terror_bp.route('/api/shared_attack_strategies', methods=['GET'])
def shared_attack_strategies_route():
    """
    זיהוי אזורים עם אסטרטגיות תקיפה משותפות בין קבוצות
    """
    try:
        # קבלת פרמטרים מהבקשה
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')

        # חישוב המידע
        results = find_shared_attack_strategies(region_filter, country_filter)

        # החזרת התוצאה
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/similar_targets', methods=['GET'])
def groups_with_similar_target_preferences_route():
    """
    איתור קבוצות עם העדפות דומות למטרות
    """
    try:
        region_filter = request.args.get('region', None)
        country_filter = request.args.get('country', None)

        result = find_groups_and_attack_count_by_target(region_filter, country_filter)

        # החזרת התוצאה כ-JSON
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/areas_with_high_intergroup_activity', methods=['GET'])
def areas_with_high_intergroup_activity():
    try:
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')

        result = find_areas_with_high_intergroup_activity(region_filter, country_filter)

        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/most_influential_groups', methods=['GET'])
def influential_groups_route():
    try:
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')

        result = find_influential_groups(region_filter=region_filter, country_filter=country_filter)

        # החזרת תוצאה
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@analysis_terror_bp.route('/api/same_targets_in_new_year', methods=['GET'])
def shared_targets_on_same_year_route():
    """
    נתיב לזיהוי קבוצות טרור התוקפות מטרות זהות באותה שנה.
    """
    try:
        result = find_groups_with_shared_targets_by_year()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


from flask import render_template



@analysis_terror_bp.route('/in')
def home():
    return render_template('index.html')


@analysis_terror_bp.route('/run_query', methods=['POST'])
def run_query():
    try:
        # קבלת הנתונים מהבקשה (JSON)
        data = request.get_json()
        function_type = data.get('function')
        region = data.get('region')

        # אם הפונקציה היא אחוז שינוי במספר הפיגועים
        if function_type == 'terror_incidents_change':
            results = get_terror_incidents_change(region)
        # אם הפונקציה היא ממוצע פצועים לפי אזור
        elif function_type == 'average_wounded_by_region':
            results = get_average_wounded_by_region(region)
        else:
            return jsonify({"status": "error", "message": "Invalid function selected"}), 400

        return jsonify({"status": "success", "data": results}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500




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

