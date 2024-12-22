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
#     try:
#         results = get_average_wounded_by_region()
#
#         return jsonify({"status": "success", "data": results}), 200
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/top_groups_by_victims', methods=['GET'])
def top_groups_by_victims_route():
    try:
        top_n = request.args.get('top_n', default=5, type=int)  # ברירת מחדל: 5
        results = get_top_groups_by_victims(top_n)

        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# @analysis_terror_bp.route('/api/attack_type_target_correlation', methods=['GET'])
# def attack_type_target_correlation_route():
#     try:
#         results = get_attack_type_target_correlation()
#
#         return jsonify({"status": "success", "data": results}), 200
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)}), 500






# def get_terror_incidents_change(region):
#     pass
#
#
# def get_average_wounded_by_region(region):
#     pass

