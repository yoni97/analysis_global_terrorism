from flask import Blueprint, jsonify, request
from db_service.analyze_data import get_average_wounded_by_region, get_top_attack_types, get_top_groups_by_victims, \
    get_terror_incidents_change, get_groups_with_common_targets, get_top_active_groups, track_group_expansion_over_time, \
    find_shared_attack_strategies, find_groups_in_same_attack, \
    find_groups_and_attack_count_by_target, find_areas_with_high_intergroup_activity, \
    find_groups_with_shared_targets_by_year, find_influential_groups

analysis_terror_bp = Blueprint('analysis_terror_bp', __name__)

@analysis_terror_bp.route('/api/top_attack_types', methods=['GET'])
def top_attack_types_route():
    try:
        top_n = request.args.get('top_n', type=int)
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
        # results = get_average_wounded_by_region(show_top_5)
        return get_average_wounded_by_region(show_top_5=show_top_5)

        # return jsonify({"status": "success", "data": results}), 200
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

@analysis_terror_bp.route('/api/terror_incidents_change', methods=['GET'])
def terror_incidents_change_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'
        results = get_terror_incidents_change(show_top_5=show_top_5)

        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/groups_with_common_targets', methods=['GET'])
def groups_with_common_targets_route():
    try:
        filter_by = request.args.get('filter_by', 'region')  # region או country
        filter_value = request.args.get('filter_value', None)  # ערך סינון

        results = get_groups_with_common_targets(filter_by=filter_by, filter_value=filter_value)

        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/top_active_groups', methods=['GET'])
def top_active_groups_route():
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
    try:
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')

        results = find_shared_attack_strategies(region_filter, country_filter)

        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api/similar_targets', methods=['GET'])
def groups_with_similar_target_preferences_route():
    try:
        region_filter = request.args.get('region', None)
        country_filter = request.args.get('country', None)

        result = find_groups_and_attack_count_by_target(region_filter, country_filter)

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

        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@analysis_terror_bp.route('/api/same_targets_in_new_year', methods=['GET'])
def shared_targets_on_same_year_route():
    try:
        result = find_groups_with_shared_targets_by_year()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@analysis_terror_bp.route('/run_query', methods=['POST'])
def run_query():
    try:
        data = request.get_json()
        function_type = data.get('function')
        region = data.get('region')

        if function_type == 'terror_incidents_change':
            results = get_terror_incidents_change(region)
        elif function_type == 'average_wounded_by_region':
            results = get_average_wounded_by_region(region)
        else:
            return jsonify({"status": "error", "message": "Invalid function selected"}), 400

        return jsonify({"status": "success", "data": results}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500




# def get_terror_incidents_change(region):
#     pass
#
#
# def get_average_wounded_by_region(region):
#     pass

