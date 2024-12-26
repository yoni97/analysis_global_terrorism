import logging

from flask import Blueprint, jsonify, request, render_template, redirect
from db_service.analyze_data import get_average_wounded_by_region, get_top_attack_types, get_top_groups_by_victims, \
    get_terror_incidents_change, get_groups_with_common_targets, get_top_active_groups, track_group_expansion_over_time, \
    find_shared_attack_strategies, find_groups_in_same_attack, \
    find_groups_and_attack_count_by_target, find_areas_with_high_intergroup_activity, \
    find_groups_with_shared_targets_by_year, find_influential_groups, get_terror_incidents_change1, \
    get_average_wounded_by_region_event

function_to_route = {
    "get_top_active_groups": "/top_active_groups",
    'get_terror_incidents_change': '/api/terror_incidents_change',
    'get_groups_with_common_targets': '/api/groups_with_common_targets',
    'find_influential_groups': '/api/most_influential_groups',
    'get_average_wounded_by_region_event': '/api/average_wounded_by_region_event',
    'get_terror_incidents_change1': 'terror_incidents_change_region',
    'find_areas_with_high_intergroup_activity': '/api/areas_with_high_intergroup_activity',
    'find_groups_with_common_targets': '/api/groups_with_common_targets',
    'find_shared_attack_strategies': '/api/shared_attack_strategies',
}
analysis_terror_bp = Blueprint('analysis_terror_bp', __name__)

# 1:
@analysis_terror_bp.route('/api/top_attack_types', methods=['GET'])
def top_attack_types_route():
    try:
        top_n = request.args.get('top_n', type=int)
        results = get_top_attack_types(top_n)

        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 2a:
@analysis_terror_bp.route('/api/average_wounded_by_region_event', methods=['GET'])
def average_wounded_by_region_event_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'

        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')
        results, map_html = get_average_wounded_by_region_event(show_top_5)

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": results
            }), 200
        else:
            return render_template("map.html", map_html=map_html)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 2b:
@analysis_terror_bp.route('/api/average_wounded_by_region', methods=['GET'])
def average_wounded_by_region_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'
        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        response = get_average_wounded_by_region(show_top_5)

        if response["status"] != "success":
            return jsonify({"status": "error", "message": response.get("message", "Unknown error")}), 500

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": response["data"]
            }), 200
        else:
            return render_template("map.html", map_html=response["map_html"])

    except Exception as e:
        logging.error(f"Error in average_wounded_by_region_route: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# 3:
@analysis_terror_bp.route('/api/top_groups_by_victims', methods=['GET'])
def top_groups_by_victims_route():
    try:
        top_n = request.args.get('top_n', default=5, type=int)
        results = get_top_groups_by_victims(top_n)

        return jsonify({"status": "success", "data": results}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 6:
@analysis_terror_bp.route('/api/terror_incidents_change', methods=['GET'])
def terror_incidents_change_route():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'
        results, map_html = get_terror_incidents_change(show_top_5=show_top_5)
        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": results
            }), 200
        else:
            return render_template("map.html", map_html=map_html)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@analysis_terror_bp.route('/api/terror_incidents_change_region', methods=['GET'])
def terror_incidents_change_routers():
    try:
        show_top_5 = request.args.get('show_top_5', 'false').lower() == 'true'

        result, map_html = get_terror_incidents_change1(show_top_5=show_top_5)

        if isinstance(result, dict) and result.get("status") == "error":
            return jsonify(result), 500

        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": result
            }), 200
        else:
            return render_template("map.html", map_html=map_html)

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 8:
@analysis_terror_bp.route('/top_active_groups', methods=['GET'])
def top_active_groups_route():
    try:
        filter_by = request.args.get('filter_by', 'region')
        filter_value = request.args.get('filter_value', None)
        top_n = int(request.args.get('top_n', 5))

        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')

        results, map_html = get_top_active_groups(filter_by=filter_by, filter_value=filter_value, top_n=top_n)

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": results
            }), 200
        else:
            return render_template("map.html", map_html=map_html)

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 11:
@analysis_terror_bp.route('/api/groups_with_common_targets', methods=['GET'])
def groups_with_common_targets_route():
    try:
        filter_by = request.args.get('filter_by', 'region')
        filter_value = request.args.get('filter_value', None)
        results = get_groups_with_common_targets(filter_by=filter_by, filter_value=filter_value)

        user_agent = request.headers.get('User-Agent', '').lower()
        accept_header = request.headers.get('Accept', '')
        if results["status"] != "success":
            return jsonify({"status": "error", "message": results.get("message", "Unknown error")}), 500

        if "postman" in user_agent or "application/json" in accept_header:
            return jsonify({
                "status": "success",
                "data": results["data"]
            }), 200
        else:
            return render_template("map.html", map_html=results["map_html"])
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 12:
@analysis_terror_bp.route('/api/group_expansion', methods=['GET'])
def group_expansion_route():
    try:
        results = track_group_expansion_over_time()
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 13:
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

        return results
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 15:
@analysis_terror_bp.route('/api/similar_targets', methods=['GET'])
def groups_with_similar_target_preferences_route():
    try:
        region_filter = request.args.get('region', None)
        country_filter = request.args.get('country', None)
        result = find_groups_and_attack_count_by_target(region_filter, country_filter)

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 16:
@analysis_terror_bp.route('/api/areas_with_high_intergroup_activity', methods=['GET'])
def areas_with_high_intergroup_activity():
    try:
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')
        result = find_areas_with_high_intergroup_activity(region_filter, country_filter)

        return result
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 18:
@analysis_terror_bp.route('/api/most_influential_groups', methods=['GET'])
def influential_groups_route():
    try:
        region_filter = request.args.get('region')
        country_filter = request.args.get('country')

        result = find_influential_groups(region_filter=region_filter, country_filter=country_filter)
        return result
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

# 19:
@analysis_terror_bp.route('/api/same_targets_in_new_year', methods=['GET'])
def shared_targets_on_same_year_route():
    try:
        result = find_groups_with_shared_targets_by_year()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@analysis_terror_bp.route('/api', methods=['GET'])
def analyze_data():
    function = request.args.get('function')
    filter_type = request.args.get('filter_type', '')
    display_option = request.args.get('display_option', '')

    if not function:
        return {"error": "Missing required 'function' parameter"}, 400

    route = function_to_route.get(function)
    if route:
        if display_option:
            query_params1 = f"?display_option={display_option}"
        if filter_type:
            query_params2 = f"?filter_type={filter_type}"
        if filter_type and display_option:
            query_params = f"{query_params1}&{query_params2}"
        return redirect(route + query_params)
    else:
        return {"error": "Invalid function parameter"}, 400