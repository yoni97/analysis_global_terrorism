from flask import request, jsonify, Blueprint
from elastic_search.indexing import es

elastic_bp = Blueprint('elastic_bp', __name__)

@elastic_bp.route("/keywords/search/", methods=["GET"])
def keyword_search():
    keywords = request.json.get("keywords")
    limit = int(request.json.get("limit", 10))
    query = {"query": {"multi_match": {"query": keywords, "fields": ["*"]}}}
    res = es.search(index="*", body=query, size=limit)
    return jsonify(res["hits"]["hits"])

@elastic_bp.route("/news/search/", methods=["GET"])
def news_search():
    keywords = request.args.get("keywords")
    limit = int(request.args.get("limit", 10))
    query = {"query": {"match": {"text": keywords}}}
    res = es.search(index="realtime_news", body=query, size=limit)
    return jsonify(res["hits"]["hits"])

@elastic_bp.route("/historic/search/", methods=["GET", "POST"])
def historic_search():
    keywords = request.json.get("keywords")
    limit = int(request.json.get("limit", 10))

    if not keywords:
        return jsonify({"error": "Missing 'keywords' parameter"}), 400
    query = {
        "query": {
            "match": {
                "_all": keywords
            }
        }
    }

    try:
        res = es.search(index="historic_data", body=query, size=limit)
        return jsonify(res["hits"]["hits"])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@elastic_bp.route("/combined/search/", methods=["GET", "POST"])
def combined_search():
    keywords = request.json.get("keywords")
    limit = int(request.json.get("limit", 10))

    if not keywords:
        return jsonify({"error": "Missing 'keywords' parameter"}), 400
    query = {
        "query": {
            "multi_match": {
                "query": keywords,
                "fields": ["*"]
            }
        }
    }

    try:
        res = es.search(index="*", body=query, size=limit)
        return jsonify(res["hits"]["hits"])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# if __name__ == "__main__":
#     elastic_bp.run(debug=True)