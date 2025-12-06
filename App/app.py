from flask import Flask, jsonify, render_template, request
from elasticsearch import Elasticsearch

INDEX_NAME = "all-digital-music-products"

app = Flask(__name__)

es = Elasticsearch(
    'http://localhost:9200'
)


@app.route('/')
def home():
    return render_template("home.html")

@app.route('/search')
def search():
    query = request.args.get("q","")

    if not query:
        return jsonify([])
    
    body = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["title^2", "description"]
            }
        },
        "size": 20
    }

    response = es.search(index=INDEX_NAME, body=body)

    hits = [
        {
            "title": h["_source"].get("title"),
            "asin": h["_source"].get("parent_asin"),
            "rating": h["_source"].get("average_rating"),
            "image": (h["_source"].get("images") or [{}])[0].get("thumb")
        }
        for h in response["hits"]["hits"]
    ]
    
    return jsonify(hits)


if __name__ == '__main__':
    app.run()