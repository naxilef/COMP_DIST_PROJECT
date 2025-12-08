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
    rating = request.args.get("rating")
    sort_price = request.args.get("price_order")


    if not query:
        return jsonify([])
    
    must_clauses = []
    filter_clauses = []

    must_clauses.append({
        "multi_match": {
                "query": query,
                "fields": ["title^2", "description"]
            }
    })

    if rating:
        filter_clauses.append({
            "range": {
                "average_rating": {
                    "gte": float(rating)
                }
            }
        })

    filter_clauses.append({
        "range": {
            "price": {"gt": 0}
        }
    })
    
    sort_clause =[]
    if sort_price in ("asc", "desc"):
        sort_clause.append({"price": {"order": sort_price}})

    body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses
            }
        },
        "size": 20
    }
    if sort_clause:
        body["sort"] = sort_clause

    response = es.search(index=INDEX_NAME, body=body)

    hits = [
        {
            "title": h["_source"].get("title"),
            "asin": h["_source"].get("parent_asin"),
            "rating": h["_source"].get("average_rating"),
            "image": (h["_source"].get("images") or [{}])[0].get("thumb"),
            "price": h["_source"].get("price")
        }
        for h in response["hits"]["hits"]
    ]
    
    return jsonify(hits)

@app.route("/item/<asin>")
def item_page(asin):
    return render_template("item.html", item=asin)

if __name__ == '__main__':
    app.run()