from flask import Flask, jsonify, render_template, request
from elasticsearch import Elasticsearch

INDEX_NAME = "video-game-products"
DASHBOARD_ID = "d82dc7f0-d470-11f0-8b21-657c1ad5fc00"

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
            "ID": h["_id"],
            "title": h["_source"].get("title"),
            "asin": h["_source"].get("parent_asin"),
            "rating": h["_source"].get("average_rating"),
            "image": (h["_source"].get("images") or [{}])[0].get("thumb"),
            "price": h["_source"].get("price")
            
        }
        for h in response["hits"]["hits"]
    ]
    
    return jsonify(hits)

@app.route("/item/<ID>")
def item_page(ID):
    try:
        res = es.get(index=INDEX_NAME, id=ID)
    except Exception:
        return render_template("item.html", item=None)

    src = res["_source"]
    asin = src.get("parent_asin")

    images = src.get("images") or []
    image = None
    if images and isinstance(images, list):
        image = images[0].get("large") or images[0].get("thumb")

    item = {
        "ID": ID,
        "title": src.get("title", "Untitle"),
        "description": src.get("description"),
        "image": image
    }

    review_query = {
        "query": {
            "term": {
                "asin": asin
            }
        },
        "size": 20
    }

    review_res = es.search(index="video-game-reviews", body=review_query)

    if len(review_res["hits"]["hits"]) == 0:
        review_query = {
            "query": {
                "term": {
                    "parent_asin.keyword": asin
                }
            },
            "size": 20
        }
        review_res = es.search(index="video-game-reviews", body=review_query)
    

    # Format reviews
    reviews = []
    for r in review_res["hits"]["hits"]:
        s = r["_source"]

        reviews.append({
            "user": s.get("user_id", "Anonymous"),
            "rating": s.get("rating", "N/A"),
            "text": s.get("text") or "",
            "title": s.get("title", ""),
            "helpful": s.get("helpful_vote", 0),
            "verified": s.get("verified_purchase", False),
            "timestamp": s.get("timestamp")
        })

    base_url = (f"http://localhost:5601/app/dashboards#/view/{DASHBOARD_ID}"
                "?embed=true&_g=(refreshInterval:(pause:!t,value:60000),time:(from:now-15m,to:now))")
    filter_part = (
        "&_a=(filters:!("
        "("
            "meta:(alias:!n,disabled:!f,index:'video-game-reviews',key:asin,"
            "negate:!f,params:(query:'{asin}'),type:phrase),"
            "query:(match_phrase:(asin:'{asin}'))"
        "),"
        "("
            "meta:(alias:!n,disabled:!f,index:'video-game-reviews',key:parent_asin,"
            "negate:!f,params:(query:'{asin}'),type:phrase),"
            "query:(match_phrase:(parent_asin:'{asin}'))"
        ")"
        "))&hide-filter-bar=true"
    )

    kibana_url = base_url + filter_part.format(asin=asin)


    return render_template("item.html", item=item, reviews=reviews, kibana_url=kibana_url)

if __name__ == '__main__':
    app.run()