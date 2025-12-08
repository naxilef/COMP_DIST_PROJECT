import json 
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

load_dotenv()

ES_USER = "elastic" 
ES_HOST = "https://localhost:9200"
INDEX_NAME = "video-game-products"
JSON_FILE = r"..\data\meta_Video_Games.jsonl" #put complete path to data


es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=120,
    max_retries=10,
    retry_on_timeout=True
)

print(es.info())

mapping = {
  "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1   
  },
  "mappings": {
    "properties": {
      "parent_asin": {"type": "keyword"},
      "main_category": {"type": "keyword"},
      "categories": {"type": "keyword"},
      "title": {"type": "text"},
      "subtitle": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
      "description": {"type": "text"},
      "price": {"type": "double"},
      "average_rating": {"type": "double"},
      "rating_number": {"type": "long"},
      "store": {"type": "keyword"},

      "author": {
        "properties": {
          "name": {"type": "text"},
          "about": {"type": "text"},
          "avatar": {"type": "keyword"}
        }
      },

      "images": {
        "properties": {
          "hi_res": {"type": "keyword"},
          "large": {"type": "keyword"},
          "thumb": {"type": "keyword"},
          "variant": {"type": "keyword"}
        }
      },

      "videos": {
        "properties": {
          "title": {"type": "text"},
          "url": {"type": "keyword"},
          "user_id": {"type": "keyword"}
        }
      },

      "features": {"type": "text"},

      "details": {
        "type": "flattened"
      }
    }
  }
}

es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
es.indices.create(index=INDEX_NAME, body=mapping)

print("Mapping created!")

def load_documents(path):
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            if not line.strip():
                continue

            try:
                doc = json.loads(line)
                yield transform_doc(doc)

            except Exception as e:
                print(f"[ERROR] Failed at line {lineno}: {e}")
                continue



def clean_number(value):
    
    try:
        return float(value)
    except:
        return None

def clean_list(value):
    
    if isinstance(value, list):
        return " ".join([str(v) for v in value])
    return value

def clean_details(details):
    
    if isinstance(details, dict):
        return details
    return {}

def clean_images(images):
    
    if isinstance(images, list):
        return images
    return []

def transform_doc(doc):


    doc["price"] = clean_number(doc.get("price"))
    doc["average_rating"] = clean_number(doc.get("average_rating"))
    doc["rating_number"] = clean_number(doc.get("rating_number"))

    doc["description"] = clean_list(doc.get("description"))
    doc["features"] = clean_list(doc.get("features"))

    doc["details"] = clean_details(doc.get("details"))
    doc["images"] = clean_images(doc.get("images"))

    return doc


def bulk_ingest(path):
    batch = []
    successes = 0
    failures = 0

    print("Indexing...")

    for doc in load_documents(path):
        batch.append({
            "_index": INDEX_NAME,
            "_source": doc
        })

        if len(batch) == 1000:
            ok, fail = helpers.bulk(
                es,
                batch,
                raise_on_error=False,
                raise_on_exception=False
            )
            successes += ok
            failures += len(fail)
            batch = []

    # leftover docs
    if batch:
        ok, fail = helpers.bulk(
            es,
            batch,
            raise_on_error=False,
            raise_on_exception=False
        )
        successes += ok
        failures += len(fail)

    print(f"✓ Indexed: {successes}")
    print(f"✗ Failed:  {failures}")


if __name__ == "__main__":
  bulk_ingest(JSON_FILE)
  print(f"Total in ES: {es.count(index=INDEX_NAME)['count']:,}")