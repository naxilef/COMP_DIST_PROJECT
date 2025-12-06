import json 
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

load_dotenv()

ES_USER = "elastic" 
ES_HOST = "https://localhost:9200"
INDEX_NAME = "all-digital-music-products"
JSON_FILE = "data\meta_Digital_Music.jsonl"


es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=120,
    max_retries=10,
    retry_on_timeout=True
)

print(es.info())

mapping = {
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

def load_documents(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        # Handle both array JSON and line-delimited JSON
        first_char = f.read(1)
        f.seek(0)

        if first_char == "[":
            data = json.load(f)
            for doc in data:
                yield transform_doc(doc)
        else:
            for line in f:
                if line.strip():
                    yield transform_doc(json.loads(line))

def transform_doc(doc):
    details = doc.get("details", {})

    # Keep flattened version of all details
    doc["details"] = details  # already a dict → becomes flattened

    return doc

def bulk_ingest(file):
    actions = (
        {
            "_index": INDEX_NAME,
            "_source": doc
        }
        for doc in load_documents(file)
    )

    print("Indexing documents...")
    try:
        helpers.bulk(es, actions, chunk_size=200)
        print(f"Done ingesting: {file}")

    except helpers.BulkIndexError as e:
        print(f"Some documents failed in {file}. Skipping bad docs...")
        print(f"Failed count: {len(e.errors)}")
        pass
    print("Done!")

if __name__ == "__main__":
  bulk_ingest(JSON_FILE)
  print(f"Total in ES: {es.count(index=INDEX_NAME)['count']:,}")