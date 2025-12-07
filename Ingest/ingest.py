from elasticsearch import Elasticsearch, helpers
import json
INDEX_NAME = "video-game-reviews"


# Connect to Elasticsearch
es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=120,
    max_retries=10,
    retry_on_timeout=True
)
index_settings = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1
    }
}

es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
es.indices.create(index=INDEX_NAME, body=index_settings)

def read_and_index():
    with open(r'D:\ECOLE\PREPA MASTER\FALL2025\COMP6231 - Distributed System\New folder\data\Video_Games.jsonl', 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            try:
                doc = json.loads(line)
                yield {
                    '_index': INDEX_NAME,
                    '_source': doc
                }
                 
                if i % 10000 == 0:
                    print(f"Processed {i:,} documents...")
            except json.JSONDecodeError as e:
                print(f"Skipping line {i}: {e}")
                continue


# Bulk insert with batches of 1000
success, failed = helpers.bulk(es, read_and_index(), chunk_size=200, raise_on_error=False)

print(f"\nIngestion complete!")
print(f"Successfully indexed: {success:,}")
print(f"Failed: {failed}")
print(f"Total in ES: {es.count(index=INDEX_NAME)['count']:,}")