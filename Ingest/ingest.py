from elasticsearch import Elasticsearch, helpers
import json

# Connect to Elasticsearch
es = Elasticsearch(
    "http://localhost:9200",
    request_timeout=120,
    max_retries=10,
    retry_on_timeout=True
)

def read_and_index():
    with open('data\Digital_Music.jsonl', 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            try:
                doc = json.loads(line)
                yield {
                    '_index': 'all-music-review',
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
print(f"Total in ES: {es.count(index='all-music-review')['count']:,}")