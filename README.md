# Project Team

Philippe Lizotte  
Sami Khalayli  
Karim Moussa  
Félix-An Pham-Ponton  
Refat Abuzriba


FIRST TIME BUILDING THE PROJECT: 
Dataset ingesting:
1. Create a virtual environment
2. install requirement with the requirement.txt file using pip install -r requirements.txt
3. run nltk_download.py to download the stopword, tokeniser
4. make sure the dataset in data folder is named correctly, meta_Video_Games.jsonl for the product and Video_Games.jsonl for the  review: dataset available at {link}
5. start the containers with docker-compose up -d when in project directory
6. cd in Ingest, run python ingest.py and python ingest_file.py for ingesting the dataset in Elasticsearch

Dashboard Configuration:
Using the kibana UI at localhost:5601:
1. Open Stack Management → Saved Objects
2. Click Import
3. Upload your dashboard.ndjson, found in kibana_dashboard

WHEN PROJECT BUILT:

Make sure you ve run docker-compose up -d

Open Flask Web App:
1. cd in ../app/ 
2. run python app.py 
3. go to http://127.0.0.1:5000 to see the Web App