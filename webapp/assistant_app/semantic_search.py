import json
from databricks.vector_search.client import VectorSearchClient
from dotenv import load_dotenv
import os

load_dotenv('.env')

vsc = VectorSearchClient()

index = vsc.get_index(endpoint_name="uk-search-endpoint" , index_name=os.environ.get("VECTOR_SEARCH_INDEX"))

async def semantic_search(user_query):
    results = index.similarity_search(query_text=user_query, num_results=10, query_type="hybrid", score_threshold=0.5,
    columns=[
        "ID",
        "Date",
        "Channel",
        "Chunk",
        "URL",
    ],
    )
    return json.dumps(results)