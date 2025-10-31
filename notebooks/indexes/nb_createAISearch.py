# Databricks notebook source
!pip install azure-search-documents==11.6.0b4

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../common/nb_init

# COMMAND ----------

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from azure.search.documents.models import VectorizableTextQuery

from azure.search.documents.indexes.models import (
    AzureOpenAIEmbeddingSkill,
    AzureOpenAIParameters,
    AzureOpenAIVectorizer,
    HnswAlgorithmConfiguration,
    InputFieldMappingEntry,
    OutputFieldMappingEntry,  
    SemanticPrioritizedFields,    
    SearchField,  
    SearchFieldDataType,  
    SearchIndex,  
    SearchIndexer,  
    SearchIndexerDataContainer,  
    SearchIndexerDataSourceConnection,  
    SearchIndexerIndexProjectionSelector,
    SearchIndexerIndexProjections,  
    SearchIndexerSkillset,  
    SemanticConfiguration,  
    SemanticField,  
    SemanticSearch,
    SplitSkill,  
    SqlIntegratedChangeTrackingPolicy,
    VectorSearch,  
    VectorSearchAlgorithmKind,
    VectorSearchProfile
)  

import openai

# COMMAND ----------

search_indexname = 'social-index-v1'

# COMMAND ----------

search_key = dbutils.secrets.get(scope=secretScope,key="search-key")
search_endpoint = dbutils.secrets.get(scope=secretScope,key="search-endpoint")

db_server = dbutils.secrets.get(scope=secretScope,key="sql-server-fqdn")
db_database = dbutils.secrets.get(scope=secretScope,key="sql-db-name")
db_user = get_secret('sql-server-admin-name')
db_password = get_secret('sql-server-admin-password')

openai_endpoint = get_secret('openai-endpoint')
openai_key = get_secret('openai-key')

# COMMAND ----------

openai_type = 'azure'
openai_deployment_embedding = 'text-embedding-ada-002'
openai_model_embedding = 'text-embedding-ada-002'
openai_deployment_completion = 'gpt-35-turbo'
openai_model_completion = 'gpt-35-turbo'
openai_api_version = '2024-02-01'
embedding_length = 1536

# COMMAND ----------

akc = AzureKeyCredential(search_key)

# COMMAND ----------

ds_conn_str = f'Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;Server=tcp:{db_server};Database={db_database};User ID={db_user};Password={db_password};'

ds_client = SearchIndexerClient(search_endpoint, akc)
container = SearchIndexerDataContainer(name='ai.consolidated_per_row_json_v2')

data_source_connection = SearchIndexerDataSourceConnection(
    name=f"{search_indexname}-azuresql-connection",
    type="azuresql",
    connection_string=ds_conn_str,
    container=container,
)
data_source = ds_client.create_or_update_data_source_connection(data_source_connection)

# COMMAND ----------

# Create a search index
index_client = SearchIndexClient(
    endpoint=search_endpoint, credential=akc)

# COMMAND ----------

fields = [
    # Properties of individual chunk
    SearchField(name="hash_key", type=SearchFieldDataType.String, key=True,sortable=True, filterable=True, facetable=True, analyzer_name="keyword"),
    SearchField(name="chunk", type=SearchFieldDataType.String, sortable=False, filterable=False, facetable=False),
    SearchField(name="vector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single), 
                vector_search_dimensions=embedding_length, vector_search_profile_name="code-vector-search-profile"),
    # Properties of original row in DB that the chunk belonged to
    SearchField(name="parent_id", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),
    # SearchField(name="social_post_id", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),
    SearchField(name="object", type=SearchFieldDataType.String, sortable=True, filterable=True, facetable=True),
    SearchField(name="source_table", type=SearchFieldDataType.String, sortable=False, filterable=True, facetable=False),
]

# Configure the vector search configuration  
vector_search = VectorSearch(
    algorithms=[
        HnswAlgorithmConfiguration(
            name="code-hnsw-config",
            kind=VectorSearchAlgorithmKind.HNSW
        )
    ],
    profiles=[
        VectorSearchProfile(
            name="code-vector-search-profile",
            algorithm_configuration_name="code-hnsw-config",
            vectorizer="code-openai-vectorizer"
        )
    ],
    vectorizers=[
        AzureOpenAIVectorizer(
            name="code-openai-vectorizer",
            kind="azureOpenAI",
            azure_open_ai_parameters=AzureOpenAIParameters(
                resource_uri=openai_endpoint,
                deployment_id=openai_deployment_embedding,
                api_key=openai_key,
                model_name=openai_model_embedding
            )
        )  
    ]  
)


## todo
semantic_config = SemanticConfiguration(
    name="code-semantic-config",
    prioritized_fields=SemanticPrioritizedFields(
        content_fields=[SemanticField(field_name="hash_key")]
    )
)
semantic_search = SemanticSearch(configurations=[semantic_config])
index = SearchIndex(name=search_indexname, fields=fields,
                    vector_search=vector_search, semantic_search=semantic_search)
result = index_client.create_or_update_index(index)
print(f'{result.name} created')

# COMMAND ----------

# Create a skillset  
skillset_name = f"{search_indexname}-sk"

split_skill = SplitSkill(
    description="Split skill to chunk documents",
    text_split_mode="pages",
    context="/document",
    maximum_page_length=300,
    page_overlap_length=20,
    inputs=[
        InputFieldMappingEntry(name="text", source="/document/object"),
    ],
    outputs=[
        OutputFieldMappingEntry(name="textItems", target_name="pages")
    ]
)

embedding_skill = AzureOpenAIEmbeddingSkill(
    description="Skill to generate embeddings via Azure OpenAI",
    context="/document/pages/*",
    resource_uri=openai_endpoint,
    deployment_id=openai_deployment_embedding,
    api_key=openai_key,
    model_name=openai_model_embedding,
    inputs=[
        InputFieldMappingEntry(name="text", source="/document/pages/*"),
    ],
    outputs=[
        OutputFieldMappingEntry(name="embedding", target_name="vector")
    ]
)

index_projections = SearchIndexerIndexProjections(
    selectors=[
        SearchIndexerIndexProjectionSelector(
            target_index_name=search_indexname,
            parent_key_field_name="parent_id", # Note: this populates the "parent_id" search field
            source_context="/document/pages/*",
            mappings=[
                InputFieldMappingEntry(name="chunk", source="/document/pages/*"),
                InputFieldMappingEntry(name="vector", source="/document/pages/*/vector"),
                # InputFieldMappingEntry(name="social_post_id", source="/document/social_post_id"),
                InputFieldMappingEntry(name="object", source="/document/object"),
                InputFieldMappingEntry(name="source_table", source="/document/source_table"),
            ],  
        ),  
    ],
)  

skillset = SearchIndexerSkillset(  
    name=skillset_name,  
    description="Skillset to chunk documents and generating embeddings",
    skills=[split_skill, embedding_skill],
    index_projections=index_projections,
)

client = SearchIndexerClient(search_endpoint, akc)
client.create_or_update_skillset(skillset)
print(f' {skillset.name} created')

# COMMAND ----------

# Create an indexer  
indexer_name = f"{search_indexname}-indexer"  

indexer = SearchIndexer(  
    name=indexer_name,  
    description="Indexer to chunk documents and generate embeddings",  
    skillset_name=skillset_name,  
    target_index_name=search_indexname,  
    data_source_name=data_source.name
)  
  
indexer_client = SearchIndexerClient(search_endpoint, akc)
indexer_result = indexer_client.create_or_update_indexer(indexer)  

# COMMAND ----------

# MAGIC %md
# MAGIC check if the indexer is running, before executing the cell below

# COMMAND ----------

# Run the indexer  
# indexer_client.run_indexer(indexer_name)
# print(f' {indexer_name} created')
