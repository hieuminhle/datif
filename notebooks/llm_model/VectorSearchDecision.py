# Databricks notebook source
# MAGIC %pip install -U langgraph langsmith pydantic==2.9.2 databricks-vectorsearch mlflow mlflow-skinny langchain==0.3.3 langchain_core==0.3.12 langchain_community==0.3.2 databricks-sql-connector[sqlalchemy]==3.7.1 langchain-openai==0.2.2  

# COMMAND ----------

# Comment this out after running the pip  installs above.

dbutils.library.restartPython()

# COMMAND ----------

from pydantic import BaseModel, Field
from langchain_openai import AzureChatOpenAI
import getpass
import os

from langchain.prompts import ChatPromptTemplate

from typing import Annotated
import json
from typing_extensions import TypedDict
from langchain.output_parsers import PydanticOutputParser

from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch

from IPython.display import Image, display

import mlflow
from sqlalchemy import create_engine
from openai import OpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import (
    PromptTemplate,
    ChatPromptTemplate,
    MessagesPlaceholder,
)
from operator import itemgetter

from langchain_core.messages import HumanMessage, AIMessage
from langchain.agents import AgentExecutor, create_react_agent, Tool, load_tools, create_openai_tools_agent, create_tool_calling_agent
from langchain.tools import BaseTool
from langchain.tools.retriever import create_retriever_tool
from langchain_core.runnables import RunnableLambda, RunnablePassthrough

from langchain_community.chat_models import ChatDatabricks
from langchain_core.runnables import RunnablePassthrough

from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase

from mlflow.langchain.output_parsers import StringResponseOutputParser, ChatCompletionsOutputParser, ChatCompletionOutputParser
from langchain.agents.output_parsers.openai_tools import OpenAIToolsAgentOutputParser
from pyspark.sql import SparkSession

from datetime import datetime

from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentChunk, ChatAgentResponse, ChatContext
from langgraph.graph.state import CompiledStateGraph

from typing import Any, Generator, Optional

# COMMAND ----------

if "AZURE_OPENAI_API_KEY_PROD" not in os.environ:
    raise('Please set the environment variable "AZURE_OPENAI_API_KEY_PROD"')

# COMMAND ----------

# MAGIC %md
# MAGIC LLM for Decision

# COMMAND ----------

llm_model = AzureChatOpenAI(
    base_url="https://api.competence-cente-cc-genai-prod.A-az.cloud/openai/deployments/gpt-4o",
    openai_api_version="2024-06-01",
    api_key=os.environ["AZURE_OPENAI_API_KEY_PROD"],
    temperature=0.0, # 0 versuchen 0.2 vorher
    )

# COMMAND ----------

# Define the structured response model using Pydantic
class LLMResponse(BaseModel):
    decision: bool = Field(..., description="True für Ja, False für Nein als Antwort.")
    reason: str = Field(..., max_length=500, description="Kurze Begründung für die Entscheidung.")

# COMMAND ----------

llm_model = llm_model.with_structured_output(LLMResponse)

# COMMAND ----------

'''
tables = spark.catalog.listTables("datif_pz_uk_dev.04_icc_mart")

table_info = ""

for table in tables:
    table_info += f"Table: {table.name}\n"
    table_schema = spark.table(f"datif_pz_uk_dev.04_icc_mart.{table.name}").schema
    for field in table_schema.fields:
        table_info += f"  Column: {field.name}, Type: {field.dataType}\n"
    table_info += "\n" '''

# COMMAND ----------

system_prompt = f'''
Du bist ein KI-Modell, das entscheidet, ob eine zusätzliche Vektorsuche nach einer SQL-Abfrage durchgeführt werden sollte. Die SQL-Abfrage wird immer durchgeführt und von einem separaten Modell verwaltet. Deine Aufgabe ist es, zu entscheiden, ob zusätzlich eine Vektorsuche ausgeführt werden soll. Beachte dabei folgende Punkte:

#### 1. SQL-Abfragen: 
Diese werden immer automatisch durchgeführt und sind für die Beantwortung der meisten Fragen zuständig. SQL-Abfragen liefern exakte, strukturierte Daten aus den folgenden Tabellen:

Table: argus_online_media_panel
  Column: clipping_id, Type: StringType()
  Column: clipping_publication_date, Type: StringType()
  Column: clipping_deeplink, Type: StringType()
  Column: clipping_headline, Type: StringType()
  Column: clipping_subtitle, Type: StringType()
  Column: clipping_search_terms, Type: StringType()
  Column: clipping_teaser, Type: StringType()
  Column: clipping_abstract, Type: StringType()
  Column: clipping_order, Type: StringType()
  Column: tonalitaet, Type: StringType()
  Column: clipping_author, Type: StringType()
  Column: media_title, Type: StringType()
  Column: media_editorial, Type: StringType()
  Column: media_publisher, Type: StringType()
  Column: media_genre, Type: StringType()
  Column: media_type, Type: StringType()
  Column: media_sector, Type: StringType()
  Column: media_state, Type: StringType()
  Column: media_gross_reach, Type: LongType()
  Column: media_visits, Type: LongType()
  Column: media_page_views, Type: LongType()
  Column: media_unique_user, Type: LongType()

Table: argus_print_media_panel
  Column: clipping_id, Type: StringType()
  Column: clipping_publication_date, Type: StringType()
  Column: clipping_deeplink, Type: StringType()
  Column: clipping_headline, Type: StringType()
  Column: clipping_subtitle, Type: StringType()
  Column: clipping_search_terms, Type: StringType()
  Column: clipping_teaser, Type: StringType()
  Column: clipping_abstract, Type: StringType()
  Column: clipping_order, Type: StringType()
  Column: tonalitaet, Type: StringType()
  Column: clipping_author, Type: StringType()
  Column: media_title, Type: StringType()
  Column: media_editorial, Type: StringType()
  Column: media_publisher, Type: StringType()
  Column: media_genre, Type: StringType()
  Column: media_type, Type: StringType()
  Column: media_sector, Type: StringType()
  Column: media_state, Type: StringType()
  Column: media_gross_reach, Type: LongType()
  Column: media_print_run, Type: LongType()
  Column: media_copies_sold, Type: LongType()
  Column: media_copies_distributed, Type: LongType()
  Column: files_pdf_doc, Type: StringType()

Table: argus_social_listening_panel
  Column: clipping_id, Type: StringType()
  Column: clipping_publication_date, Type: StringType()
  Column: clipping_deeplink, Type: StringType()
  Column: clipping_headline, Type: StringType()
  Column: clipping_subtitle, Type: StringType()
  Column: clipping_search_terms, Type: StringType()
  Column: clipping_teaser, Type: StringType()
  Column: tonalitaet, Type: StringType()
  Column: clipping_author, Type: StringType()
  Column: clipping_language, Type: StringType()
  Column: media_title, Type: StringType()
  Column: media_editorial, Type: StringType()
  Column: media_publisher, Type: StringType()
  Column: media_genre, Type: StringType()
  Column: media_type, Type: StringType()
  Column: media_sector, Type: StringType()
  Column: medium_url, Type: StringType()
  Column: media_gross_reach, Type: LongType()

Table: facebook_organic_total
  Column: PostID, Type: StringType()
  Column: CreatedDate, Type: DateType()
  Column: PostMessage, Type: StringType()
  Column: PostURL, Type: StringType()
  Column: PostType, Type: StringType()
  Column: PostTotalImpressionsLifetime, Type: IntegerType()
  Column: TotalPostLikes, Type: IntegerType()
  Column: TotalLikeReactions, Type: IntegerType()
  Column: TotalAngerReactions, Type: IntegerType()
  Column: TotalhahaReactions, Type: IntegerType()
  Column: TotalLoveReactions, Type: IntegerType()
  Column: TotalSadReactions, Type: IntegerType()
  Column: TotalwowReactions, Type: IntegerType()
  Column: TotalPostShares, Type: IntegerType()
  Column: TotalPostComments, Type: IntegerType()
  Column: OrganicVideoViewsLifetime, Type: IntegerType()
  Column: LinkClicksLifetime, Type: IntegerType()
  Column: UniquePostCommentsLifetime, Type: IntegerType()
  Column: CLevelErwaehnungen, Type: StringType()
  Column: SubjectArea1, Type: StringType()
  Column: SubjectArea2, Type: StringType()
  Column: SubjectArea3, Type: StringType()
  Column: SubjectArea1_Confidence, Type: IntegerType()
  Column: SubjectArea2_Confidence, Type: IntegerType()
  Column: SubjectArea3_Confidence, Type: IntegerType()
  Column: Strategie2030, Type: BooleanType()
  Column: FinanzierungEnergiewende, Type: BooleanType()
  Column: EMobilitaet, Type: BooleanType()
  Column: Performancekultur, Type: BooleanType()
  Column: VernetzeEnergiewelt, Type: BooleanType()
  Column: Commodity, Type: BooleanType()
  Column: TransformationGasnetzeWasserstoff, Type: BooleanType()
  Column: ErneuerbareEnergien, Type: BooleanType()
  Column: DisponibleErzeugung, Type: BooleanType()
  Column: IntelligenteStromnetze, Type: BooleanType()
  Column: AAlsArbeitgeberIn, Type: BooleanType()
  Column: NachhaltigkeitCSRESG, Type: BooleanType()
  Column: MarkeA, Type: BooleanType()

Table: ga4_daily
  Column: Date, Type: DateType()
  Column: Session_campaign, Type: StringType()
  Column: First_user_campaign, Type: StringType()
  Column: Page_path, Type: StringType()
  Column: Page_path_query_string, Type: StringType()
  Column: Page_path_query_string_and_screen_class, Type: StringType()
  Column: Full_page_URL, Type: StringType()
  Column: Session_source__medium, Type: StringType()
  Column: Engaged_sessions, Type: IntegerType()
  Column: Sessions, Type: IntegerType()
  Column: Views, Type: IntegerType()
  Column: User_engagement, Type: IntegerType()
  Column: Total_session_duration, Type: IntegerType()
  Column: Bounce_rate, Type: DoubleType()
  Column: Average_session_duration, Type: DoubleType()
  Column: Views_per_session, Type: DoubleType()

Table: google_analytics_4_contents
  Column: dt_created, Type: TimestampType()
  Column: dt_updated, Type: TimestampType()
  Column: dt_filename, Type: StringType()
  Column: data_index, Type: StringType()
  Column: day, Type: DateType()
  Column: data_origin, Type: StringType()
  Column: account_id, Type: StringType()
  Column: account_name, Type: StringType()
  Column: property_id, Type: StringType()
  Column: channel, Type: StringType()
  Column: landingpage_type, Type: StringType()
  Column: landingpage_url, Type: StringType()
  Column: landingpage_path, Type: StringType()
  Column: utm_source, Type: StringType()
  Column: utm_campaign, Type: StringType()
  Column: utm_medium, Type: StringType()
  Column: utm_content, Type: StringType()
  Column: utm_term, Type: StringType()
  Column: sessions, Type: LongType()
  Column: engaged_sessions, Type: LongType()
  Column: bounces, Type: LongType()
  Column: page_views, Type: LongType()
  Column: session_starts, Type: LongType()
  Column: average_session_duration, Type: DecimalType(20,4)
  Column: qualified_visits_first_level, Type: LongType()
  Column: qualified_visits_second_level, Type: LongType()
  Column: qualified_visits_third_level, Type: LongType()
  Column: seite, Type: StringType()
  Column: unique_users, Type: LongType()

Table: instagram_organic_total
  Column: PostID, Type: StringType()
  Column: CreatedDate, Type: DateType()
  Column: PostMessage, Type: StringType()
  Column: PostURL, Type: StringType()
  Column: PostType, Type: StringType()
  Column: TotalImpressions, Type: IntegerType()
  Column: TotalLikes, Type: IntegerType()
  Column: TotalShares, Type: IntegerType()
  Column: TotalComments, Type: IntegerType()
  Column: TotalSaved, Type: IntegerType()
  Column: TotalInteractions, Type: IntegerType()
  Column: CLevelErwaehnungen, Type: StringType()
  Column: SubjectArea1, Type: StringType()
  Column: SubjectArea2, Type: StringType()
  Column: SubjectArea3, Type: StringType()
  Column: SubjectArea1_Confidence, Type: IntegerType()
  Column: SubjectArea2_Confidence, Type: IntegerType()
  Column: SubjectArea3_Confidence, Type: IntegerType()
  Column: Strategie2030, Type: BooleanType()
  Column: FinanzierungEnergiewende, Type: BooleanType()
  Column: EMobilitaet, Type: BooleanType()
  Column: Performancekultur, Type: BooleanType()
  Column: VernetzeEnergiewelt, Type: BooleanType()
  Column: Commodity, Type: BooleanType()
  Column: TransformationGasnetzeWasserstoff, Type: BooleanType()
  Column: ErneuerbareEnergien, Type: BooleanType()
  Column: DisponibleErzeugung, Type: BooleanType()
  Column: IntelligenteStromnetze, Type: BooleanType()
  Column: AAlsArbeitgeberIn, Type: BooleanType()
  Column: NachhaltigkeitCSRESG, Type: BooleanType()
  Column: MarkeA, Type: BooleanType()

Table: linkedin_organic_total
  Column: PostID, Type: StringType()
  Column: CreatedDate, Type: DateType()
  Column: PostTitle, Type: StringType()
  Column: PostContent, Type: StringType()
  Column: PostURL, Type: StringType()
  Column: ContentType, Type: StringType()
  Column: TotalImpressions, Type: IntegerType()
  Column: TotalLikes, Type: IntegerType()
  Column: TotalShares, Type: IntegerType()
  Column: TotalComments, Type: IntegerType()
  Column: TotalClicks, Type: IntegerType()
  Column: TotalCTR, Type: DoubleType()
  Column: CLevelErwaehnungen, Type: StringType()
  Column: SubjectArea1, Type: StringType()
  Column: SubjectArea2, Type: StringType()
  Column: SubjectArea3, Type: StringType()
  Column: SubjectArea1_Confidence, Type: IntegerType()
  Column: SubjectArea2_Confidence, Type: IntegerType()
  Column: SubjectArea3_Confidence, Type: IntegerType()
  Column: Strategie2030, Type: BooleanType()
  Column: FinanzierungEnergiewende, Type: BooleanType()
  Column: EMobilitaet, Type: BooleanType()
  Column: Performancekultur, Type: BooleanType()
  Column: VernetzeEnergiewelt, Type: BooleanType()
  Column: Commodity, Type: BooleanType()
  Column: TransformationGasnetzeWasserstoff, Type: BooleanType()
  Column: ErneuerbareEnergien, Type: BooleanType()
  Column: DisponibleErzeugung, Type: BooleanType()
  Column: IntelligenteStromnetze, Type: BooleanType()
  Column: AAlsArbeitgeberIn, Type: BooleanType()
  Column: NachhaltigkeitCSRESG, Type: BooleanType()
  Column: MarkeA, Type: BooleanType()

Table: linkedin_organic_video_total
  Column: VideoID, Type: StringType()
  Column: CreatedDate, Type: DateType()
  Column: FirstPublishTime, Type: TimestampType()
  Column: VideoText, Type: StringType()
  Column: PostURL, Type: StringType()
  Column: ContentType, Type: StringType()
  Column: Author, Type: IntegerType()
  Column: Origin, Type: IntegerType()
  Column: Visibility, Type: IntegerType()
  Column: TotalImpressions, Type: IntegerType()
  Column: TotalLikes, Type: IntegerType()
  Column: TotalShares, Type: IntegerType()
  Column: TotalComments, Type: IntegerType()
  Column: TotalViews, Type: IntegerType()
  Column: TotalClicks, Type: IntegerType()
  Column: TotalViewers, Type: IntegerType()
  Column: TotalTimeWatchedForVideoViews, Type: DoubleType()
  Column: TotalTimeWatched, Type: DoubleType()
  Column: CLevelErwaehnungen, Type: StringType()
  Column: SubjectArea1, Type: StringType()
  Column: SubjectArea2, Type: StringType()
  Column: SubjectArea3, Type: StringType()
  Column: SubjectArea1_Confidence, Type: IntegerType()
  Column: SubjectArea2_Confidence, Type: IntegerType()
  Column: SubjectArea3_Confidence, Type: IntegerType()
  Column: Strategie2030, Type: BooleanType()
  Column: FinanzierungEnergiewende, Type: BooleanType()
  Column: EMobilitaet, Type: BooleanType()
  Column: Performancekultur, Type: BooleanType()
  Column: VernetzeEnergiewelt, Type: BooleanType()
  Column: Commodity, Type: BooleanType()
  Column: TransformationGasnetzeWasserstoff, Type: BooleanType()
  Column: ErneuerbareEnergien, Type: BooleanType()
  Column: DisponibleErzeugung, Type: BooleanType()
  Column: IntelligenteStromnetze, Type: BooleanType()
  Column: AAlsArbeitgeberIn, Type: BooleanType()
  Column: NachhaltigkeitCSRESG, Type: BooleanType()
  Column: MarkeA, Type: BooleanType()

Table: marktforschung
  Column: Breite_Öffentlichkeit_BaWü, Type: StringType()
  Column: Jan_23, Type: IntegerType()
  Column: Feb_23, Type: IntegerType()
  Column: Mrz_23, Type: IntegerType()
  Column: Apr_23, Type: IntegerType()
  Column: Mai_23, Type: IntegerType()
  Column: Jun_23, Type: IntegerType()
  Column: Jul_23, Type: IntegerType()
  Column: Aug_23, Type: IntegerType()
  Column: Sep_23, Type: IntegerType()
  Column: Okt_23, Type: IntegerType()
  Column: Nov_23, Type: IntegerType()
  Column: Dez_23, Type: IntegerType()
  Column: Jan_24, Type: IntegerType()
  Column: Feb_24, Type: IntegerType()
  Column: Mrz_24, Type: IntegerType()
  Column: Apr_24, Type: IntegerType()
  Column: Mai_24, Type: IntegerType()
  Column: Jun_24, Type: IntegerType()

Table: regionalschlussel
  Column: Regionalschlüssel, Type: LongType()
  Column:  Gemeinde, Type: StringType()

Table: soziodemografie_facebook
  Column: Kanal, Type: StringType()
  Column: Alterssegment, Type: StringType()
  Column: Nutzerverteilung, Type: DoubleType()
  Column: Nutzeranzahl_Mio, Type: DoubleType()

Table: soziodemografie_gender
  Column: Kanal, Type: StringType()
  Column: Männlich, Type: LongType()
  Column: Weiblich, Type: LongType()
  Column: Divers, Type: LongType()

Table: soziodemografie_linkedin
  Column: Kanal, Type: StringType()
  Column: Alterssegment, Type: StringType()
  Column: Nutzerverteilung, Type: DoubleType()
  Column: Nutzeranzahl_Mio, Type: DoubleType()

Table: soziodemografie_x
  Column: Kanal, Type: StringType()
  Column: Alterssegment, Type: StringType()
  Column: Nutzerverteilung, Type: DoubleType()
  Column: Nutzeranzahl_Mio, Type: DoubleType()

Table: x_organic_total
  Column: PostID, Type: StringType()
  Column: CreatedDate, Type: DateType()
  Column: InReplyToStatusID, Type: StringType()
  Column: PostMessage, Type: StringType()
  Column: PostURL, Type: StringType()
  Column: PostType, Type: StringType()
  Column: TotalImpressions, Type: IntegerType()
  Column: TotalLikes, Type: IntegerType()
  Column: TotalReposts, Type: IntegerType()
  Column: TotalReplies, Type: IntegerType()
  Column: TotalEngagements, Type: IntegerType()
  Column: TotalFollows, Type: IntegerType()
  Column: TotalClicks, Type: IntegerType()
  Column: TotalLinkClicks, Type: IntegerType()
  Column: TotalAppClicks, Type: IntegerType()
  Column: TotalCardEngagements, Type: IntegerType()
  Column: CLevelErwaehnungen, Type: StringType()
  Column: SubjectArea1, Type: StringType()
  Column: SubjectArea2, Type: StringType()
  Column: SubjectArea3, Type: StringType()
  Column: SubjectArea1_Confidence, Type: IntegerType()
  Column: SubjectArea2_Confidence, Type: IntegerType()
  Column: SubjectArea3_Confidence, Type: IntegerType()
  Column: Strategie2030, Type: BooleanType()
  Column: FinanzierungEnergiewende, Type: BooleanType()
  Column: EMobilitaet, Type: BooleanType()
  Column: Performancekultur, Type: BooleanType()
  Column: VernetzeEnergiewelt, Type: BooleanType()
  Column: Commodity, Type: BooleanType()
  Column: TransformationGasnetzeWasserstoff, Type: BooleanType()
  Column: ErneuerbareEnergien, Type: BooleanType()
  Column: DisponibleErzeugung, Type: BooleanType()
  Column: IntelligenteStromnetze, Type: BooleanType()
  Column: AAlsArbeitgeberIn, Type: BooleanType()
  Column: NachhaltigkeitCSRESG, Type: BooleanType()
  Column: MarkeA, Type: BooleanType()



Sie bieten Antworten auf konkrete Datenanfragen.

#### 2. Vektorsuche: 
Eine Vektorsuche wird benötigt, wenn die Antwort auf eine Frage auf der Suche nach ähnlichen Inhalten basiert. Zum Beispiel: Wenn der Nutzer nach einem ähnlichen Begriff oder nach inhaltlich verwandten Informationen fragt, die durch semantische Ähnlichkeit erfasst werden müssen.

#### 3. Wann Vektorsuche sinnvoll ist:
- Wenn der Nutzer nach einer Ähnlichkeit von Begriffen oder Konzepten fragt (z.B. ähnliche Inhalte oder bestimmte Begriffe).
- Bei Anfragen, die eine tiefergehende semantische Suche benötigen, wie bei der Suche nach verwandten Konzepten, obwohl der exakte Suchbegriff nicht vorhanden ist.
- Bei Fragen zu freien Texten oder Metadaten, bei denen semantische Ähnlichkeiten zwischen unterschiedlichen Begriffen erfasst werden müssen.

#### 4. Wann keine Vektorsuche sinnvoll ist:
- Bei der Suche nach exakt übereinstimmenden Daten, wie z.B. exakten IDs oder anderen eindeutigen Datenpunkten.
- Wenn die Anfrage nur eine exakte Übereinstimmung oder einfache Datenabfragen über SQL erfordert, ohne dass semantische Ähnlichkeit notwendig ist.

Bitte überprüfe, ob für die Anfrage eine Vektorsuche sinnvoll ist, basierend auf der Art der Frage und den verfügbaren Daten in diesen Tabellen.
'''

prompt_decision = ChatPromptTemplate.from_messages([("system", system_prompt),("placeholder", "{chat_history}"),
("human", "{question}"),
("placeholder", "{agent_scratchpad}"),
])

# COMMAND ----------

llm_with_decision_prompt = prompt_decision | llm_model

# COMMAND ----------

class State(TypedDict):
    messages: Annotated[list, add_messages]
    decision: bool
    reason= str
    processed_query: str
    react_chat_history: Annotated[list, add_messages]

graph_builder = StateGraph(State)

# COMMAND ----------

# MAGIC %md
# MAGIC LLM Decision Node

# COMMAND ----------

def chatbot(state: State):
    messages = state["messages"]
    query = state["messages"][-1].content
    response = llm_with_decision_prompt.invoke(messages)
    try:
    
        #test = parser.parse(response_content)
        return {
            "messages": [{"role": "assistant", "content": f"{response}"}],
            "decision": response.decision,
            "reason": response.reason,
            #"processed_query": None,
            #"react_chat_history": [{"role": "user", "content": query}],
        }
    except Exception as e:
        return {"messages": [{"role": "assistant", "content": f"Fehler: {str(e)}"}]}


# COMMAND ----------

# MAGIC %md
# MAGIC VectorSearch Node

# COMMAND ----------

def should_execute_vector_search(state: State):
    decision = state.get("decision", False)
    return decision

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

index = vsc.get_index(endpoint_name="vector-search-endpoint" , index_name="datif_pz_uk_dev.03_served.consolidated_socials_vector")

def vector_search(state: State):
    query = state["messages"][-2].content
    results = index.similarity_search(query_text = query, num_results=10,
    columns=[
        "social_post_id",
        "channel",
    ],)
    # Debugging: Ausgabe der Ergebnisse vor der Rückgabe
    response = "\n".join([f"{result[1]}: {result[0]}" for result in results["result"]["data_array"]])
    processed_query = f"{query} Verwende nur diese Inhalte zum beantworten und stelle keine WHERE Bedingung mit LIKE: {response}"
    return {
        "messages": [{"role": "assistant", "content": f"Vektorsuche Ergebnisse:\n{response}"}],
        "processed_query": processed_query,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC LLM SQl

# COMMAND ----------

# MAGIC %md
# MAGIC Return muss bei vector node die neue query inklusive id's sein und bei der LLM Node soll auch die user query übergeben werden. und nicht die Antwort des LLM. Dann kann man das bei extract user query einfach verwenden. sonst ist es einmal messages[-2] und einmal messages von [-3 bzw -1]
# MAGIC

# COMMAND ----------

model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")

# COMMAND ----------

prompt_sql = ChatPromptTemplate.from_messages([("system", '''
Du bist ein Datenanalyst in der Kommunikationsabteilung der deutschen Energiefirma A. Deine Aufgabe ist es, die Leistung und Wirkung der Kommunikationsmaßnahmen datenbasiert zu bewerten und zu veranschaulichen. Nutze geeignete Tools und präzise Abfragen, um relevante Einblicke zu liefern.

### Anforderungen

#### 1. Allgemeine Vorgaben
- Alle Antworten basieren ausschließlich auf verfügbaren, überprüfbaren Daten.
- Zahlen sind präzise anzugeben, keine Rundungen.
- Wenn keine Daten verfügbar sind: "Es liegen keine Daten vor, um diese Frage zu beantworten."
- Antworten müssen klar, prägnant und geschäftlich formuliert sein.

#### 2. Format der Antwort
- **Fazit/Gesamteinschätzung**: Beginne jede Antwort mit einer Einordnung in den Gesamtkontext und einer kurzen Zusammenfassung der wichtigsten Erkenntnisse. Liefere dabei eine prägnante Bewertung.  
- **Details**: Gehe anschließend detailliert auf die Analyse ein.  
- Erkläre auffällige Trends oder Entwicklungen.  
- Vergleiche relevante Plattformen mit konkreten Zahlen.  
- Erläutere prozentuale Veränderungen oder bedeutende Kennzahlen im Zusammenhang.  
- Gib stets eine Gesamteinschätzung, falls mehrere Plattformen betrachtet werden, oder erwähne explizit, wenn nur eine Plattform relevant ist.

#### 3. Zahlen und Erläuterungen
- Zahlen müssen detailliert erläutert werden.  
- Gib bei Reichweitenanalysen an, wie diese sich zusammensetzen (z. B.: "Reichweite 50.000 aus 20 Posts").  

#### 4. Zeitangaben
- Jede Antwort muss den Zeitraum der Daten benennen (z. B. "August 2024", "Q3 2024").
- Das heutige Datum ist {date}.

#### 5. Printmedien-Spezifika
- Reichweite und verkaufte Exemplare müssen präzise angegeben werden (z. B.: "Reichweite 50.000, verkaufte Exemplare 30.000").

#### 6. Empirisches Arbeiten und Quellenangaben
- Gib für alle erwähnten Posts, Artikel oder Beiträge immer eine verifizierte Quelle an, sofern verfügbar.  
- Verwende das Format: *"Titel des Beitrags", Datum, [Link].*    
- Stelle sicher, dass alle Links überprüfbar sind und auf die analysierten Inhalte verweisen.

#### 7. SQL-Abfragen
- Erstelle SQL-Abfragen nur mit Spalten, die zur Beantwortung der Frage relevant sind.

#### 8. Konsistenz
- Nutze den bisherigen Chatverlauf nur, wenn die Nutzerfragen in einem direkten Zusammenhang stehen, um Antworten konsistent zu halten und Zusammenhänge herzustellen. Wenn kein Zusammenhang besteht, beantworte die Anfrage unabhängig vom bisherigen Verlauf.

### 9. Themenbereiche sind:
- Elektromobilität
- Erneuerbare Energien / Photovoltaik
- Erneuerbare Energien / Geothermie/Pumpspeicherkraftwerke/ Wasserkraft
- Erneuerbare Energien / Windkraft auf See
- Erneuerbare Energien / Windkraft an Land
- Finanzen/M&A
- Innovation, Forschung, Entwicklung
- Kernenergie
- Konventionelle Erzeugung (das sind konventionelle Methoden, die durch die Energiewende abgelöst werden)
- Nachhaltigkeit/Umweltschutz
- Netze
- Personal
- Vertrieb


### 10. Strategische Themen:
Die Folgenden Strategischen Themen gibt es:
- Strategie2030
- FinanzierbarkeitEnergiewende
- Finanzmarktattraktivitaet
- Ladeinfrastruktur
- Unternehmenskultur
- Commodity
- TransformationGasnetzeWasserstoff
- BeschleunigungAusbauEE
- AusbauDisponibleLeistung
- AusUmbauIntelligenteStromnetze
- Arbeitgeberattraktivitaet
- Portfoliomanagement
Die Werte geben an, wie sehr der Post zu dem Thema beiträgt.
'''),
("placeholder", "{chat_history}"),
("human", "{question}"),
("placeholder", "{agent_scratchpad}"),
])



# COMMAND ----------

def extract_user_query_string(state: State):
    query = state.get("processed_query", state["messages"][-2].content)
    return { "input": query}

def extract_chat_history(state: State):
    return state.get("react_chat_history", [])

def get_date(*args):
    current_date = datetime.now()
    formatted_date = current_date.strftime('%d-%m-%Y')
    return formatted_date

def get_output_string(output):
    s1 = "Final Answer:"
    s2 = "Endgültige Antwort:"
    output = output.get("output")
    if s1 in output and False:
        final_answer = output.find(s1)
        return output[final_answer + len(s1) + 1:].strip()
    if s2 in output and False:
        final_answer = output.find(s2)
        return output[final_answer + len(s2) + 1:].strip()
    return output

# COMMAND ----------

mlflow.langchain.autolog()

# COMMAND ----------

def sql_runnable(input):
    """ create sql agent """

    llm_model = AzureChatOpenAI(
        base_url="https://api.competence-cente-cc-genai-prod.A-az.cloud/openai/deployments/gpt-4o",
        openai_api_version="2024-06-01",
        api_key=os.environ["AZURE_OPENAI_API_KEY_PROD"],
        temperature=0.0, # 0 versuchen 0.2 vorher
        )
    db = SQLDatabase.from_databricks(
        catalog="datif_pz_uk_dev",
        schema="04_icc_mart",
        host=model_config.get("host"),
        warehouse_id=model_config.get("warehouse_id"),
        # include_tables=[
        #     'facebook_organic_total', 'instagram_organic_total', 'linkedin_organic_total', 'linkedin_organic_video_total', 'x_organic_total', 
        #     'argus_print_media_panel', 'argus_online_media_panel', 'argus_social_listening_panel',
        # ],
        ignore_tables=['google_analytics_4_contents', 'argus_social_listening_panel']
    )
    toolkit = SQLDatabaseToolkit(db=db, llm=llm_model)
    tools=[ *toolkit.get_tools() ]
    agent = create_tool_calling_agent(llm=llm_model, tools=tools, prompt=prompt_sql)
    agent_executor = AgentExecutor(agent=agent,  tools=tools, verbose=True, handle_parsing_errors=True)
    return agent_executor

# COMMAND ----------

react_chain = ({
        "question": RunnableLambda(extract_user_query_string),
        "chat_history": RunnableLambda(extract_chat_history),
        "date": RunnableLambda(get_date),
    }
    | RunnablePassthrough()
    | RunnableLambda(sql_runnable)
    | RunnableLambda(get_output_string)
    | ChatCompletionOutputParser() 
)

# COMMAND ----------

def sql_search(state: State):
    try:
        query = state.get("processed_query", state["messages"][-2].content)
        result = react_chain.invoke(
        state)
        return {"messages": [{"role": "assistant", "content": f"{result}"}],
                "react_chat_history": [{"role": "assistant", "content": f"{query}"},{"role": "assistant", "content": f"{result}"}],
                }
    except Exception as e:
        return {"messages": [{"role": "assistant", "content": f"Fehler: {str(e)}"}]}

# COMMAND ----------

# MAGIC %md
# MAGIC Build Graph

# COMMAND ----------

graph_builder.add_node("chatbot", chatbot)
graph_builder.add_node("vector_search", vector_search)
graph_builder.add_node("sql_search", sql_search)
graph_builder.add_edge(START, "chatbot")
graph_builder.add_conditional_edges(
    "chatbot",
    should_execute_vector_search,
    {
        True: "vector_search",
        False: "sql_search",
    }
)
graph_builder.add_edge("vector_search", "sql_search")
graph_builder.add_edge("sql_search", END)
graph = graph_builder.compile()

# COMMAND ----------

class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {"messages": [msg.__dict__ for msg in messages]}  # oder eine passende Konvertierung
        collected_messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                collected_messages.extend(
                    [ChatAgentMessage(**msg) for msg in node_data.get("messages", [])]
                )
        return ChatAgentResponse(messages=collected_messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": [msg.__dict__ for msg in messages]}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    yield ChatAgentChunk(delta=msg)

# COMMAND ----------

AGENT = LangGraphChatAgent(graph)
mlflow.models.set_model(AGENT)

# COMMAND ----------

'''
try:
    display(Image(graph.get_graph().draw_mermaid_png()))
except Exception:
    pass
'''

# COMMAND ----------

# MAGIC %md
# MAGIC Run Graph

# COMMAND ----------


input_example = {
    "messages": [
        # {"role": "user", "content": "Wie war die Kommunikation im Oktober?"}
        # {"role": "user", "content": "Wie war die Kommunikation auf Twitter im Oktober?"}
        # {"role": "user", "content": "Wie gut war die Kommunikation im Vergleich von Juli und August?"}
        # {"role": "user", "content": "Wie gut sind die Facebook Posts von heute?"}
        # {"role": "user", "content": "Wie gut waren die Facebook Posts im letzten Monat?"}
        # {"role": "user", "content": "Welche Posts waren im August 2024 besonders gut? Wie sind sie im Vergleich zu Juli?"}
        # {"role": "user", "content": "Wie gut ist die Kommunikation in diesem Monat?"}

        # {'role': 'user', "content": "Welche Themen generieren generell die höchste Interaktionen [Likes, Kommentare, Shares] auf unseren Social-Media-Kanälen und Reichweiten Print?"}
        #{'role': 'user', "content": "Wie hat sich die Gesamtreichweite unserer Inhalte auf den verschiedenen Plattformen im Vergleich zum Vorjahr entwickelt?"}
        # {'role': 'user', "content": "Wie war die Performance auf Social Media im Juli 2024?"}
        # {'role': 'user', "content": "Wie erfolgreich war die Kommunikation zum Macherbus im Jahr 2024?"}
        {'role': 'user', 'content': 'Wie verlief die Kommunikation in der bisherigen Kalenderwoche?'},
        # {'role': 'user', 'content': 'Wie verlief die Kommunikation gestern?'},
 
 
    ]
}
'''
for event in graph.stream(input_example):
            for value in event.values():
                print("Assistant:", value["messages"][-1]["content"])
'''
