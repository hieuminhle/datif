# Databricks notebook source
# MAGIC %pip install -U -qqqq pydantic==2.9.2 databricks-agents mlflow mlflow-skinny databricks-vectorsearch langchain==0.3.3 langchain_core==0.3.12 langchain_community==0.3.2 databricks-sql-connector[sqlalchemy]==3.7.1 langgraph langchain-openai==0.2.2

# COMMAND ----------

# Comment this out after running the pip  installs above.

# dbutils.library.restartPython()


# COMMAND ----------

# %run ../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC Import-Section:

# COMMAND ----------

import os
from operator import itemgetter
from openai import OpenAI
import mlflow
from sqlalchemy import create_engine

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import (
    PromptTemplate,
    ChatPromptTemplate,
    MessagesPlaceholder,
)

from langchain_core.messages import HumanMessage, AIMessage
from langchain.agents import AgentExecutor, create_react_agent, Tool, load_tools, create_openai_tools_agent, create_tool_calling_agent
from langchain.tools import BaseTool
from langchain.tools.retriever import create_retriever_tool

from langchain_openai import AzureChatOpenAI
from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch

from langchain_core.runnables import RunnableLambda, RunnablePassthrough

from langchain_community.chat_models import ChatDatabricks
from langchain_core.runnables import RunnablePassthrough

from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase

from mlflow.langchain.output_parsers import StringResponseOutputParser, ChatCompletionsOutputParser
from langchain.agents.output_parsers.openai_tools import OpenAIToolsAgentOutputParser
from pyspark.sql import SparkSession

from datetime import datetime

# COMMAND ----------

# UC_CATALOG = get_secret('dbw-pz-catalog')

# COMMAND ----------

# MAGIC %md
# MAGIC Create LLM:

# COMMAND ----------

model_config = mlflow.models.ModelConfig(development_config="rag_chain_config.yaml")

# COMMAND ----------

if "AZURE_OPENAI_API_KEY_PROD" not in os.environ:
    # os.environ["AZURE_OPENAI_API_KEY_PROD"] = ''
    raise Exception('Please set the environment variable "AZURE_OPENAI_API_KEY_PROD"')

# COMMAND ----------

prompt = ChatPromptTemplate.from_messages([("system", '''
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

# MAGIC %md
# MAGIC Tools:

# COMMAND ----------

def clean_sql_query(query):
    query = query.replace('```','').replace('sql','')
    return query.strip()

query_cleaner = Tool(
    name="query_cleaner",
    description="Clean up the SQL query; always use it if the query checker indicates that the created query is incorrect!",
    func=clean_sql_query,
)

# COMMAND ----------

# vsc = VectorSearchClient(disable_notice=True)

# index = vsc.get_index(endpoint_name="vector-search-endpoint" , index_name="datif_pz_uk_dev.03_served.consolidated_socials_vector")

# vector_search_as_retriever = DatabricksVectorSearch(
#     index,
#     text_column="post_text",
#     columns=[
#         "day",
#         "channel",
#         "post_text",
#         "post_url",
#     ],
# ).as_retriever(search_kwargs={"k":10})
    
# retriever_tool = create_retriever_tool(
#     vector_search_as_retriever,
#     "dbs_vector_tool",
#     "Use this tool when answering general knowledge queries to get more information about the topic related to social media data.",
# )

# COMMAND ----------

# MAGIC %md
# MAGIC Functions:

# COMMAND ----------

def extract_user_query_string(chat_messages_array):
    return { "input": chat_messages_array[-1]["content"] }

def extract_chat_history(chat_messages_array):
    chat_history = []
    for message in chat_messages_array:
        if message['role'] == 'user':
            chat_history.append(HumanMessage(content=message['content']))
        elif message['role'] == 'assistant':
            chat_history.append(AIMessage(content=message['content']))
    return chat_history

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

# MAGIC %md
# MAGIC Run Agent:

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
        catalog=model_config.get("dbw-pz-catalog"),
        schema="04_icc_mart",
        host=model_config.get("host"),
        warehouse_id=model_config.get("warehouse_id"),
        # include_tables=[
        #     'facebook_organic_total', 'instagram_organic_total', 'linkedin_organic_total', 'linkedin_organic_video_total', 'x_organic_total', 
        #     'argus_print_media_panel', 'argus_online_media_panel', 'argus_social_listening_panel',
        # ],
        # ignore_tables=['google_analytics_4_contents', 'argus_social_listening_panel']
    )
    toolkit = SQLDatabaseToolkit(db=db, llm=llm_model)
    tools=[ *toolkit.get_tools() ]
    agent = create_tool_calling_agent(llm=llm_model, tools=tools, prompt=prompt)
    agent_executor = AgentExecutor(agent=agent,  tools=tools, verbose=True, handle_parsing_errors=True)
    return agent_executor

# COMMAND ----------

react_chain = ({
        "question": itemgetter("messages") | RunnableLambda(extract_user_query_string),
        "chat_history": itemgetter("messages") | RunnableLambda(extract_chat_history),
        "date": RunnableLambda(get_date),
    }
    | RunnablePassthrough()
    | RunnableLambda(sql_runnable)
    | RunnableLambda(get_output_string)
    | StringResponseOutputParser() 
)
mlflow.models.set_model(model=react_chain)

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

# output = react_chain.invoke(input_example)

# COMMAND ----------


