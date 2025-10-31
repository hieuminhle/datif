# Databricks notebook source
from openai import OpenAI
import os

# COMMAND ----------

# MAGIC %run ../common/nb_init

# COMMAND ----------

# MAGIC %run ../common/nb_tagging_functions

# COMMAND ----------

target_schema_name = "04_ai"
target_path = "prepared_data"

databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

# COMMAND ----------

"""
configure the questions, that should be pre-answered.
 - "ID": id for the prepared question
 - "Question": the question that should be pre-answered
 - "ShortName": the short name for the question, that is displayed in the UI
 - "Icon": the icon for the question, that is displayed in the UI
 - "Order": the order of the question in the UI (only relevant, if "StarterButton" is true)
 - "StarterButton": if true, the question will be displayed as a starter button

 notice the 'Themenbereich' and 'Strategische Themen' selection in the webapp. the first three are general questions, the rest are specific to the 'Themenbereich' or 'Strategische Themen' selection
"""

strategic_tags = [
    "Strategie2030",
    "FinanzierungEnergiewende",
    "EMobilitaet",
    "Performancekultur",
    "VernetzeEnergiewelt",
    "TransformationGasnetzeWasserstoff",
    "ErneuerbareEnergien",
    "DisponibleErzeugung",
    "IntelligenteStromnetze",
    "AAlsArbeitgeberIn",
    "NachhaltigkeitCSRESG",
    "MarkeA",
]

questions = [
    {"ID": "Yesterday", "Question": "Wie lief meine Kommunikation gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": 0, "StarterButton": True},
    {"ID": "LastWeek", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": 2, "StarterButton": True},
    {"ID": "ThisWeek", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": 1, "StarterButton": True},

    # Themenbereich
    # Yesterday
    {"ID": "YesterdayEMobility", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Elektromobilität gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayErneuerbareEnergien", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Erneuerbare Energien gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayFinanzenMA", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Finanzen / M&A gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayInnoForschEntw", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Innovation, Forschung, Entwicklung gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayKernenergie", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Kernenergie gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayKonventionelleEnergie", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Konventionelle Energie gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayNachhaltigkeitUmweltschutz", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Nachhaltigkeit / Umweltschutz gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayNetze", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Netze gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayPersonal", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Personal gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},
    {"ID": "YesterdayVertrieb", "Question": "Wie lief meine Kommunikation in Bezug auf den Themenbereich Vertrieb gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False},

    # ThisWeek
    {"ID": "ThisWeekEMobility", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Elektromobilität?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekErneuerbareEnergien", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Erneuerbare Energien?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekFinanzenMA", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Finanzen / M&A?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekInnoForschEntw", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Innovation, Forschung, Entwicklung?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekKernenergie", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Kernenergie?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekKonventionelleEnergie", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Konventionelle Energie?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekNachhaltigkeitUmweltschutz", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Nachhaltigkeit / Umweltschutz?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekNetze", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Netze?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekPersonal", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Personal?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    {"ID": "ThisWeekVertrieb", "Question": "Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Vertrieb?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False},
    
    # LastWeek
    {"ID": "LastWeekEMobility", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Elektromobilität?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekErneuerbareEnergien", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Erneuerbare Energien?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekFinanzenMA", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Finanzen / M&A?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekInnoForschEntw", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Innovation, Forschung, Entwicklung?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekKernenergie", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Kernenergie?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekKonventionelleEnergie", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Konventionelle Energie?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekNachhaltigkeitUmweltschutz", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Nachhaltigkeit / Umweltschutz?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekNetze", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Netze?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekPersonal", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Personal?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},
    {"ID": "LastWeekVertrieb", "Question": "Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf den Themenbereich Vertrieb?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False},

    # Strategische Themen
    # Yesterday
    *[
        {"ID": f"YesterdayStrategic{t}", "Question": f"Wie lief meine Kommunikation in Bezug auf das strategische Thema {t} gestern?", "ShortName": "Kommunikation gestern", "Icon": "./public/yesterday.png", "Order": -1, "StarterButton": False} for t in strategic_tags
    ],

    # ThisWeek
    *[
        {"ID": f"ThisWeekStrategic{t}", "Question": f"Wie verlief die Kommunikation in der bisherigen Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf das strategische Thema {t}?", "ShortName": "Kommunikation bisherige Woche", "Icon": "./public/thisweek.png", "Order": -1, "StarterButton": False} for t in strategic_tags
    ],

    # LastWeek
    *[
        {"ID": f"LastWeekStrategic{t}", "Question": f"Wie verlief die Kommunikation in der letzten Kalenderwoche in Print-, Online und auf den Social Media Kanälen in Bezug auf das strategische Thema {t}?", "ShortName": "Kommunikation letzte Woche", "Icon": "./public/lastweek.png", "Order": -1, "StarterButton": False} for t in strategic_tags
    ],
]


# COMMAND ----------

def query_model(question):
    """
    queries the uk chatbot model rest api
    """
    client = OpenAI(
        api_key=databricks_token,
        base_url=f"https://{workspace_url}/serving-endpoints"
    )
    return client.chat.completions.create(
        model=f"agents_datif_pz_uk_{env}-04_ai-uk_chatbot",
        messages=[
            {
                "role": "user",
                "content": question
            }
        ]
    )

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

def answer_question(column_value):
    """
    queries the uk chatbot model rest api
    retries three times, if an error occures
    """
    MAX_RETRIES = 3
    for retry in range(MAX_RETRIES):
        response = query_model(column_value)
        if response is not None:
            return response.content
    return None

def answer_question_partition(partition):
    """
    handles one partition by querying the model for each row
    """ 
    for row in partition:
        yield (row.ID, answer_question(row.Question), )

# answer_question_udf = udf(answer_question, StringType())

df_questions = spark.createDataFrame(questions).select("ID", "ShortName", "Question", "Icon", "Order", "StarterButton")
df_questions = df_questions.repartition(4).cache() # limit parallelism, otherwise openai token limit will be reached

answers = df_questions.rdd.mapPartitions(answer_question_partition)
df_answers = answers.toDF(["ID", "Answer"])
df_questions = df_questions.join(df_answers, "ID")

# df_questions = df_questions.withColumn("Answer", answer_question_udf("Question"))
# df_questions.display()

# COMMAND ----------

# the complete table will be overwritten
OVERWRITE = True
if OVERWRITE:
    fn_overwrite_table(df_questions, target_schema_name, "prepared_questions", target_path)
# else:
#     fn_create_or_upsert_table(df_source=df_questions,target_path=target_path,target_database_name=target_schema_name,target_table_name="prepared_questions",primary_key=["ID"])

# COMMAND ----------


