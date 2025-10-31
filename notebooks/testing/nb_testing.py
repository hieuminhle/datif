# Databricks notebook source
# MAGIC %md
# MAGIC Work in Progress.
# MAGIC
# MAGIC Currently we simply generate answers to test questions multiple times to manually inspect the output.

# COMMAND ----------

# MAGIC %pip uninstall -y mlflow mlflow-skinny
# MAGIC %pip install -U -qqqq databricks-agents mlflow mlflow-skinny databricks-vectorsearch databricks-sdk langchain==0.3.3 langchain_core==0.3.12 langchain_community==0.3.2 typing-extensions databricks-sql-connector[sqlalchemy] langchain-openai==0.2.2 textstat
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../common/nb_init

# COMMAND ----------

import os
import json
import requests

import numpy as np
import pandas as pd

import openai
import mlflow

from langchain_openai import AzureChatOpenAI
from mlflow.deployments import set_deployments_target, get_deploy_client

# COMMAND ----------

t = [{"question":"Welche Formate (Video, Bild, etc.) erzielten die höchste Reichweite und Interaktion in den letzten 3 Monaten?","ground_truth":"In den letzten 3 Monaten (05.06.2024 – 05.09.2024) erzielten die verschiedenen Medienformate auf den Social-Media-Kanälen von EnBW folgende Reichweiten und Interaktionen: - Videos hatten die höchste Reichweite mit 591.572 Impressionen und 8.382 Interaktionen. Dieses Format performte sowohl in Bezug auf Reichweite als auch Interaktion am besten. - Links erzielten 379.321 Impressionen und 6.484 Interaktionen, was sie zum zweitbesten Format in beiden Kategorien macht. - Fotos erreichten 103.233 Impressionen und 3.423 Interaktionen. Interessanterweise schnitten Formate wie Tweets (24.040 Impressionen, 546 Interaktionen) und Alben (13.909 Impressionen, 203 Interaktionen) deutlich schlechter ab. Das Karussellalbum erzielte zwar 4.248 Impressionen, jedoch keine Interaktionen. Zusammenfassend zeigt sich, dass Videos auf Social Media-Plattformen von EnBW derzeit das effektivste Format sind, sowohl in Bezug auf Reichweite als auch Engagement.","answer":""},
{"question":"Wie hat sich die Anzahl der Kommentare auf meine Posts im Vergleich zu früheren Perioden (1. Quartal 2023, 1. Quartal 2024, 2. Quartal 2024) verändert?","ground_truth":"In den letzten drei betrachteten Perioden zeigt sich eine deutliche Steigerung in der Anzahl der Kommentare auf den Social-Media-Posts von EnBW: 1. Quartal 2023: Es wurden 740 Kommentare verzeichnet. 1. Quartal 2024: Die Anzahl der Kommentare stieg auf 1.400, was einem Anstieg von etwa 89% im Vergleich zum 1. Quartal 2023 entspricht. 2. Quartal 2024: Die Kommentare nahmen weiter zu und erreichten 3.243 Kommentare, was eine zusätzliche Steigerung von 131% im Vergleich zum 1. Quartal 2024 darstellt. Diese positive Entwicklung deutet darauf hin, dass die Interaktion mit den Posts, insbesondere in Form von Kommentaren, im Laufe des Jahres 2024 stark zugenommen hat.","answer":""},
{"question":"Wie hoch ist die Interaktionsrate (Kommentare, Likes, Shares) meiner Posts im August 2024 im Vergleich zur durchschnittlichen Rate auf den jeweiligen Kanälen?","ground_truth":"Im August 2024 zeigt sich, dass die Interaktionsrate deiner Social-Media-Posts im Vergleich zur durchschnittlichen Rate auf den jeweiligen Kanälen stark variiert: LinkedIn sticht mit einer überdurchschnittlichen Performance hervor, da die Interaktionsrate 110,85% über dem Durchschnitt liegt. Dies bedeutet, dass LinkedIn in diesem Zeitraum besonders gut abgeschnitten hat und die Nutzer deutlich aktiver waren. Facebook liegt mit einer Interaktionsrate von 5,43% über dem Durchschnitt, was ebenfalls eine leicht überdurchschnittliche Performance darstellt. Instagram zeigt hingegen eine unterdurchschnittliche Leistung mit einer Interaktionsrate, die 40,05% unter dem Durchschnitt liegt. Twitter verzeichnet die schwächste Performance, da die Interaktionsrate 76,23% unter dem Durchschnitt liegt. Hier gibt es Potenzial zur Verbesserung. Zusammenfassend lässt sich sagen, dass LinkedIn und Facebook im August 2024 besonders gut abgeschnitten haben, während Instagram und Twitter hinter ihren jeweiligen Durchschnittswerten zurückblieben.","answer":""},
{"question":"Wie viele Posts gab es auf jeder Plattform im 1. Quartal 2024 und im 2. Quartal 2024?","ground_truth":"Im 1. und 2. Quartal 2024 wurden auf den Social-Media-Plattformen von EnBW folgende Beiträge veröffentlicht: 1. Quartal 2024: Facebook: 23 Posts Instagram: 58 Posts LinkedIn: 45 Posts Twitter: 39 Posts 2. Quartal 2024: Facebook: 34 Posts Instagram: 118 Posts LinkedIn: 55 Posts Twitter: 41 Posts Es lässt sich feststellen, dass die Aktivität im 2. Quartal 2024 auf allen Plattformen gestiegen ist, insbesondere auf Instagram, wo die Anzahl der Posts mehr als verdoppelt wurde (von 58 auf 118 Posts). Facebook, LinkedIn und Twitter verzeichneten ebenfalls moderate Zuwächse.","answer":""},
{"question":"Was waren die 5 erfolgreichsten Social-Media-Posts bezüglich der Reichweite im März 2024?","ground_truth":"Im März 2024 waren dies die fünf erfolgreichsten Social-Media-Posts von EnBW, basierend auf der Reichweite (Impressions): Facebook (20. März 2024) Post: \"Was wissen ELIF & Nico Rosberg über #EMobilität? Wir haben knifflige Fragen ausgepackt. 😎🤯 Wie viele Treffer hattest du? Schreibs gerne in die Kommentare.\" Reichweite: 318.863 Impressionen https://facebook.com/125667397492554_1547125662686668 Facebook (11. März 2024) Post: \"#LadepowerFürAlle – Von Nico Rosberg bis ELIF, von Moabit bis Monaco. Volle Ladung für jede und jeden!\" Reichweite: 261.693 Impressionen https://facebook.com/125667397492554_1983241988737601. Facebook (13. März 2024) Post: \"Wenn ELIF & Nico Rosberg zusammenkommen, entsteht elektrisierende Magie!\" Reichweite: 177.152 Impressionen https://facebook.com/125667397492554_3765111327065788. LinkedIn (8. März 2024) Post: \"Führungswechsel bei der #EnBW: Der Aufsichtsrat stimmt der Amtsniederlegung von Andreas Schell zu und ernennt Georg Stamatelopoulos zum neuen Vorstandsvorsitzenden.\" Reichweite: 97.710 Impressionen https://www.linkedin.com/feed/update/urn:li:share:7171826307398381568. Instagram (28. März 2024) Post: \"Kennzeichen raten – bist du dabei? 🤗 @nicorosberg & @elifmusic haben schon mal vorgelegt.\" Reichweite: 73.502 Impressionen https://www.instagram.com/reel/C5D3N0Ei6_D/. Diese Posts zeigen, dass Inhalte zur Elektromobilität, insbesondere in Zusammenarbeit mit bekannten Persönlichkeiten wie Nico Rosberg und ELIF, besonders erfolgreich waren.","answer":""},
{"question":"Wie viele Posts und Impressions gab es im Juni 2024 und wie viele im Juli 2024 auf LinkedIn, und wie hoch war die prozentuale Veränderung?","ground_truth":"Im Juni und Juli 2024 gab es auf LinkedIn jeweils 18 Posts. Die Reichweite (Impressions) hat sich jedoch verringert: Juni 2024: 261.584 Impressionen Juli 2024: 214.056 Impressionen Das bedeutet eine prozentuale Abnahme der Impressionen von 18,15 % von Juni zu Juli. Obwohl die Anzahl der Posts konstant geblieben ist, ist die Reichweite im Juli deutlich zurückgegangen, was möglicherweise auf weniger engagementstarke Inhalte oder Veränderungen im LinkedIn-Algorithmus zurückzuführen sein könnte.","answer":""},
{"question":"Was war der durchschnittliche Prominenzwert der EnBW in den Printmedien im ersten Quartal 2024?","ground_truth":"Der durchschnittliche Prominenzwert der EnBW in den Printmedien im ersten Quartal 2024 lag bei 0,538. Dieser Wert gibt an, wie stark die EnBW in den Printmedien im Vergleich zu anderen Themen oder Akteuren vertreten war. Ein höherer Prominenzwert deutet auf eine größere mediale Sichtbarkeit hin.","answer":""},
{"question":"Welche 5 Veröffentlichungen hatten die besten Bewertungen (clipping_rating) im Januar 2024 und in welchem Genre wurden sie veröffentlicht?","ground_truth":"Die fünf Veröffentlichungen mit den besten Bewertungen (clipping_rating) im Januar 2024 stammen alle aus den Printmedien und haben eine durchweg positive Bewertung erhalten. Hier sind die Veröffentlichungen mit ihren jeweiligen Titeln und Bewertungen: \"Erfolgreiches Marketing\" – Printmedien, Bewertung: 5 (Positiv) \"Die Kiste\" – Printmedien, Bewertung: 5 (Positiv) \"Ziel: Nachhaltige Entwicklung\" – Printmedien, Bewertung: 5 (Positiv) \"Es sind zehn Windräder\" – Printmedien, Bewertung: 5 (Positiv) \"Das Problem der hohen Gewinne\" – Printmedien, Bewertung: 5 (Positiv) Alle Veröffentlichungen wurden mit der höchsten möglichen Bewertung von 5 und einem positiven Ton versehen, was auf eine sehr gute mediale Rezeption hinweist.","answer":""},
{"question":"Wie viele Medienberichte enthielten im ersten Halbjahr 2024 eine Referenz zur Firma und wie hoch war deren durchschnittliche Reichweite?","ground_truth":"Im ersten Halbjahr 2024 wurden insgesamt 2.065 Medienberichte veröffentlicht, die eine Referenz zur Firma EnBW enthielten. Die durchschnittliche Reichweite dieser Medienberichte lag bei 859.424.","answer":""},
{"question":"Welche 5 Printmedien hatten die höchste Bruttoreichweite im dritten Quartal 2024 und wie hoch war die durchschnittliche Reichweite?","ground_truth":"Im dritten Quartal 2024 waren die fünf Printmedien mit der höchsten durchschnittlichen Bruttoreichweite wie folgt: n-tv – Durchschnittliche Reichweite: 8.000.705 FOCUS Online – Durchschnittliche Reichweite: 7.297.602 Spiegel Online – Durchschnittliche Reichweite: 5.871.797 DIE WELT – Durchschnittliche Reichweite: 3.473.092 Focus – Durchschnittliche Reichweite: 2.885.000 Diese Zahlen zeigen, dass n-tv die höchste Reichweite im betrachteten Zeitraum hatte, während Focus unter den Top 5 mit einer deutlich geringeren Reichweite abschloss.","answer":""},
{"question":"In welchen 5 Printmedien, mit mindestens 5 Veröffentlichungen, hatte die EnBW im ersten Quartal 2024 die höchste durchschnittliche Prominenz (prominence_score), wie viele Veröffentlichungen gab es und wie hoch war die durchschnittliche Reichweite dieser Veröffentlichungen?","ground_truth":"Im ersten Quartal 2024 hatte die EnBW in den folgenden fünf Printmedien die höchste durchschnittliche Prominenz, wobei mindestens fünf Veröffentlichungen verzeichnet wurden: Zeit Online Anzahl der Veröffentlichungen: 11 Durchschnittliche Reichweite: 2.221.981 Durchschnittliche Prominenz: 0.77 FAZ.net Anzahl der Veröffentlichungen: 14 Durchschnittliche Reichweite: 1.939.495 Durchschnittliche Prominenz: 0.77 EiD Energie Informationsdienst (Print) Anzahl der Veröffentlichungen: 15 Durchschnittliche Reichweite: 5.473 Durchschnittliche Prominenz: 0.73 Süddeutsche.de Anzahl der Veröffentlichungen: 30 Durchschnittliche Reichweite: 1.783.499 Durchschnittliche Prominenz: 0.73 Neckar- und Enzbote Anzahl der Veröffentlichungen: 5 Durchschnittliche Reichweite: 10.456 Durchschnittliche Prominenz: 0.70 Zeit Online und FAZ.net erzielten die höchste durchschnittliche Prominenz bei Veröffentlichungen über die EnBW, während Süddeutsche.de die meisten Veröffentlichungen hatte. Die Reichweite variiert stark, wobei Zeit Online die größte und EiD Energie Informationsdienst (Print) die geringste Reichweite aufwies.","answer":""}
]

# COMMAND ----------

df_test = spark.createDataFrame(t)
df_test = df_test.withColumnRenamed("question", "inputs").select("inputs", "ground_truth")

# COMMAND ----------

df_test_correctness = spark.createDataFrame(
    [
        ("Wie viele Facebook Posts gab es im ersten Quartal 2024?", "23"),
        ("Wie viele Instagram Posts gab es im ersten Quartal 2024?", "20"),
        ("Wie viele Likes, Shares und Kommentare gab es im ersten Quartal 2024 auf Instagram", "Likes: 4685\nShares: 84\nKommentare: 111"),
        ("Wie viele Likes, Shares und Kommentare gab es im Jan 2023 auf Facebook", "Likes: 624\nShares: 31\nKommentare: 176"),
        ("Welche Themen waren im Januar 2023 auf Facebook relevant?", "Elektromobilität, Ladeinfrastruktur"),
    ], schema='inputs string, ground_truth string'
)

# COMMAND ----------

class RAGModelWrapper(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        pass

    def predict(self, context, model_input):
        
        client = get_deploy_client("databricks")
        
        def complete(prompt):
            print('input:', prompt)
            print('type:', type(prompt))
            input_dict = {
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            return client.predict(endpoint="agents_datif_pz_uk_dev-04_ai-uk_chatbot", inputs=input_dict)['content']

        return model_input['inputs'].apply(complete)

# COMMAND ----------

with mlflow.start_run() as run:
    set_deployments_target("databricks")
    mlflow.set_registry_uri("databricks-uc")
    gpt4o_answer_similarity = mlflow.metrics.genai.answer_similarity(
        model="endpoints:/openai-gpt-4o"
    )
    gpt4o_answer_correctness = mlflow.metrics.genai.answer_correctness(
        model="endpoints:/openai-gpt-4o"
    )

    logged_model_info = mlflow.pyfunc.log_model(
        "model", python_model=RAGModelWrapper(), pip_requirements=["pandas"]
    )

    results = mlflow.evaluate(
        logged_model_info.model_uri,
        df_test_correctness,
        targets="ground_truth",
        predictions="content",
        model_type="question-answering",
        extra_metrics=[
            mlflow.metrics.latency(),
            gpt4o_answer_similarity,
            gpt4o_answer_correctness,
        ]
    )

    print(f'Results: \n{results.metrics}')
    eval_table = results.tables['eval_results_table']
    print(f"Eval table: \n{eval_table.display()}")

# COMMAND ----------


