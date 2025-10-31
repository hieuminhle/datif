# Databricks notebook source
# MAGIC %md
# MAGIC # Abstract Generierung für Argus Print Medien Panel Daten 
# MAGIC
# MAGIC * Um was handelt es sich hier  (Kurzbeschreibung Inhalt):  
# MAGIC In diesem Notebook werden für die Print Medien Artikel, die über Argus API abegrufen wurden, ein Abstract für den Volltext generiert.
# MAGIC Dazu werden die zuvor abgerufenen Volltexte der Artikel aus dem raw Container aufgerufen und an ein OpenAI GPT-4 Modell zur Abstract Generierung übergeben. 
# MAGIC Der Output des Modells wird als parquet im 02-cleaned Modell gespeichert.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC * QUELLEN:  
# MAGIC - 01-raw/argus/print_media_volltexte/YYYY/MM/DD/binary/{clipping_id}.json
# MAGIC
# MAGIC * ZIEL:  
# MAGIC - 02-cleaned/argus/print_media_abstracts/YYYY/MM/DD/binary/{clipping_id}.parquet
# MAGIC   
# MAGIC ---
# MAGIC * Versionen (aktuelle immer oben):     
# MAGIC * 16.10.2024 Svenja Schuder: Update PZ Integration
# MAGIC * 05.09.2024 Svenja Schuder: Init
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Initialnotebooks & Libraries

# COMMAND ----------

import os
from openai import AzureOpenAI
import json
import re
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../../common/nb_init

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Utils & Help - parameters & functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Parameters

# COMMAND ----------

# source dir volltexte
source_dir_json = f"argus/print_media_volltexte_historic/json"
volltext_path = sta_endpoint_pz_uk['01_raw'] + '/' + source_dir_json

target_schema = "datif_pz_uk_dev.02_cleaned"
target_path = "argus"


# COMMAND ----------

AZURE_OPENAI_API_KEY = get_secret('openai-key')
AZURE_OPENAI_ENDPOINT = get_secret('openai-endpoint')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Functions

# COMMAND ----------

def fn_build_merge_condition(base:str, source:str, keys: list, conjunction: str = "and"):
  """
  Builds merge-conditions based on and source and target alias name and a list of keys.
  The Keys must be present in both the source and the target.
  """

  connect = f" {conjunction.strip()} "
  conditions = [f"{base}.{key} <=> {source}.{key}" for key in keys]

  res = connect.join(conditions)

  return res

# COMMAND ----------

def fn_create_or_upsert_table(df_source, target_path, target_database_name, target_table_name, primary_key):
    """
    Creates a table if it does not exist or upserts a table based on the source DataFrame.
    Args:
        df_source (DataFrame): Source DataFrame.
        target_table_name (str): Target table name.
        target_database_name (str): Target database name.
        target_path (str): Path to target.
        primary_key (list): List of primary key column names.
    Returns:
        None
    """
    full_tablename = f"{target_database_name}.{target_table_name}"
    target_path = f"{sta_endpoint_pz_uk['02_cleaned']}/{target_path}/{target_table_name}"
    table_exists = spark.sql(f"SHOW TABLES IN {target_database_name} LIKE '{target_table_name}'").count()

    # If the table does not exist, create it
    if not table_exists:
        df_source.write.saveAsTable(
            full_tablename,
            format="delta",
            path=target_path,
            mode="overwrite",
        )
        print(f"Newly created {full_tablename}") 
    # If the table exists, upsert the data
    else:
        merge_condition = fn_build_merge_condition("target", "source", primary_key)
        delta_target_table = DeltaTable.forName(spark, full_tablename)
        delta_target_table.alias("target") \
            .merge(df_source.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Updated {full_tablename}")

# COMMAND ----------

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

def dbutils_walk(dir_path):
    files = []
    directories = [dir_path]

    while directories:
        path = directories.pop()
        subdirs = dbutils.fs.ls(path)
        for subd in subdirs:
            if subd.isDir():
                directories.append(subd.path)
            else:
                files.append(subd.path)
    return files

# COMMAND ----------

class UnscorableCommentError(Exception):
    pass

#  Function: Call LLM for abstract generation
def generate_abstract(s: str, model: str) -> str:
    messages = generate_prompt_messages(s)
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        seed=42,
        max_tokens=800,
        temperature=0.0,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        stream=False
    )
    abstract = response.choices[0].message.content.strip()
    # Raise an Attribute Error if abstract could not be generated
    try:
        return str(abstract)
    except AttributeError:
        raise UnscorableCommentError(f"Could not generate abstract: {s}")

# COMMAND ----------

from typing import List, Dict

def generate_prompt_messages(s: str) -> List[Dict]:
    return [
        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die EnBW hat. Wenn in dem Text Wettbewerber von EnBW wie RWE, Vattenfall, Uniper oder E.ON erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der EnBW und oder der CEO beziehungsweise der Vorstandsvorsitzende von EnBW erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die EnBW anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Neues Leben für alte Batterien - EnBW und Audi testen in einem gemeinsamen Projekt, wie alte Antriebsbatterien aus E-Autos weiter genutzt werden können. Dafür wird ein stationärer Batteriespeicher mit Akkus befüllt, die aus zerlegten Testfahrzeugen stammen. Das Aggregat besteht aus zwölf Hochvolt-Batteriesystemen. Zusammengeschaltet bringen sie es auf eine Gesamtleistung von einem MW. In den kommenden Wochen wird zunächst die Leistungsfähigkeit des Speichers geprüft und verschiedene Einsatzszenarien simuliert. Dazu zählt auch die Bereitstellung von Regelleistung für das Stromnetz. Umgekehrt wird bei einem Uberangebot von Wind- oder PV-Anlagen Strom gespeichert. Außerdem wird untersucht, wie die Speicherkapazität am Energiemarkt eingesetzt werden kann, falls günstiger Strom aus erneuerbaren Energien vorhanden ist. In ihrem »zweiten Leben« haben die Zellen nach Ansicht der Experten eine Einsatzzeit von mindestens fünf bis zehn Jahren. Danach führt Audi die Batterien einem endgültigen Recycling zu. Dabei werden sie in ihre einzelnen Bestandteile und Rohstoffe zerlegt, um perspektivisch wieder in neuen Batterien zum Einsatz zu kommen. Die Anlage dient als Referenz für zunächst vier Projekte, die bei EnBW derzeit für die nähere Zukunft geplant sind.
            """.strip(),
        },
        {
            "role": "assistant", 
            "content": """
            In einer Kooperation zwischen EnBW und Audi wird die Weiterverwendung alter Antriebsbatterien aus E-Autos in einem stationären Batteriespeicher untersucht. Dieser Speicher, wird zunächst hinsichtlich seiner Leistungsfähigkeit und diverser Einsatzmöglichkeiten getestet. Dazu gehört die Bereitstellung von Regelleistung für das Stromnetz und die Speicherung von Überschussstrom aus erneuerbaren Energiequellen. Ziel ist es auch, die Speicherkapazität gewinnbringend am Energiemarkt zu nutzen. Die Batterien sollen nach ihrer Wiederverwendung, die fünf bis zehn Jahre andauern könnte, durch Audi recycelt werden. Dieses Projekt dient als Modell für vier weitere Vorhaben, die bei EnBW geplant sind, und ist somit von strategischer Bedeutung für die nachhaltige Energieverwaltung und den Ausbau der Energieeffizienz im Unternehmen.

            Tonalität: +0.9
            """
        },

        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die EnBW hat. Wenn in dem Text Wettbewerber von EnBW wie RWE, Vattenfall, Uniper oder E.ON erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der EnBW und oder der CEO beziehungsweise der Vorstandsvorsitzende von EnBW erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die EnBW anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Ökostrom-Kunden gucken in die Röhre Strom aus erneuerbaren Energien ist genauso teuer wie aus fossilen Quellen. Daran sind die Mechanismen der Energiemärkte schuld VON CATHARINA SCHUL catharina.schulz@suedkurier.de Freiburg – Als der Gaspreis nach dem Beginn des Kriegs in der Ukraine die Strompreise in die Höhe trieb, wähnten sich viele Verbraucher auf der sicheren Seite, deren Strom aus Sonne, Wind und Co. stammt. Doch sie lagen falsch. Zahlreiche Versorger haben mittlerweile die Preise für Ökostrom erhöht. So gibt der deutsch-schweizerische Energieversorger Energiedienst auf Nachfrage an, 100 000 Kunden seien von einer Preiserhöhung betroffen, um durchschnittlich 40 Prozent. Bei Deutschlands drittgrößtem Energieversorger EnBW aus Karlsruhe müssten Kunden für Ökostrom im Schnitt 15,2 Prozent mehr zahlen als im Vorjahr. Auch ein SÜDKURIER-Leser aus Hilzingen berichtet, dass ihm Energiedienst eine Preiserhöhung angekündigt hat. Obwohl er zu 100 Prozent Strom aus erneuerbaren Energien bezieht, müssten er und andere umweltbewusste Nutzer die gestiegenen Kosten für fossile Energieträger mit bezahlen, kritisiert er. Mit der Annahme hat er recht. Die Preise für Stromlieferungen entstehen an den Handelsplätzen wie der Leipziger Strombörse EEX. Hier treffen sich Anbieter wie Kraftwerksbetreiber oder Großhändler und Nachfragende wie Stromlieferanten oder Industriebetriebe. Der Einheits-Börsenpreis wird über die sogenannte Merit-  Order (englisch: Reihenfolge der Vorteilhaftigkeit) gebildet. Bei diesem System richtet sich der Strompreis nach dem teuersten Angebot im Markt, das gebraucht wird, um den Bedarf zu decken. Zuletzt waren das  häufig die teuren Gaskraftwerke. Da die Preise für Kohle und Gas gestiegen sind, steigen die Stromkosten – auch für Ökostrom. Selbst direkte oder regionale Lieferverträge werden vom Börsenpreis geprägt, erklärt der Energiedienst-Sprecher Christoph Klink. Jeder Ökostromanbieter hängt am Börsenpreis: Will er zu einem höheren Preis als dem Börsenpreis einkaufen, hat er einen Nachteil anderen Anbietern gegenüber. Versucht er, zu niedrigeren Preisen einzukaufen, würden die Betreiber von Windrädern und Photovoltaik-Anlagen sich Abnehmer suchen, die den teureren Börsenpreis zahlen oder direkt an die Strompreisbörse gehen. Energiedienst müsse zudem auch Strom von anderen Hochrhein-Wasserkraftwerken zu Börsenpreisen einkaufen, so Klink weiter. „Wir wissen, dass die allgemein gestiegenen Energiekosten insgesamt eine echte Mehrbelastung für die Verbraucher bedeuten“, sagt EnBW-Sprecher Rashid Elshahed. Doch gerade die Energiekrise zeige, wie wichtig die nachhaltige Energiewende hin zu erneuerbaren Energien sei. Wer sich für Ökostrom entscheide, unterstütze den Ausbau aktiv. Auch wenn er preislich dadurch keinen Vorteil hat. Preisdruck entsteht zudem durch die Netzentgelte. Mit ihnen wird der Ausbau und Betrieb der Energienetze finanziert. Nach einer Auswertung des Verbraucherportals Verivox steigen die Entgelte 2023 um fast 19 Prozent. 2022 betrugen sie rund acht Cent je Kilowattstunde und damit knapp 20 Prozent des Endkundenstrompreises. Zwar soll ein Bundeszuschuss die Netzentgelte 2023 stabil halten, klar ist aber schon jetzt, dass das Geld nicht reichen wird. Ökostrom ist zwar günstig in der Produktion, aber teuer für die Verbraucher.
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Trotz der Erzeugung aus erneuerbaren Quellen wie Sonne und Wind sind die Kosten für Ökostrom gestiegen, was auf die Preisbildung an der Leipziger Strombörse EEX zurückzuführen ist. Dort wird der Einheitspreis durch das teuerste benötigte Angebot bestimmt, zuletzt häufig durch Gaskraftwerke. Diese Mechanik führt dazu, dass auch die Preise für Ökostrom ansteigen, da alle Anbieter, einschließlich EnBW, am Börsenpreis hängen. EnBW hat die Preise für Ökostrom um durchschnittlich 15,2 Prozent erhöht. Dies betrifft nicht nur EnBW, sondern auch andere Anbieter wie Energiedienst. Der Artikel betont, dass trotz höherer Kosten die Wahl von Ökostrom den Ausbau erneuerbarer Energien unterstützt und somit langfristig zur Energiekrise beiträgt. Die Bedeutung für EnBW liegt in der Herausforderung, die Energiewende zu fördern, während sie gleichzeitig finanzielle Nachhaltigkeit sicherstellen müssen.

            Tonalität: +0.1
            """
        },
        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die EnBW hat. Wenn in dem Text Wettbewerber von EnBW wie RWE, Vattenfall, Uniper oder E.ON erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der EnBW und oder der CEO beziehungsweise der Vorstandsvorsitzende von EnBW erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die EnBW anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Region macht Tempo bei Solarenergie HEILBRONN Bis Jahresende sollen zusätzliche Flächen und Erleichterungen feststehen Sechs Großprojekte erhalten Zuschlag Heiko Fritze Der Ausbau der FreiflächenPhotovoltaik in HeilbronnFranken soll sich massiv beschleunigen. Soeben hat die Bundesnetzagentur in der jüngsten Ausschreibungsrunde sechs Projekten in der Region den Zuschlag erteilt Fünf davon sollen auf Wiesen- oder Ackerflächen errichtet werden, eines im Öhringer Gewerbegebiet Ost. Sie haben zusammen eine Fläche von 32 Hektar. Millioneninvestitionen Der Zuschlag ging an Investoren, die bei Forchtenberg, Braunsbach, Niedersteffen, Schwäbisch Hall und Weikersheim bauen wollen. Neben Landwirten erhielten auch der Energieversorger Zeag und die Stadtwerke Schwäbisch Hall je einen Zuschlag. Die Zeag hatte zudem mit einem Projekt bei Billigheim (NeckarOdenwald-Kreis) Erfolg. Die EnBW-Tochter will in beide Vorhaben insgesamt 12,5 Millionen Euro investieren. Um die Voraussetzungen für weitere Anlagen zu schaffen, sammelt der Regionalverband gerade von allen 111 Kommunen in der Region Daten über bereits vorhandene Photovoltaik. Zwar weiß die Behörde von 75 Bebauungsplänen mit 314 Hektar Fläche und weiteren 41 Planverfahren mit 354 Hektar, doch können sich die Daten nach der Bearbeitung durch den Regionalverband nochmals geändert haben, erläutert der Direktor des Regionalverbands, Klaus Mandel. Zwei-Prozent-Ziel Die Daten sollen Basis für eine Teilfortschreibung des Flächennutzungsplans werden: Bis Ende des Jahres will der Regionalverband die Bedingungen für Freiflächen-Photovoltaik erleichtern. Dann sollen auch in regionalen Grünzügen bis zu zehn Hektar große Anlagen möglich sein. Außerdem soll es Ausnahmen geben, wenn in der Nähe großer Stromverbraucher wie etwa Industriebetrieben gebaut werden soll, kündigt Mandel an. Die nötigen Kriterienkataloge sollen noch im Frühjahr vorliegen, sagt er. Dass das Tempo beschleunigt wird, liegt auch an dem Zwei-Prozent-Ziel, das die Bundesregierung vergangenes Jahr in Gesetzesform gegossen hat: Demnach sollen zwei Prozent der Flächen in allen Bundesländern mit Anlagen für Erneuerbare Energien, also Windrädern und Photovoltaik, bebaut werden. Die baden-württembergische Landesregierung hat für alle Regionen, also auch Heilbronn-Franken, als Ziel 1,8 Prozent der Fläche für Windkraft ausgegeben. Ob nur die übrigen 0,2 Prozent von Photovoltaik übernommen werden müssen oder sogar noch mehr, stehe noch nicht fest, berichtet Mandel. Zwei Prozent entsprechen jedenfalls 9530 Hektar, jene 0,2 Prozent also 953 Hektar und damit mehr als alle bislang vorhandenen und in Planung befindlichen Photovoltaik-Flächen zusammen einnehmen. Mehr Tempo Um die Ziele der Energiewende rechtzeitig zu erreichen, müssten nach Berechnungen des Energieversorgers EnBW täglich in Deutschland sieben Windräder errichtet und 117 Fußballfelder mit Photovoltaik belegt werden wobei hier auch Dachflächen einbezogen sind. Diese Zahlen präsentierte jüngst Andreas Pick, Gesamt-Projektleiter für Energiewende-Projekte bei der EnBW. Alleine für die Versorgung der Stadt Heilbronn mit Solarthermie werde eine Fläche von 170 bis 200 Fußballfeldern benötigt, sagte er. Die EnBW will am Heilbronner Kohlekraftwerk, das 2026 stillgelegt werden soll, eine Gasturbine errichten. Meinung „Bremsklötze weg“ Ziele bis 2030 Ziel der Bundesregierung ist es, den Anteil des aus erneuerbaren Energien erzeugten Stroms am Stromverbrauch bis 2030 auf 80 Prozent zu steigern. Im vergangenen Jahr waren es nach ersten Branchenberechnungen rund 47 Prozent. Wer in Deutschland Wind- oder Solarparks mit staatlicher Förderungauen will, kann in Ausschreibungen Gebote ab- geben. Die Höchstwerte sollen nun deutlich angepasst werden, um zusätzliche Anreize zu schaffen. Außerdem wurden die Anreize für Privathaushalte, in Photovoltaik zu investieren, deutlich erhöht. So sinkt der Mehrwertsteuersatz ab 2023 auf null Prozent. Der Ertrag muss demnach in der Regel nicht mehr als Einkommen versteuert werden.
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Der Ausbau der Solarenergie in der Region Heilbronn-Franken wird durch die Bundesnetzagentur beschleunigt, die sechs Großprojekte für Freiflächen-Photovoltaikanlagen genehmigt hat. Diese Projekte, deren Investition von verschiedenen Akteuren, einschließlich der EnBW-Tochter, getragen wird, sind Teil eines größeren Plans, um die gesetzlichen Ziele der Bundesregierung zu erreichen. EnBW plant, 12,5 Millionen Euro in zwei Projekte zu investieren, um die Versorgung mit erneuerbarer Energie zu stärken. Diese Entwicklung ist für EnBW von großer Bedeutung, da sie aktiv zur Erreichung des Bundesziels beiträgt, bis 2030 den Anteil erneuerbarer Energien auf 80% zu steigern. Zudem zeigt dies EnBW's Engagement in der Energiewende und deren Bemühungen, ihre Energieproduktion nachhaltiger zu gestalten. Ein weiteres wichtiges Thema ist die Anpassung der Ausschreibungsbedingungen und steuerlichen Anreize, um den Ausbau von Photovoltaik und Windenergie zu fördern.

            Tonalität: +0.8
            """
        },

        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die EnBW hat. Wenn in dem Text Wettbewerber von EnBW wie RWE, Vattenfall, Uniper oder E.ON erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der EnBW und oder der CEO beziehungsweise der Vorstandsvorsitzende von EnBW erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die EnBW anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            KLÄRSCHLAMMVERBRENNUNG Gemeinde sieht einen Teilerfolg Die Gegner der geplanten Klärschlammverbrennungsanlage auf dem Gelände der EnBW in Walheim wird diese Nachricht freuen: Die EnBW darf derzeit Eidechsen umsiedeln mehr darf sie nicht. Alle weiteren beantragten Maßnahmen hat das Regierungspräsidium abgelehnt. WALHEIM VON ANDREA NICHT-ROTH Noch bevor das Genehmigungsverfahren zur Errichtung des Klärschlammheizkraft-werks abgeschlossen ist, hatte die EnBW Anfang Juli beantragt, vorzeitig drei Maß-nahmen ergreifen zu dürfen: Zum einen sollen Zaun und Mauereidechsen umgesiedelt werden, zum anderen sollen Gehölzflächen gerodet werden und Schilfröhricht soll entfernt werden. So lautete jedenfalls der Antrag der EnBW. Nach „sorgfältiger Prüfung" hat die Immissionsschutzbehörde beim Regierungspräsidium Stuttgart diesen Antrag nun jedoch in weiten Teilen abgelehnt. Das Regierungspräsidium erlaubt lediglich die Umsetzung der am Standort lebenden Eidechsen; gefällt und gerodet werden darf nichts. Die Umsiedlung der Eidechsen jedoch sei nicht von der Veränderungssperre erfasst, die die Gemeinde Walheim über das Gebiet gelegt hat. Die weiteren natur-schutzfachlichen Maßnahmen jedoch das Roden von Gehölz und Röhrichtflächen, die von der EnBW beantragt worden waren fallen nach Einschätzung des Regierungspräsidiums unter die Veränderungssperre und werden daher nicht zugelassen. So werde sichergestellt, dass keine irreversiblen Maßnahmen umgesetzt werden, heißt es in der Pressemitteilung. „Wir prüfen den Antrag der Vorhabenträgerin auf Errichtung eines Klärschlammheizkraftwerkes in Walheim sehr sorgfältig dies haben wir auch nochmals im Erörterungstermin Ende Juni betont. Daher haben wir nach umfassender rechtlicher Prüfung den jetzigen Antrag auf vor-zeitigen Baubeginn für zwei der drei beantragten naturschutzfachlichen Maßnahmen abgelehnt, um vor der endgültigen Entscheidung über die Veränderungssperre durch den Verwaltungsgerichtshof so-wie über den Bau des Klärschlammheiz-kraftwerkes keine Fakten zu schaffen", betonte eine Sprecherin des Regierungspräsidiums. Weil die Gemeinde Walheim unter allen Umständen die geplante Klärschlammverbrennungsanlage verhindern will, hat der Gemeinderat Ende Juli die Aufstellung des Bebauungsplans „Mühlwiesen/Mühlstraße" beschlossen. Das sagt die Gemeinde Walheim. Auch über eine Veränderungssperre des Gebiets war abgestimmt worden. Diese Entscheidung hatte noch der alte Gemeinderat getroffen, das neu gewählte Gremium hat sie kürzlich mit zehn Ja und einer Nein-Stimme bekräftigt. Die Veränderungssperre steht einer Genehmigung aktuell entgegen. Der Genehmigungsantrag befindet sich weiterhin in Prüfung durch das Regierungspräsidium Stuttgart. Die EnBW hat gegen die Veränderungs-sperre beim Verwaltungsgerichtshof Ba-den-Württemberg in Mannheim ein Normenkontrollverfahren sowie einen Antrag auf Eilrechtsschutz gestellt. Bei der Gemeinde Wahlheim sieht in der Entscheidung des Regierungspräsidiums „einen Teilerfolg, und dass man unser An-liegen ernst nimmt", sagt der Erste stell-vertretende Bürgermeister Wilhelm Weiß: „Wir fühlen uns durch diese Entscheidung in unserem Anliegen bestärkt." Nun hoffe die Gemeinde, dass die restlichen immissionsschutzrechtlichen Belange ebenso sorgfältig geprüft „und in unserem Sinn entschieden werden", sagt Weiß im Gespräch mit unserer Zeitung. Die Bürgerinitiative (BI) Bürger im Neckartal sieht zwar auch „einen kleinen Hoffnungsschimmer", Matthias Appelt von der BI kommentiert die Entscheidung aber eher nüchtern: „Das hat nicht so viel zu sagen; das Regierungspräsidium konnte eigentlich gar nicht anders entscheiden, es ist die logische Konsequenz aus der Veränderungssperre." Trotzdem freue er sich natürlich, dass die Behörde diese Aussage gemacht habe. Ganz anders die EnBW. Zur Entscheidung des Regierungspräsidiums erklärt EnBW-Projektleiter Andreas Pick: „Die EnBW bedauert, dass das Regierungspräsidium unseren Antrag in mehreren Punkten abgelehnt hat, respektiert die Entscheidung aber natürlich. Wir begrüßen, dass zumindest die Umsiedlung von Eidechsen genehmigt worden ist. Wir sehen dem weiteren Fortgang des Verfahrens zuversichtlich entgegen und hoffen, dass das von uns angestrengte Eilverfahren eine zeitnahe Entscheidung des Verwaltungs-gerichtshofes über die von der Gemeinde Walheim erlassene Veränderungssperre bringt." Der Landtagsabgeordnete der Grünen im Wahlkreis Bietigheim-Bissingen, Tay-fun Tok, kommentiert die Entscheidung folgendermaßen: „Das Regierungspräsidium verbietet der EnBW am Standort Walheim vorzeitige Veränderungen an Gehölzen und Grünanlagen. Damit zeigt die Be-hörde, dass der Konzern sich wie jeder andere an Recht und Gesetz halten muss und keine Genehmigungen per Freibrief erhält. Herr des Verfahrens ist die Landesbehörde. Darauf kann man in einem Rechtsstaat vertrauen." Hier soll eine Klärschlammverbrennungsanlageentstehen. Die Gemeinde wehrt sich dagegen. Foto, Alfred Drossel Auch erschienen in Ludwigsburger Kreiszeitung | 15.08.2024 | Online
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Die EnBW plant eine Klärschlammverbrennungsanlage in Walheim, stößt jedoch auf Widerstand von der lokalen Gemeinde und einer Bürgerinitiative. Das Regierungspräsidium Stuttgart hat kürzlich mehrere Anträge der EnBW für vorzeitige Maßnahmen, darunter Rodungen und Entfernung von Schilfröhricht, abgelehnt, lediglich die Umsiedlung von Eidechsen wurde genehmigt. Diese Entscheidung ist ein Rückschlag für EnBW, die bereits rechtliche Schritte gegen die von der Gemeinde erlassene Veränderungssperre eingeleitet hat. Dieser Fall ist für EnBW von Bedeutung, da er die Herausforderungen bei der Durchsetzung von Infrastrukturprojekten unterstreicht und die Notwendigkeit aufzeigt, sowohl rechtliche als auch kommunale Hürden zu überwinden. Die Entscheidung des Regierungspräsidiums zeigt, dass EnBW sich an die gesetzlichen Vorgaben halten muss und keine Sonderbehandlung erwarten kann.

            Tonalität: -0.5
            """
        },

        {
            "role": "user",
            "content": f"""
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die EnBW hat. Wenn in dem Text Wettbewerber von EnBW wie RWE, Vattenfall, Uniper oder E.ON erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der EnBW und oder der CEO beziehungsweise der Vorstandsvorsitzende von EnBW erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages anhand der Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            
            {s}
            """.strip(),
        },
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Data Schemas

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Define the schema for JSON file
schema_print = StructType([
    StructField("clipping_id", StringType(), True),
    StructField("Text", StringType(), True)
])

# COMMAND ----------

# Define the schema for df_abstract
schema_abstract = StructType([
    StructField("clipping_id", StringType(), nullable=False),
    StructField("Abstract", StringType(), nullable=False),
    StructField("Tonalitaet", StringType(), nullable=False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Transformationslogik

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aufruf GPT Modell zur Generierung eines Abstracts und Tonalität

# COMMAND ----------

# load Azure OpenAI Model client

client = AzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    #api_version="2024-02-01",
    api_version="2024-08-01-preview",
    azure_endpoint = AZURE_OPENAI_ENDPOINT
    )
    
deployment='gpt-4'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesen der JSON mit Volltexten aus PZ-01-raw und generieren des Abstracts

# COMMAND ----------

volltext_path = sta_endpoint_pz_uk['01_raw'] + '/' f"argus/print_media_volltexte_historic/json"
all_files = [file.path for file in dbutils.fs.ls(volltext_path)]
print(len(all_files))

# COMMAND ----------

volltext_path = sta_endpoint_pz_uk['01_raw'] + '/' f"argus/print_media_volltexte_historic/json"
all_files = [file.path for file in dbutils.fs.ls(volltext_path)]
df_abstract = spark.createDataFrame([], schema=schema_abstract)

empty_text_files = []

for file in all_files[600:]:
  file_name = file.split("/")[6]
  clipping_id = file_name.split(".")[0]

  try:
    #print(f"Processing File: {file_name}")
    # read JSON file
    df_print_media_json = spark.read.load(
    f"{file}",
    format='json',
    multiLine=True, 
    schema=schema_print)

    # extract 'clipping_id' and 'text' from JSON
    clipping_id = df_print_media_json.collect()[0][0]
    text = df_print_media_json.collect()[0][1]

    # Check if text is empty
    if text == "":
        empty_text_files.append(file_name)
        #print(f"File {file_name} has empty text.")
        continue

  except Exception as e:
        print(f"Error: {e}")

# Display the list of files with empty text
print("Files with empty text:", empty_text_files)

# COMMAND ----------

volltext_path = sta_endpoint_pz_uk['01_raw'] + '/' f"argus/print_media_volltexte_historic/json"
all_files = [file.path for file in dbutils.fs.ls(volltext_path)]
df_abstract = spark.createDataFrame([], schema=schema_abstract)

for file in all_files[630:]:
  file_name = file.split("/")[6]
  clipping_id = file_name.split(".")[0]

  if bool(df_processed_abstracts.filter(df_processed_abstracts.clipping_id.contains(clipping_id)).collect()) == True:
        print(f"Skipping file {file_name} because it already has been processed.")
        continue
  else:
    try:
      print(f"Processing File: {file_name}")
      # read JSON file
      df_print_media_json = spark.read.load(
      f"{file}",
      format='json',
      multiLine=True, 
      schema=schema_print)

      # extract 'clipping_id' and 'text' from JSON
      clipping_id = df_print_media_json.collect()[0][0]
      text = df_print_media_json.collect()[0][1]

    
      # generate abstract with LLM 
      print(f"Generiere Abstract für Artikel: {clipping_id}.pdf")
      abstract = generate_abstract(text,deployment)

      # regular expression to remove sentiment output from text
      cleaned_abstract = re.sub(r"Tonalität: ([-+]?\d*\.?\d+)\s*(.*)", "", abstract).strip()
      # regular expression to extract sentiment output
      tonalitaet = re.search(r"Tonalität: ([-+]?\d*\.?\d+)", abstract)
      if tonalitaet:
          tonalitaet_text = tonalitaet.group(1)
      else:
          print("Tonalität nicht gefunden.")

      # Create a DataFrame for the current abstract and sentiment
      current_df = spark.createDataFrame([(clipping_id, cleaned_abstract, tonalitaet_text)], schema=schema_abstract)

      # Append the current DataFrame to df_abstract
      df_abstract = df_abstract.union(current_df)
      print("Abstract erfolgreich erstellt.")

    except Exception as e:
          print(f"Error: {e}")

# COMMAND ----------

df_abstract.display()

# COMMAND ----------

#df_abstract = df_abstract.filter(df_abstract.clipping_id != "42_5872320")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Speichern der Abstracts

# COMMAND ----------

# Upsert dataframe into derived table
fn_create_or_upsert_table(df_source=df_abstract,target_path=target_path,target_database_name=target_schema,target_table_name="argus_print_media_abstracts_historic",primary_key=["clipping_id"])
