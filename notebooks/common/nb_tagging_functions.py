# Databricks notebook source
# MAGIC %pip install openai==1.55.3

# COMMAND ----------

# MAGIC %md
# MAGIC # Utility Functions

# COMMAND ----------

# MAGIC %run ./nb_init

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T, DataFrame
from pyspark.sql.types import StringType
import re
from openai import AzureOpenAI
from typing import List, Dict
import re
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Themen Tagging

# COMMAND ----------

# MAGIC %md
# MAGIC ## C-Level Tagging

# COMMAND ----------

c_level_names = ["Stamatelopoulos", "Kusterer", "Rückert-Hennen", "Güsewell"]

def find_matching_strings(post_text, keywords):
    if post_text is None:
        return None
    
    mentions = ','.join([keyword for keyword in keywords if keyword in post_text])
    return mentions if mentions != '' else None

c_level_udf = F.udf(lambda post_text: find_matching_strings(post_text, c_level_names), StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Themenbereiche, Strategische Themen, Abstracts und Tonalität

# COMMAND ----------

# DBTITLE 1,Simple Exact Matching for strategic Tags

# mapping between the tag that is used on youtube and the column name, that we use in our transformed layer
mapping_tag_to_column = {
    "Strategie": "Strategie2030",
    "Finanzierung": "FinanzierungEnergiewende",
    "EMobilitaet": "EMobilitaet",
    "Energiewelt": "VernetzeEnergiewelt",
    "Gasnetze": "TransformationGasnetzeWasserstoff",
    "ErneuerbareEnergie": "ErneuerbareEnergien",
    "DisponibleErzeugung": "DisponibleErzeugung",
    "Stromnetze": "IntelligenteStromnetze",
    "ArbeitgeberinA": "AAlsArbeitgeberIn",
    "Nachhaltigkeit": "NachhaltigkeitCSRESG",
    "MarkeA": "MarkeA"
}

owner_tags = ["UK", "uk", "VE", "EB"]


def get_strategic_tags_exact_matches(text: str, mapping_tag_to_column: Dict[str, str]):
    """
    Returns a dictionary with the strategic tags that are found in the text

    Inputs:
        text: str
        mapping_tag_to_column: Dict[str, str]
    Returns:
        Dict[str, int]: dictionary with the strategic tags that are found in the text
    """
    if text is None:
        return None
    
    matches = {}
    for tag in mapping_tag_to_column.keys():
        if tag in text:
            matches[mapping_tag_to_column[tag]] = 1
        else:
            matches[mapping_tag_to_column[tag]] = 0
    return matches

def get_owner_tag(text: str):
    if text is None:
        return "Kein Owner"
    
    for tag in owner_tags:
        if re.search(rf'\b{re.escape(tag)}\b', text):
            if tag.lower() == "uk":
                 return "UK"
            return tag
        # if tag in text:
        #     if tag.lower() == "uk":
        #         return "UK"  # Einheitliche Schreibweise
        #     return tag
    return "Kein Owner"

owner_tag_udf = F.udf(get_owner_tag, T.StringType())

strategic_tags_udf = F.udf(lambda text: get_strategic_tags_exact_matches(text, mapping_tag_to_column), returnType=T.MapType(T.StringType(), T.IntegerType()))


def tag_strategic_areas_exact_matches(df, text_col):
    df = df.withColumn("Owner", owner_tag_udf(text_col))
    df = df.withColumn("StrategicTags", strategic_tags_udf(text_col))
    df = df.select(*df, *(F.col("StrategicTags").getItem(col).alias(col) for col in mapping_tag_to_column.values()))
    df = df.drop("StrategicTags")
    return df


# COMMAND ----------

# DBTITLE 1,Define Strategic Areas
strategic_areas = {
    "Strategie2030": "Berichte und Vorstandsäußerungen zur A-Strategie; USP als integriert aufgestelltes Unternehmen; Ausbauziele (Erneuerbare Energien, Netze, E-Mobilität) ; Beitrag der A zur Energiewende; künftige Ausrichtung/Schwerpunkte der A; Zielerreichung div. Businessziele (KPIs); A-Investitionen in die Energiewende, Berichterstattung zur Bundespressekonferenz, Quartalsergebnissen oder HV, Kapitalerhöhung.",

    "FinanzierungEnergiewende": "Volkswirtschaftliche Gesamtkosten der Energiewende; A-Forderungen an Regierungen (Bund/Land/EU); Warnungen vor zu hohen Kosten und/oder zu geringer Attraktivität für Investoren, Diskussion um (Strom-) Marktdesign, EU-Regulatorik, Finanzierung durch PPAs, Strompreiszonen, Strommarktdesign, Finanzierungsmodelle durch Bürgerbeteiligungen. Kommunale Beteiligungsmodelle, Anteilskäufe und Anteilsverkäufe von Erneuerbare-Energien-Projekten oder im Bereich Netze.",

    "EMobilitaet": "Planung, Bau und Betrieb öffentlicher Schnellladeinfrastruktur in ganz Deutschland, Unterstützung der CO2-Reduzierung im Verkehrssektor, einfache Nutzung der E-Mobilität für Autofahrer*innen mit A mobility+, Vergleichstests von Mobilitätsanbietern, Kooperationen mit Partnern, Tarifanpassungen/-diskussionen (nur E-Mob, nicht allgemeine Stromtarifanpassungen!), politische Themen zum Ladeinfrastrukturausbau.",

    "VernetzeEnergiewelt": "Berichterstattung zu unserem Heim Energie Management System (HEMS) oder englisch Home Energy Management System. Dabei werden Energieerzeugung (z. B. PV-Anlage) mit Verbrauchern im Haushalt (z. B. Heizung, E-Auto) und Batteriespeichern vernetzt. Das System (Softwarelösung) funktioniert cloud-basiert.",

    "TransformationGasnetzeWasserstoff": "Diskussion um Wasserstoff-Kernnetz, Ausbau Wasserstoffinfrastruktur in BW; Wasserstoff-Pläne Terranets/VNG/Ontras, Finanzierungs- und Regulierungsfragen rund um das Kernnetz, Aufbau von Wasserstoff-Produktion/Kooperationen zu Wasserstoff-Import, Modellvorhaben und Joint Ventures im Bereich Wasserstoff",

    "ErneuerbareEnergien": "Planung, Bau, Inbetriebnahmen und Betrieb aller Erneuerbaren-Anlagen der A (Onshore/Offshore, PV/Wasserkraft/Biomasse) (Speicherlösungen nur dann, wenn sie nebenthematisch bei Berichten zu PV-Parks erwähnt werden. Ansonsten sind Berichte zu Speichern bei disponibler Leistung einzusortieren), Diskussion um Genehmigungsverfahren in Bund, Land und Kommunen, Bau von He Dreiht, mögliche Beteiligung A an weiteren Offshore-Ausschreibungen, A-Windprojekte in Europa (UK, F, etc.), Randthemen wie Offshore Drone Week, Revisar, etc., Geothermie und Lithiumgewinnung, Strafzölle, Handelsbeschränkungen für Solarmodule und Windkraftanlagenteil",

    "DisponibleErzeugung": "Fuel Switch von Kohle zu Gas später zu Wasserstoff (NICHT: H2-Infrastruktur): Planung, Bau und Inbetriebnahme von (wasserstofffähigen) Gaskraftwerken (inklusive der bereits angelaufenen Projekte in Heilbronn, Altbach/Deizisau und Stuttgart-Münster), Fuel Switch, regelbare Leistung, ggf. Berichterstattung über weitere A-Projekte, Diskussion um Kraftwerkstrategie, KWKG und Förderregime, Großwärmepumpen, A in der Verantwortung für sichere Strom-, Gas- und Fernwärmeversorgung von Kund*innen; Vorbereitungen für Krisensituationen; Bevorratung/Beschaffung von Gas, LNG, Kohle usw. Sämtliche Berichterstattung über Großbatteriespeicher oder Pumpspeicherkraftwerke (z. B. Forbach). Keine Berichte über Kernenergie/Atomenergie!",

    "IntelligenteStromnetze": "Netzausbau auf allen Ebenen, Innovative Projekte („Netzlabore“) im Stromnetz speziell der Netze BW; Roll out intelligente/digitale Zähler; Projekt „Selbstheilende Netze“, Diskussion um politische Rahmenbedingungen (z.B. Freileitungen vs. Erdkabel), Netzentwicklungsplan, Umspannwerke, jegliche Neubauprojekte der Netze BW, Äußerungen/politische Forderungen des Vorstands zum Thema allg. (z.B. Südlink, Ultranet oder neuer Trassen), Diskussionen über Redispatch-Maßnahmen, etc.",

    "AAlsArbeitgeberIn": "Kommunikation der A als ein Unternehmen, dass nicht nur attraktive Jobs und attraktive Benefits hat, sondern sich dem allgemeinen Trend widersetzt, in dem sie Menschen einstellt, Perspektiven bietet und Sicherheit; Im engeren Sinne: Kommunikation von A mit Bezug auf die A als Arbeitgeber*in (Arbeitsmarkt, Recruiting, Ausbildung und duales Studium, Berufsintegrationsprogramm für Geflüchtete und Migrant*innen, Diversity-Strategie, BestWork, (Führen in) Teilzeit, Maßnahmen zur Fachkräftegewinnung, Weiterentwicklungsangebote und -möglichkeiten, Mitarbeiterbindung (Retention), Benefits, Berichte über Mitarbeitende und ihre Arbeit bei der A. Digitalisierungsprojekte, die die Jobs attraktiver machen, egal, ob diese bei HR oder in Fachbereichen angesiedelt sind (z. B. Drohnenführerschein, AR-, VR- oder KI-Anwendungen usw.)",

    "NachhaltigkeitCSRESG": "Themen wie z. B. Net Zero-Ziele, Dekarbonisierung, Berichterstattung nach ESG/CSRD, Spenden; gesellschaftliche Stellungnahmen; Zertifizierungen für Klima-, Umwelt- und sonstige Standards, gesellschaftliches Engagement (Sponsorings, Macherbus, Zeitung in der Schule etc.), A-Nachhaltigkeitsagenda 2.0, A-Nachhaltigkeitsdialog, Klimaneutralitätsziel 2035 (Fuel Switch, Kohleausstieg 2028), Ziele und Zertifizierungen zu Net Zero, Green Finance zB Green Bonds der A.",

    "MarkeA": "Kommunikation die Werbe-Kampagnen über die Marke A erwähnt. Bspw. die Kampagne „Unsere Energie bewegt was“, die Kampagnen mit Nico Rosberg und Elif oder EM-Traineraktion (Sommer 2024)",
    
}

# COMMAND ----------

# DBTITLE 1,OpenAI Function
AZURE_OPENAI_API_KEY = get_secret('azure-openai-api-key')

def get_openai_client(api_key, endpoint="https://api.competence-cente-cc-genai-prod.A-az.cloud/openai"):
    client = AzureOpenAI(
        api_key=api_key,
        api_version="2024-10-21",
        base_url=endpoint,
    )
    return client

class UnscorableCommentError(Exception):
    pass

# COMMAND ----------

# DBTITLE 1,Abstract and Tonalitaet Functions
def generate_abstract_prompt_messages(s: str) -> List[Dict]:
    return [
        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die A hat. Wenn in dem Text Wettbewerber von A wie B, C, D oder E erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der A und oder der CEO beziehungsweise der Vorstandsvorsitzende von A erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die A anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Neues Leben für alte Batterien - A und Audi testen in einem gemeinsamen Projekt, wie alte Antriebsbatterien aus E-Autos weiter genutzt werden können. Dafür wird ein stationärer Batteriespeicher mit Akkus befüllt, die aus zerlegten Testfahrzeugen stammen. Das Aggregat besteht aus zwölf Hochvolt-Batteriesystemen. Zusammengeschaltet bringen sie es auf eine Gesamtleistung von einem MW. In den kommenden Wochen wird zunächst die Leistungsfähigkeit des Speichers geprüft und verschiedene Einsatzszenarien simuliert. Dazu zählt auch die Bereitstellung von Regelleistung für das Stromnetz. Umgekehrt wird bei einem Uberangebot von Wind- oder PV-Anlagen Strom gespeichert. Außerdem wird untersucht, wie die Speicherkapazität am Energiemarkt eingesetzt werden kann, falls günstiger Strom aus erneuerbaren Energien vorhanden ist. In ihrem »zweiten Leben« haben die Zellen nach Ansicht der Experten eine Einsatzzeit von mindestens fünf bis zehn Jahren. Danach führt Audi die Batterien einem endgültigen Recycling zu. Dabei werden sie in ihre einzelnen Bestandteile und Rohstoffe zerlegt, um perspektivisch wieder in neuen Batterien zum Einsatz zu kommen. Die Anlage dient als Referenz für zunächst vier Projekte, die bei A derzeit für die nähere Zukunft geplant sind.
            """.strip(),
        },
        {
            "role": "assistant", 
            "content": """
            In einer Kooperation zwischen A und Audi wird die Weiterverwendung alter Antriebsbatterien aus E-Autos in einem stationären Batteriespeicher untersucht. Dieser Speicher, wird zunächst hinsichtlich seiner Leistungsfähigkeit und diverser Einsatzmöglichkeiten getestet. Dazu gehört die Bereitstellung von Regelleistung für das Stromnetz und die Speicherung von Überschussstrom aus erneuerbaren Energiequellen. Ziel ist es auch, die Speicherkapazität gewinnbringend am Energiemarkt zu nutzen. Die Batterien sollen nach ihrer Wiederverwendung, die fünf bis zehn Jahre andauern könnte, durch Audi recycelt werden. Dieses Projekt dient als Modell für vier weitere Vorhaben, die bei A geplant sind, und ist somit von strategischer Bedeutung für die nachhaltige Energieverwaltung und den Ausbau der Energieeffizienz im Unternehmen.

            Tonalität: +0.9
            """
        },

        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die A hat. Wenn in dem Text Wettbewerber von A wie B, C, D oder E erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der A und oder der CEO beziehungsweise der Vorstandsvorsitzende von A erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die A anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Ökostrom-Kunden gucken in die Röhre Strom aus erneuerbaren Energien ist genauso teuer wie aus fossilen Quellen. Daran sind die Mechanismen der Energiemärkte schuld VON CATHARINA SCHUL catharina.schulz@suedkurier.de Freiburg – Als der Gaspreis nach dem Beginn des Kriegs in der Ukraine die Strompreise in die Höhe trieb, wähnten sich viele Verbraucher auf der sicheren Seite, deren Strom aus Sonne, Wind und Co. stammt. Doch sie lagen falsch. Zahlreiche Versorger haben mittlerweile die Preise für Ökostrom erhöht. So gibt der deutsch-schweizerische Energieversorger Energiedienst auf Nachfrage an, 100 000 Kunden seien von einer Preiserhöhung betroffen, um durchschnittlich 40 Prozent. Bei Deutschlands drittgrößtem Energieversorger A aus Karlsruhe müssten Kunden für Ökostrom im Schnitt 15,2 Prozent mehr zahlen als im Vorjahr. Auch ein SÜDKURIER-Leser aus Hilzingen berichtet, dass ihm Energiedienst eine Preiserhöhung angekündigt hat. Obwohl er zu 100 Prozent Strom aus erneuerbaren Energien bezieht, müssten er und andere umweltbewusste Nutzer die gestiegenen Kosten für fossile Energieträger mit bezahlen, kritisiert er. Mit der Annahme hat er recht. Die Preise für Stromlieferungen entstehen an den Handelsplätzen wie der Leipziger Strombörse EEX. Hier treffen sich Anbieter wie Kraftwerksbetreiber oder Großhändler und Nachfragende wie Stromlieferanten oder Industriebetriebe. Der Einheits-Börsenpreis wird über die sogenannte Merit-  Order (englisch: Reihenfolge der Vorteilhaftigkeit) gebildet. Bei diesem System richtet sich der Strompreis nach dem teuersten Angebot im Markt, das gebraucht wird, um den Bedarf zu decken. Zuletzt waren das  häufig die teuren Gaskraftwerke. Da die Preise für Kohle und Gas gestiegen sind, steigen die Stromkosten – auch für Ökostrom. Selbst direkte oder regionale Lieferverträge werden vom Börsenpreis geprägt, erklärt der Energiedienst-Sprecher Christoph Klink. Jeder Ökostromanbieter hängt am Börsenpreis: Will er zu einem höheren Preis als dem Börsenpreis einkaufen, hat er einen Nachteil anderen Anbietern gegenüber. Versucht er, zu niedrigeren Preisen einzukaufen, würden die Betreiber von Windrädern und Photovoltaik-Anlagen sich Abnehmer suchen, die den teureren Börsenpreis zahlen oder direkt an die Strompreisbörse gehen. Energiedienst müsse zudem auch Strom von anderen Hochrhein-Wasserkraftwerken zu Börsenpreisen einkaufen, so Klink weiter. „Wir wissen, dass die allgemein gestiegenen Energiekosten insgesamt eine echte Mehrbelastung für die Verbraucher bedeuten“, sagt A-Sprecher Rashid Elshahed. Doch gerade die Energiekrise zeige, wie wichtig die nachhaltige Energiewende hin zu erneuerbaren Energien sei. Wer sich für Ökostrom entscheide, unterstütze den Ausbau aktiv. Auch wenn er preislich dadurch keinen Vorteil hat. Preisdruck entsteht zudem durch die Netzentgelte. Mit ihnen wird der Ausbau und Betrieb der Energienetze finanziert. Nach einer Auswertung des Verbraucherportals Verivox steigen die Entgelte 2023 um fast 19 Prozent. 2022 betrugen sie rund acht Cent je Kilowattstunde und damit knapp 20 Prozent des Endkundenstrompreises. Zwar soll ein Bundeszuschuss die Netzentgelte 2023 stabil halten, klar ist aber schon jetzt, dass das Geld nicht reichen wird. Ökostrom ist zwar günstig in der Produktion, aber teuer für die Verbraucher.
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Trotz der Erzeugung aus erneuerbaren Quellen wie Sonne und Wind sind die Kosten für Ökostrom gestiegen, was auf die Preisbildung an der Leipziger Strombörse EEX zurückzuführen ist. Dort wird der Einheitspreis durch das teuerste benötigte Angebot bestimmt, zuletzt häufig durch Gaskraftwerke. Diese Mechanik führt dazu, dass auch die Preise für Ökostrom ansteigen, da alle Anbieter, einschließlich A, am Börsenpreis hängen. A hat die Preise für Ökostrom um durchschnittlich 15,2 Prozent erhöht. Dies betrifft nicht nur A, sondern auch andere Anbieter wie Energiedienst. Der Artikel betont, dass trotz höherer Kosten die Wahl von Ökostrom den Ausbau erneuerbarer Energien unterstützt und somit langfristig zur Energiekrise beiträgt. Die Bedeutung für A liegt in der Herausforderung, die Energiewende zu fördern, während sie gleichzeitig finanzielle Nachhaltigkeit sicherstellen müssen.

            Tonalität: +0.1
            """
        },
        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die A hat. Wenn in dem Text Wettbewerber von A wie B, C, D oder E erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der A und oder der CEO beziehungsweise der Vorstandsvorsitzende von A erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die A anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            Region macht Tempo bei Solarenergie HEILBRONN Bis Jahresende sollen zusätzliche Flächen und Erleichterungen feststehen Sechs Großprojekte erhalten Zuschlag Heiko Fritze Der Ausbau der FreiflächenPhotovoltaik in HeilbronnFranken soll sich massiv beschleunigen. Soeben hat die Bundesnetzagentur in der jüngsten Ausschreibungsrunde sechs Projekten in der Region den Zuschlag erteilt Fünf davon sollen auf Wiesen- oder Ackerflächen errichtet werden, eines im Öhringer Gewerbegebiet Ost. Sie haben zusammen eine Fläche von 32 Hektar. Millioneninvestitionen Der Zuschlag ging an Investoren, die bei Forchtenberg, Braunsbach, Niedersteffen, Schwäbisch Hall und Weikersheim bauen wollen. Neben Landwirten erhielten auch der Energieversorger Zeag und die Stadtwerke Schwäbisch Hall je einen Zuschlag. Die Zeag hatte zudem mit einem Projekt bei Billigheim (NeckarOdenwald-Kreis) Erfolg. Die A-Tochter will in beide Vorhaben insgesamt 12,5 Millionen Euro investieren. Um die Voraussetzungen für weitere Anlagen zu schaffen, sammelt der Regionalverband gerade von allen 111 Kommunen in der Region Daten über bereits vorhandene Photovoltaik. Zwar weiß die Behörde von 75 Bebauungsplänen mit 314 Hektar Fläche und weiteren 41 Planverfahren mit 354 Hektar, doch können sich die Daten nach der Bearbeitung durch den Regionalverband nochmals geändert haben, erläutert der Direktor des Regionalverbands, Klaus Mandel. Zwei-Prozent-Ziel Die Daten sollen Basis für eine Teilfortschreibung des Flächennutzungsplans werden: Bis Ende des Jahres will der Regionalverband die Bedingungen für Freiflächen-Photovoltaik erleichtern. Dann sollen auch in regionalen Grünzügen bis zu zehn Hektar große Anlagen möglich sein. Außerdem soll es Ausnahmen geben, wenn in der Nähe großer Stromverbraucher wie etwa Industriebetrieben gebaut werden soll, kündigt Mandel an. Die nötigen Kriterienkataloge sollen noch im Frühjahr vorliegen, sagt er. Dass das Tempo beschleunigt wird, liegt auch an dem Zwei-Prozent-Ziel, das die Bundesregierung vergangenes Jahr in Gesetzesform gegossen hat: Demnach sollen zwei Prozent der Flächen in allen Bundesländern mit Anlagen für Erneuerbare Energien, also Windrädern und Photovoltaik, bebaut werden. Die baden-württembergische Landesregierung hat für alle Regionen, also auch Heilbronn-Franken, als Ziel 1,8 Prozent der Fläche für Windkraft ausgegeben. Ob nur die übrigen 0,2 Prozent von Photovoltaik übernommen werden müssen oder sogar noch mehr, stehe noch nicht fest, berichtet Mandel. Zwei Prozent entsprechen jedenfalls 9530 Hektar, jene 0,2 Prozent also 953 Hektar und damit mehr als alle bislang vorhandenen und in Planung befindlichen Photovoltaik-Flächen zusammen einnehmen. Mehr Tempo Um die Ziele der Energiewende rechtzeitig zu erreichen, müssten nach Berechnungen des Energieversorgers A täglich in Deutschland sieben Windräder errichtet und 117 Fußballfelder mit Photovoltaik belegt werden wobei hier auch Dachflächen einbezogen sind. Diese Zahlen präsentierte jüngst Andreas Pick, Gesamt-Projektleiter für Energiewende-Projekte bei der A. Alleine für die Versorgung der Stadt Heilbronn mit Solarthermie werde eine Fläche von 170 bis 200 Fußballfeldern benötigt, sagte er. Die A will am Heilbronner Kohlekraftwerk, das 2026 stillgelegt werden soll, eine Gasturbine errichten. Meinung „Bremsklötze weg“ Ziele bis 2030 Ziel der Bundesregierung ist es, den Anteil des aus erneuerbaren Energien erzeugten Stroms am Stromverbrauch bis 2030 auf 80 Prozent zu steigern. Im vergangenen Jahr waren es nach ersten Branchenberechnungen rund 47 Prozent. Wer in Deutschland Wind- oder Solarparks mit staatlicher Förderungauen will, kann in Ausschreibungen Gebote ab- geben. Die Höchstwerte sollen nun deutlich angepasst werden, um zusätzliche Anreize zu schaffen. Außerdem wurden die Anreize für Privathaushalte, in Photovoltaik zu investieren, deutlich erhöht. So sinkt der Mehrwertsteuersatz ab 2023 auf null Prozent. Der Ertrag muss demnach in der Regel nicht mehr als Einkommen versteuert werden.
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Der Ausbau der Solarenergie in der Region Heilbronn-Franken wird durch die Bundesnetzagentur beschleunigt, die sechs Großprojekte für Freiflächen-Photovoltaikanlagen genehmigt hat. Diese Projekte, deren Investition von verschiedenen Akteuren, einschließlich der A-Tochter, getragen wird, sind Teil eines größeren Plans, um die gesetzlichen Ziele der Bundesregierung zu erreichen. A plant, 12,5 Millionen Euro in zwei Projekte zu investieren, um die Versorgung mit erneuerbarer Energie zu stärken. Diese Entwicklung ist für A von großer Bedeutung, da sie aktiv zur Erreichung des Bundesziels beiträgt, bis 2030 den Anteil erneuerbarer Energien auf 80% zu steigern. Zudem zeigt dies A's Engagement in der Energiewende und deren Bemühungen, ihre Energieproduktion nachhaltiger zu gestalten. Ein weiteres wichtiges Thema ist die Anpassung der Ausschreibungsbedingungen und steuerlichen Anreize, um den Ausbau von Photovoltaik und Windenergie zu fördern.

            Tonalität: +0.8
            """
        },

        {
            "role": "user",
            "content": """
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die A hat. Wenn in dem Text Wettbewerber von A wie B, C, D oder E erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der A und oder der CEO beziehungsweise der Vorstandsvorsitzende von A erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages mit Bezug auf die A anhand einer Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            KLÄRSCHLAMMVERBRENNUNG Gemeinde sieht einen Teilerfolg Die Gegner der geplanten Klärschlammverbrennungsanlage auf dem Gelände der A in Walheim wird diese Nachricht freuen: Die A darf derzeit Eidechsen umsiedeln mehr darf sie nicht. Alle weiteren beantragten Maßnahmen hat das Regierungspräsidium abgelehnt. WALHEIM VON ANDREA NICHT-ROTH Noch bevor das Genehmigungsverfahren zur Errichtung des Klärschlammheizkraft-werks abgeschlossen ist, hatte die A Anfang Juli beantragt, vorzeitig drei Maß-nahmen ergreifen zu dürfen: Zum einen sollen Zaun und Mauereidechsen umgesiedelt werden, zum anderen sollen Gehölzflächen gerodet werden und Schilfröhricht soll entfernt werden. So lautete jedenfalls der Antrag der A. Nach „sorgfältiger Prüfung" hat die Immissionsschutzbehörde beim Regierungspräsidium Stuttgart diesen Antrag nun jedoch in weiten Teilen abgelehnt. Das Regierungspräsidium erlaubt lediglich die Umsetzung der am Standort lebenden Eidechsen; gefällt und gerodet werden darf nichts. Die Umsiedlung der Eidechsen jedoch sei nicht von der Veränderungssperre erfasst, die die Gemeinde Walheim über das Gebiet gelegt hat. Die weiteren natur-schutzfachlichen Maßnahmen jedoch das Roden von Gehölz und Röhrichtflächen, die von der A beantragt worden waren fallen nach Einschätzung des Regierungspräsidiums unter die Veränderungssperre und werden daher nicht zugelassen. So werde sichergestellt, dass keine irreversiblen Maßnahmen umgesetzt werden, heißt es in der Pressemitteilung. „Wir prüfen den Antrag der Vorhabenträgerin auf Errichtung eines Klärschlammheizkraftwerkes in Walheim sehr sorgfältig dies haben wir auch nochmals im Erörterungstermin Ende Juni betont. Daher haben wir nach umfassender rechtlicher Prüfung den jetzigen Antrag auf vor-zeitigen Baubeginn für zwei der drei beantragten naturschutzfachlichen Maßnahmen abgelehnt, um vor der endgültigen Entscheidung über die Veränderungssperre durch den Verwaltungsgerichtshof so-wie über den Bau des Klärschlammheiz-kraftwerkes keine Fakten zu schaffen", betonte eine Sprecherin des Regierungspräsidiums. Weil die Gemeinde Walheim unter allen Umständen die geplante Klärschlammverbrennungsanlage verhindern will, hat der Gemeinderat Ende Juli die Aufstellung des Bebauungsplans „Mühlwiesen/Mühlstraße" beschlossen. Das sagt die Gemeinde Walheim. Auch über eine Veränderungssperre des Gebiets war abgestimmt worden. Diese Entscheidung hatte noch der alte Gemeinderat getroffen, das neu gewählte Gremium hat sie kürzlich mit zehn Ja und einer Nein-Stimme bekräftigt. Die Veränderungssperre steht einer Genehmigung aktuell entgegen. Der Genehmigungsantrag befindet sich weiterhin in Prüfung durch das Regierungspräsidium Stuttgart. Die A hat gegen die Veränderungs-sperre beim Verwaltungsgerichtshof Ba-den-Württemberg in Mannheim ein Normenkontrollverfahren sowie einen Antrag auf Eilrechtsschutz gestellt. Bei der Gemeinde Wahlheim sieht in der Entscheidung des Regierungspräsidiums „einen Teilerfolg, und dass man unser An-liegen ernst nimmt", sagt der Erste stell-vertretende Bürgermeister Wilhelm Weiß: „Wir fühlen uns durch diese Entscheidung in unserem Anliegen bestärkt." Nun hoffe die Gemeinde, dass die restlichen immissionsschutzrechtlichen Belange ebenso sorgfältig geprüft „und in unserem Sinn entschieden werden", sagt Weiß im Gespräch mit unserer Zeitung. Die Bürgerinitiative (BI) Bürger im Neckartal sieht zwar auch „einen kleinen Hoffnungsschimmer", Matthias Appelt von der BI kommentiert die Entscheidung aber eher nüchtern: „Das hat nicht so viel zu sagen; das Regierungspräsidium konnte eigentlich gar nicht anders entscheiden, es ist die logische Konsequenz aus der Veränderungssperre." Trotzdem freue er sich natürlich, dass die Behörde diese Aussage gemacht habe. Ganz anders die A. Zur Entscheidung des Regierungspräsidiums erklärt A-Projektleiter Andreas Pick: „Die A bedauert, dass das Regierungspräsidium unseren Antrag in mehreren Punkten abgelehnt hat, respektiert die Entscheidung aber natürlich. Wir begrüßen, dass zumindest die Umsiedlung von Eidechsen genehmigt worden ist. Wir sehen dem weiteren Fortgang des Verfahrens zuversichtlich entgegen und hoffen, dass das von uns angestrengte Eilverfahren eine zeitnahe Entscheidung des Verwaltungs-gerichtshofes über die von der Gemeinde Walheim erlassene Veränderungssperre bringt." Der Landtagsabgeordnete der Grünen im Wahlkreis Bietigheim-Bissingen, Tay-fun Tok, kommentiert die Entscheidung folgendermaßen: „Das Regierungspräsidium verbietet der A am Standort Walheim vorzeitige Veränderungen an Gehölzen und Grünanlagen. Damit zeigt die Be-hörde, dass der Konzern sich wie jeder andere an Recht und Gesetz halten muss und keine Genehmigungen per Freibrief erhält. Herr des Verfahrens ist die Landesbehörde. Darauf kann man in einem Rechtsstaat vertrauen." Hier soll eine Klärschlammverbrennungsanlageentstehen. Die Gemeinde wehrt sich dagegen. Foto, Alfred Drossel Auch erschienen in Ludwigsburger Kreiszeitung | 15.08.2024 | Online
            """.strip(),
        },
        {
            "role": "assistant",
            "content": """
            Die A plant eine Klärschlammverbrennungsanlage in Walheim, stößt jedoch auf Widerstand von der lokalen Gemeinde und einer Bürgerinitiative. Das Regierungspräsidium Stuttgart hat kürzlich mehrere Anträge der A für vorzeitige Maßnahmen, darunter Rodungen und Entfernung von Schilfröhricht, abgelehnt, lediglich die Umsiedlung von Eidechsen wurde genehmigt. Diese Entscheidung ist ein Rückschlag für A, die bereits rechtliche Schritte gegen die von der Gemeinde erlassene Veränderungssperre eingeleitet hat. Dieser Fall ist für A von Bedeutung, da er die Herausforderungen bei der Durchsetzung von Infrastrukturprojekten unterstreicht und die Notwendigkeit aufzeigt, sowohl rechtliche als auch kommunale Hürden zu überwinden. Die Entscheidung des Regierungspräsidiums zeigt, dass A sich an die gesetzlichen Vorgaben halten muss und keine Sonderbehandlung erwarten kann.

            Tonalität: -0.5
            """
        },

        {
            "role": "user",
            "content": f"""
            Schreibe einen Abstract des folgenden Textes. Der Abstract soll maximal 1000 Zeichen haben. Beantworte innerhalb des Abstracts auch die Frage, welche Bedeutung der Inhalt des Textes für die A hat. Wenn in dem Text Wettbewerber von A wie B, C, D oder E erwähnt werden, dann fasse zusätzlich auch die Aussagen zu diesen Wettbewerbern zusammen. Wenn in dem Text auch Vorstandsmitglieder der A und oder der CEO beziehungsweise der Vorstandsvorsitzende von A erwähnt werden muss dies auf jeden Fall in dem Abstract erwähnt werden. Wenn in dem Text mehr als ein überwiegendes Hauptthema behandelt wird, nenne auch das zweite wichtige Thema in dem Inhalt. Zum Schluss analysiere bitte gesondert die Tonalität des Beitrages anhand der Skala von minus 1 bis plus 1 und gebe den genauen Score der Analyse aus.
            
            Dies ist der Text:
            
            {s}
            """.strip(),
        },
    ]

#  Function: Call LLM for abstract generation
def generate_abstract(s: str, client: AzureOpenAI, model: str = "gpt-4") -> str:
    if s is None:
        return None
    try:
        messages = generate_abstract_prompt_messages(s)
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
        print(f"{datetime.now()}: Generated Abstract -> '{abstract}'")
        # Raise an Attribute Error if abstract could not be generated
        return str(abstract)
    except AttributeError as e:
        print(f"Could not generate abstract: {s}, {e}")
    except Exception as e:
        print(f"e: {e}")
        print(f"response: {response}")
        # raise e

def generate_abstracts_local(df, id_col, fulltext_col):
    client = get_openai_client(AZURE_OPENAI_API_KEY)

    schema = T.StructType([
        T.StructField(id_col, T.StringType(), True),
        T.StructField("GeneratedAbstract", T.StringType(), True),
        T.StructField("GeneratedTonalitaet", T.StringType(), True),
        T.StructField("Generatedrecompute_abstract", T.BooleanType(), True),
    ])
    
    buffer = []

    df_res = spark.createDataFrame([], schema=schema)

    for row in df.toLocalIterator():
        print(f"{datetime.now()}: '{row.asDict()[id_col]}'")
        if not row.asDict()['skip'] and row.asDict()['compute_abstract']:
            print(f"{datetime.now()}: Generating Abstract for '{row.asDict()[id_col]}'")
            abstract  = generate_abstract(row.asDict()['Text'], client)
            cleaned_abstract = re.sub(r"Tonalität: ([-+]?\d*\.?\d+)\s*(.*)", "", abstract).strip()
            tonalitaet = re.search(r"Tonalität: ([-+]?\d*\.?\d+)", abstract)
            if tonalitaet:
                tonalitaet_text = tonalitaet.group(1)
            else:
                tonalitaet_text = None
                print("Tonalität nicht gefunden.")
            compute_abstract = False
        else:
            print('skip')
            cleaned_abstract = None
            tonalitaet_text = None
            compute_abstract = row.asDict()['compute_abstract']

        buffer.append((row.asDict()[id_col], cleaned_abstract, tonalitaet_text, compute_abstract, ))
        if len(buffer) > 5:
            print('flush buffer')
            df_res = df_res.union(spark.createDataFrame(buffer, schema = df_res.schema))
            buffer = []

    df_res = df_res.union(spark.createDataFrame(buffer, schema = df_res.schema))
    df = df.join(df_res, on=id_col, how="left")

    for gen_col in df.columns:
        if gen_col.startswith("Generated"):
            df = df.withColumn(gen_col[len("Generated"):], F.coalesce(F.col(gen_col), F.col(gen_col[len("Generated"):])))
            df = df.drop(gen_col)
    return df

def select_abstract_columns():
    return [
        F.col("Abstract").alias("TransformedAbstract"),
        F.col("Tonalitaet").alias("TransformedTonalitaet"),
        F.col("recompute_abstract"),
    ]

def select_abstracts_or_none(df):
    df = df.withColumn("Abstract", F.lit(None).cast(T.StringType()))
    df = df.withColumn("Tonalitaet", F.lit(None).cast(T.StringType()))
    df = df.withColumn("recompute_abstract", F.lit(True))
    return df

# COMMAND ----------

# DBTITLE 1,Themenbereiche and strategic areas Functions

tag_cols = {
    "Themenbereich1": T.StringType(),
    "Themenbereich2": T.StringType(),
    "Themenbereich3": T.StringType(),
    "Themenbereich1_Conf": T.IntegerType(),
    "Themenbereich2_Conf": T.IntegerType(),
    "Themenbereich3_Conf": T.IntegerType(),
    "Strategie2030": T.DoubleType(),
    "FinanzierungEnergiewende": T.DoubleType(),
    "EMobilitaet": T.DoubleType(),
    "VernetzeEnergiewelt": T.DoubleType(),
    "TransformationGasnetzeWasserstoff": T.DoubleType(),
    "ErneuerbareEnergien": T.DoubleType(),
    "DisponibleErzeugung": T.DoubleType(),
    "IntelligenteStromnetze": T.DoubleType(),
    "AAlsArbeitgeberIn": T.DoubleType(),
    "NachhaltigkeitCSRESG": T.DoubleType(),
    "MarkeA": T.DoubleType(),
}

def generate_subject_area_tags_prompt_messages(abstract: str) -> List[Dict]:
    return [
        {
            "role": "user",
            "content": f"""
                Es gibt folgende Themenfelder:
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
                Posts über FuelSwitch gehören zu den Erneuerbaren Energien.
                Antworte mit einer | separierten Liste und gib für jede Kategorie in Klammern in Prozent an wie sicher du dir bist.
                Wenn keines der Themenfelder passt, dann gib nichts aus.

                Beispiel Output: Elektromobilität (100%)|Erneuerbare Energien (80%)|Finanzen/M&A (50%)
                
                Gib 0, 1, 2 oder 3 Themenfelder an, die zu diesem Post passen:
                {abstract}
            """,
        },
    ]

def generate_subject_area_tags(abstract: str, client: AzureOpenAI, model: str = "gpt-4") -> str:
    if abstract is None:
        return None
    try:
        messages = generate_subject_area_tags_prompt_messages(abstract)
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
        result = response.choices[0].message.content.strip()
        print(f"{datetime.now()}: Generated Subject Areas. {abstract[:50]} -> {result}")
        # Raise an Attribute Error if result could not be generated
        return str(result)
    except AttributeError as e:
        print(f"Could not generate tags: {abstract[:50] if abstract is not None else None}, {e}")
    except Exception as e:
        print(f"e: {e}")
        print(f"response: {response}")
        # raise e
    return None

def generate_strategic_tags_prompt_messages(abstract: str, strategic_area: str) -> list[dict]:
    return [
        {
            "role": "user",
            "content": f"""
                Die Beschreibung eines Tags lautet:

                {strategic_areas[strategic_area]}

                Gib nur eine Zahl zwischen 0 und 1 aus, die Angibt wie sehr die Beschreibung zu dem Post passt.

                Post:
                {abstract}
            """,
        },
    ]

def generate_strategic_tags(abstract: str, strategic_area: str, client: AzureOpenAI, model: str = "gpt-4") -> str:
    if abstract is None:
        return None
    try:
        messages = generate_strategic_tags_prompt_messages(abstract, strategic_area)
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
        result = response.choices[0].message.content.strip()
        print(f"{datetime.now()}: Generated strategic area: {strategic_area} -> {result}")
        # Raise an Attribute Error if result could not be generated
        return float(result)
    except AttributeError as e:
        print(f"Could not generate tags: {abstract[:50] if abstract is not None else None}, {e}")
    except Exception as e:
        print(f"e: {e}")
        # print(f"response: {response if response is not None else None}")
        # raise e
    return None

def generate_tags_local(df, id_col, text_col):
    client = get_openai_client(AZURE_OPENAI_API_KEY)

    schema = T.StructType(
        [ T.StructField(id_col, T.StringType(), True), ] +
        [ T.StructField("Generated" + c, tag_cols[c], True) for c in tag_cols ] +
        [ T.StructField("Generatedrecompute_tags", T.BooleanType(), True), ]
    )

    buffer = []

    df_res = spark.createDataFrame([], schema=schema)

    for row in df.toLocalIterator():
        print(f"{datetime.now()}: Tags '{row.asDict()[id_col]}', '{row.asDict()['skip']}'', '{'Themenbereich1' in row.asDict()}', '{row.asDict()[text_col] is not None}'")
        if not row.asDict()['skip'] and row.asDict()['compute_tags'] and row.asDict()[text_col] is not None:
            try:
                print(f"{datetime.now()}: Generating Subject Areas for '{row.asDict()[id_col]}'")
                raw = generate_subject_area_tags(row.asDict()[text_col], client)
                if raw is not None:
                    print(f"{datetime.now()}: Unpacking Tags for '{raw}'")
                    raw = raw.split("|")
                    confs = list(map(lambda x: re.search(r"\(([0-9]+)\%\)$", x), raw))
                    confs = list(map(lambda x: int(x.group(1)) if x is not None else None, confs))
                    
                    subjectarea1_conf, subjectarea2_conf, subjectarea3_conf, *_ = confs + [None] * 3

                    tags = list(map(lambda x: re.search(r"^(.*?)\s*\(\d+%?\)$", x), raw))
                    tags = list(map(lambda x: x.group(1) if x is not None else None, tags))
                    subjectarea1, subjectarea2, subjectarea3, *_ = tags + [None] * 3
                    print(f"{datetime.now()}: Generated Subject Areas for '{row.asDict()[id_col]}'")
                else:
                    subjectarea1_conf, subjectarea2_conf, subjectarea3_conf = None, None, None
                    subjectarea1, subjectarea2, subjectarea3 = None, None, None
            except Exception as e:
                raise e
                subjectarea1_conf, subjectarea2_conf, subjectarea3_conf = None, None, None
                subjectarea1, subjectarea2, subjectarea3 = None, None, None

            try:
                strategie2030 = generate_strategic_tags(row.asDict()[text_col], "Strategie2030", client)
                finanzierungenergiewende = generate_strategic_tags(row.asDict()[text_col], "FinanzierungEnergiewende", client)
                emob = generate_strategic_tags(row.asDict()[text_col], "EMobilitaet", client)
                vernetzteenergie = generate_strategic_tags(row.asDict()[text_col], "VernetzeEnergiewelt", client)
                transformgaswasser = generate_strategic_tags(row.asDict()[text_col], "TransformationGasnetzeWasserstoff", client)
                ee = generate_strategic_tags(row.asDict()[text_col], "ErneuerbareEnergien", client)
                dispoerzeug = generate_strategic_tags(row.asDict()[text_col], "DisponibleErzeugung", client)
                intelligentestrom = generate_strategic_tags(row.asDict()[text_col], "IntelligenteStromnetze", client)
                Aalsarbeitgeber = generate_strategic_tags(row.asDict()[text_col], "AAlsArbeitgeberIn", client)
                nachhaltigkeit = generate_strategic_tags(row.asDict()[text_col], "NachhaltigkeitCSRESG", client)
                marke = generate_strategic_tags(row.asDict()[text_col], "MarkeA", client)
            except Exception as e:
                raise e
            compute_tags = False
        else:
            print('skip')
            subjectarea1 = None
            subjectarea2 = None
            subjectarea3 = None
            subjectarea1_conf = None
            subjectarea2_conf = None
            subjectarea3_conf = None
            strategie2030 = None
            finanzierungenergiewende = None
            emob = None
            vernetzteenergie = None
            transformgaswasser = None
            ee = None
            dispoerzeug = None
            intelligentestrom = None
            Aalsarbeitgeber = None
            nachhaltigkeit = None
            marke = None
            compute_tags = row.asDict()["compute_tags"]

        buffer.append(
            (
                row.asDict()[id_col],
                subjectarea1, subjectarea2, subjectarea3,
                subjectarea1_conf, subjectarea2_conf, subjectarea3_conf,
                strategie2030, finanzierungenergiewende, emob, vernetzteenergie, transformgaswasser,
                ee, dispoerzeug, intelligentestrom, Aalsarbeitgeber, nachhaltigkeit, marke,
                compute_tags
            )
        )
        if len(buffer) > 5:
            print('flush buffer')
            df_res = df_res.union(spark.createDataFrame(buffer, schema = df_res.schema))
            buffer = []
    
    df_res = df_res.unionByName(spark.createDataFrame(buffer, schema = df_res.schema), allowMissingColumns=True)
    df = df.join(df_res, on=id_col, how="left")

    for gen_col in df.columns:
        if gen_col.startswith("Generated"):
            df = df.withColumn(gen_col[len("Generated"):], F.coalesce(F.col(gen_col), F.col(gen_col[len("Generated"):])))
            df = df.drop(gen_col)
    # df.display()
    return df

def select_tag_columns():
    return [ F.col(c).alias("Transformed" + c) for c in tag_cols] + [ F.col("recompute_tags") ]

def select_tag_columns_youtube():
    return [ F.col(c).alias("Transformed" + c) for c in tag_cols_youtube] + [ F.col("recompute_tags") ]

def select_tags_or_none(df):
    for c in tag_cols:
        df = df.withColumn(c, F.lit(None).cast(tag_cols[c]))
    df = df.withColumn("is_old", F.lit(False))
    df = df.withColumn("recompute_tags", F.lit(True))
    return df

# COMMAND ----------

# DBTITLE 1,Logic and control Functions
def cleanup_logic_columns(df):
    return df.drop("row_num", "skip", "is_old", "compute_tags", "compute_abstract")

def join_pre_computed(df_cleaned, df_transformed, id_col, check_col, generate_cols=['tags', 'abstract']):
    df_transformed = df_transformed.select(
        id_col,
        *select_abstract_columns() if 'abstract' in generate_cols else [],
        *select_tag_columns() if 'tags' in generate_cols else [],
        F.when(F.col("Transformed" + check_col).isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_old"),
        # "recompute_tags",
        # "recompute_abstract",
    )
    df = df_cleaned.join(df_transformed, id_col, "left")
    df = df.withColumn("is_old", F.coalesce(F.col("is_old"), F.lit(False)))
    # df = df.withColumn("recompute_tags", F.coalesce(F.col("recompute_tags"), F.lit(True)))
    # df = df.withColumn("recompute_abstract", F.coalesce(F.col("recompute_abstract"), F.lit(True)))

    for transf_col in df.columns:
        if transf_col.startswith("Transformed"):
            df = df.withColumn(transf_col[len("Transformed"):], F.coalesce(F.col(transf_col), F.lit(None)))
            df = df.drop(transf_col)
    return df

def generate_limited(df, id_col, order_col, limit_n, text_col, generate_cols=['tags', 'abstract'], fulltext_col=None):
    if 'abstract' in generate_cols and 'tags' in generate_cols:
        df_done = df.where((F.col("is_old") == True) & (F.col("recompute_tags") == False) & (F.col("recompute_abstract") == False))
        df_todo = df.where((F.col("is_old") == False) | (F.col("recompute_tags") == True) | (F.col("recompute_abstract") == True))
    elif 'tags' in generate_cols:
        df_done = df.where((F.col("is_old") == True) & (F.col("recompute_tags") == False))
        df_todo = df.where((F.col("is_old") == False) | (F.col("recompute_tags") == True))

    window_spec = Window.orderBy(F.col(order_col).desc())
    df_todo = df_todo.withColumn("row_num", F.row_number().over(window_spec))
    df_todo = df_todo.withColumn("skip", F.when(F.col("row_num") > limit_n, F.lit(True)).otherwise(F.lit(False)))

    # compute tags and abstracts unabhängig von skip -> damit der aktuelle wert in transformed für nächsten Run gespeichert werden kann

    if 'abstract' in generate_cols:
        df_todo = df_todo.withColumn("compute_abstract", (F.col("recompute_abstract") == True) | F.col("recompute_abstract").isNull())
        df_todo = generate_abstracts_local(df_todo, id_col, fulltext_col)
        df_todo = df_todo.drop(fulltext_col)
        df_done = df_done.drop(fulltext_col)
    if 'tags' in generate_cols:
        df_todo = df_todo.withColumn("compute_tags", (F.col("recompute_tags") == True) | F.col("recompute_tags").isNull())
        df_todo = generate_tags_local(df_todo, id_col, text_col)
    df_todo = cleanup_logic_columns(df_todo)
    df_done = cleanup_logic_columns(df_done)

    df = df_done.unionByName(df_todo)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Youtube Themenbereich bestimmen

# COMMAND ----------

# To generate Tags for Youtube
tag_cols_youtube = {
    "Themenbereich1": T.StringType(),
    "Themenbereich2": T.StringType(),
    "Themenbereich3": T.StringType(),
    "Themenbereich1_Conf": T.IntegerType(),
    "Themenbereich2_Conf": T.IntegerType(),
    "Themenbereich3_Conf": T.IntegerType()
}

def generate_limited_youtube(df: DataFrame, id_col: str, order_col: str, limit_n: int, text_col: str, generate_cols=['tags'], fulltext_col=None) -> DataFrame:
    """
    Looks witch rows need to be generated tags and generates them with the function generate_tags_local_youtube
    Input:
    df: The dataframe to generate tags for
    id_col: The column containing the id of the df
    order_col: The column to order the df
    text_col: The column containing the videodescription of the df
    generate_cols: Decided what should be generate: here just the tags 
    return: 
        The dataframe with the generated tags or abstracts
    """

    df_done = df.where((F.col("is_old") == True) & (F.col("recompute_tags") == False))
    df_todo = df.where((F.col("is_old") == False) | (F.col("recompute_tags") == True))

    window_spec = Window.orderBy(F.col(order_col).desc())
    df_todo = df_todo.withColumn("row_num", F.row_number().over(window_spec))
    df_todo = df_todo.withColumn("skip", F.when(F.col("row_num") > limit_n, F.lit(True)).otherwise(F.lit(False)))

    # compute tags indepentend of skip -> damit der aktuelle wert in transformed für nächsten Run gespeichert werden kann

    df_todo = df_todo.withColumn("compute_tags", (F.col("recompute_tags") == True) | F.col("recompute_tags").isNull())
    df_todo = generate_tags_local_youtube(df_todo, id_col, text_col)
    df_todo = cleanup_logic_columns(df_todo)
    df_done = cleanup_logic_columns(df_done)

    df = df_done.unionByName(df_todo)
    return df
  
def generate_tags_local_youtube(df: DataFrame, id_col: str, text_col: str) -> DataFrame:
    """
    Generates tags for the given dataframe using the OpenAI API
    Input:
    df: The dataframe to generate tags for
    id_col: The column containing the id of the df
    text_col: The column containing the videodescription of the df
    return: 
        The dataframe with the generated tags
    """
    client = get_openai_client(AZURE_OPENAI_API_KEY)

    schema = T.StructType(
        [ T.StructField(id_col, T.StringType(), True), ] +
        [ T.StructField("Generated" + c, tag_cols_youtube[c], True) for c in tag_cols_youtube ] +
        [ T.StructField("Generatedrecompute_tags", T.BooleanType(), True), ]
    )

    buffer = []

    df_res = spark.createDataFrame([], schema=schema)

    for row in df.toLocalIterator():
        print(f"{datetime.now()}: Tags '{row.asDict()[id_col]}', '{row.asDict()['skip']}'', '{'Themenbereich1' in row.asDict()}', '{row.asDict()[text_col] is not None}'")
        if not row.asDict()['skip'] and row.asDict()['compute_tags'] and row.asDict()[text_col] is not None:
            try:
                print(f"{datetime.now()}: Generating Subject Areas for '{row.asDict()[id_col]}'")
                raw = generate_subject_area_tags(row.asDict()[text_col], client)
                if raw is not None:
                    print(f"{datetime.now()}: Unpacking Tags for '{raw}'")
                    raw = raw.split("|")
                    confs = list(map(lambda x: re.search(r"\(([0-9]+)\%\)$", x), raw))
                    confs = list(map(lambda x: int(x.group(1)) if x is not None else None, confs))
                    
                    subjectarea1_conf, subjectarea2_conf, subjectarea3_conf, *_ = confs + [None] * 3

                    tags = list(map(lambda x: re.search(r"^(.*?)\s*\(\d+%?\)$", x), raw))
                    tags = list(map(lambda x: x.group(1) if x is not None else None, tags))
                    subjectarea1, subjectarea2, subjectarea3, *_ = tags + [None] * 3
                    print(f"{datetime.now()}: Generated Subject Areas for '{row.asDict()[id_col]}'")
                else:
                    subjectarea1_conf, subjectarea2_conf, subjectarea3_conf = None, None, None
                    subjectarea1, subjectarea2, subjectarea3 = None, None, None
            except Exception as e:
                raise e
                subjectarea1_conf, subjectarea2_conf, subjectarea3_conf = None, None, None
                subjectarea1, subjectarea2, subjectarea3 = None, None, None

            try:
                print("Stratetic Themen skiped")
            except Exception as e:
                raise e
            compute_tags = False
        else:
            print('skip')
            subjectarea1 = None
            subjectarea2 = None
            subjectarea3 = None
            subjectarea1_conf = None
            subjectarea2_conf = None
            subjectarea3_conf = None
            compute_tags = row.asDict()["compute_tags"]

        buffer.append(
            (
                row.asDict()[id_col],
                subjectarea1, subjectarea2, subjectarea3,
                subjectarea1_conf, subjectarea2_conf, subjectarea3_conf,
                compute_tags
            )
        )
        if len(buffer) > 5:
            print('flush buffer')
            df_res = df_res.union(spark.createDataFrame(buffer, schema = df_res.schema))
            buffer = []
    
    df_res = df_res.unionByName(spark.createDataFrame(buffer, schema = df_res.schema), allowMissingColumns=True)
    df = df.join(df_res, on=id_col, how="left")
    for gen_col in df.columns:
        if gen_col.startswith("Generated"):
            df = df.withColumn(gen_col[len("Generated"):], F.coalesce(F.col(gen_col), F.col(gen_col[len("Generated"):])))
            df = df.drop(gen_col)
    # df.display()
    return df

def join_pre_computed_youtube(df_cleaned, df_transformed, id_col, check_col, generate_cols=['tags', 'abstract']):
    df_transformed = df_transformed.select(
        id_col,
        *select_abstract_columns() if 'abstract' in generate_cols else [],
        *select_tag_columns_youtube() if 'tags' in generate_cols else [],
        F.when(F.col("Transformed" + check_col).isNotNull(), F.lit(True)).otherwise(F.lit(False)).alias("is_old"),
        # "recompute_tags",
        # "recompute_abstract",
    )
    df = df_cleaned.join(df_transformed, id_col, "left")
    df = df.withColumn("is_old", F.coalesce(F.col("is_old"), F.lit(False)))
    # df = df.withColumn("recompute_tags", F.coalesce(F.col("recompute_tags"), F.lit(True)))
    # df = df.withColumn("recompute_abstract", F.coalesce(F.col("recompute_abstract"), F.lit(True)))

    for transf_col in df.columns:
        if transf_col.startswith("Transformed"):
            df = df.withColumn(transf_col[len("Transformed"):], F.coalesce(F.col(transf_col), F.lit(None)))
            df = df.drop(transf_col)
    return df

# COMMAND ----------


