# 💬 Willkommen beim A KI-Chatbot

Dieser Chatbot wurde speziell zur Analyse und Exploration von **Kommunikationsdaten** entwickelt. Er hilft dir, Inhalte, Trends, KPIs und thematische Zuordnungen effizient zu untersuchen – ganz ohne Programmierkenntnisse.

---

## 📊 Datengrundlage

Der Chatbot greift auf **zwei täglich aktualisierte CSV-Dateien** zu:

### 1. **Social-Media-Daten**

- Enthält Beiträge von Facebook, Instagram, LinkedIn, X (ehem. Twitter) und YouTube.
- Enthaltene Kennzahlen: Impressions und EngagementRateInPercent.
- Thematische Einordnung über die Felder `StrategischesThema` und `Themenbereich`.

### 2. **Eco-Journal-Daten**

- Enthält Nutzungsdaten von redaktionellen Artikeln der Eco-Journal-Plattform.
- Beinhaltet Kennzahlen wie durchschnittliche Sitzungsdauer, aktive Nutzer, Seitenaufrufe und thematische Einordnungen.

---

## 📈 EngagementRate je Kanal

Die EngagementRate ist ein zusammengesetzter Interaktionswert – individuell pro Kanal berechnet:

| Kanal         | Formel                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------ |
| **Facebook**  | `((Shares × 0.1 + Comments × 0.4 + LinkClicks × 0.3 + Reactions × 0.2) / Impressions) × 100`           |
| **Instagram** | `((Likes × 0.2 + Comments × 0.4 + Shares × 0.1 + Saves × 0.3) / Impressions) × 100`                    |
| **LinkedIn**  | `((Likes × 0.1 + Shares × 0.3 + Comments × 0.4 + Clicks × 0.2) / Impressions) × 100`                   |
| **X**         | `((Interactions × 0.35 + Replies × 0.45 + Reposts × 0.2) / Impressions) × 100`                         |
| **YouTube**   | `((Likes × 0.2 + Dislikes × 0.05 + Comments × 0.25 + Shares × 0.15 + Watchtime × 0.35) / Views) × 100` |

---

## 🛠️ Verfügbare Analyse-Tools

Der Chatbot nutzt zwei spezialisierte Analysewerkzeuge:

### 1. **Code Interpreter (🧼)**

Ideal für:

- Kennzahlen, Rankings und Zeitreihen
- Beispiel:
  _„Welche Plattform hatte im Mai die höchste durchschnittliche EngagementRate?“_

### 2. **Semantic Search (🔍)**

Ideal für:

- Inhaltliche Fragen und Themenexploration
- Beispiel:
  _„Gab es Posts über Wärmepumpen oder Photovoltaik?“_

---

## 💡 Beispiel-Prompts

Hier sind einige nützliche Prompts zum Start:

- **„Zeige mir die Top 5 Posts nach EngagementRate im letzten Monat.“**
- **„Welche strategischen Themen wurden im Februar häufig kommuniziert?“**
- **„Gab es LinkedIn-Posts mit hoher Reichweite zum Thema Elektromobilität?“**
- **„Wie viele Artikel im Eco-Journal hatten mehr als 1.000 aktive Nutzer?“**
- **„Welche Kanäle haben im Durchschnitt die beste EngagementRate?“**

---

## 📁 Zugriff & Aktualität

Die Daten werden **täglich aktualisiert** und sind vollständig analysierbar (keine Excel-Dateien). Alle IDs und Zugriffsparameter werden automatisch durch das System gesetzt – du brauchst dich um nichts zu kümmern.

---

Wir wünschen dir viel Erfolg bei der Analyse!
