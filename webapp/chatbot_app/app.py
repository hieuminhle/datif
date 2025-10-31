from dotenv import load_dotenv

load_dotenv('env')

import json
import sys
import os
import hashlib
import asyncio
import aiohttp
import logging
import base64
from typing import Union

import chainlit as cl
from chainlit.input_widget import Slider
from chainlit import data as cl_data

from fastapi import Request, Response

from sqlalchemy import (
    MetaData,
    select,
    Table,
    Column,
    Integer,
    String,
    Boolean,
)

from datalayer import CustomDataLayer, get_04_ai_sql_engine

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

cl_data._data_layer = CustomDataLayer()

metadata_obj = MetaData()

prepared_questions_table = Table(
    "prepared_questions",
    metadata_obj,
    Column("ID", String(30), primary_key=True),
    Column("ShortName", String(30)),
    Column("Question", String),
    Column("Answer", String),
    Column("Icon", String(30)),
    Column("Order", Integer),
    Column("StarterButton", Boolean),
)

async def query_prepared_questions():
    """
    This will be executed during startup to list the Starters Buttons and during chatstart again,
    because then we can cache the result in the user session.
    """
    def f():
        stmt = select(prepared_questions_table).order_by(prepared_questions_table.c.Order.asc())#.where(prepared_questions_table.c.ID == id)
        question_lookup = {}
        id_lookup = {}
        engine = get_04_ai_sql_engine()

        with engine.connect() as conn:
            for row in conn.execute(stmt):
                question_lookup[row.Question] = row
                id_lookup[row.ID] = row.Question

        return question_lookup, id_lookup

    return await asyncio.to_thread(f)


def get_actions_themenbereiche():
    id = cl.user_session.get('prepared_questions_id')
    return [
        cl.Action(name="themenbereich_selection_button", label="Elektromobilität", value=f"{id}EMobility"),
        cl.Action(name="themenbereich_selection_button", label="Erneuerbare Energien", value=f"{id}ErneuerbareEnergien"),
        cl.Action(name="themenbereich_selection_button", label="Finanzen / M&A", value=f"{id}FinanzenMA"),
        cl.Action(name="themenbereich_selection_button", label="Innovation, Forschung, Entwicklung", value=f"{id}InnoForschEntw"),
        cl.Action(name="themenbereich_selection_button", label="Kernenergie", value=f"{id}Kernenergie"),
        cl.Action(name="themenbereich_selection_button", label="Konventionelle Energie", value=f"{id}KonventionelleEnergie"),
        cl.Action(name="themenbereich_selection_button", label="Nachhaltigkeit / Umweltschutz", value=f"{id}NachhaltigkeitUmweltschutz"),
        cl.Action(name="themenbereich_selection_button", label="Netze", value=f"{id}Netze"),
        cl.Action(name="themenbereich_selection_button", label="Personal", value=f"{id}Personal"),
        cl.Action(name="themenbereich_selection_button", label="Vertrieb", value=f"{id}Vertrieb"),
    ]

def get_actions_strategische_themen():
    id = cl.user_session.get('prepared_questions_id')
    return [
        cl.Action(name="strategisches_thema_selection_button", label="Strategie 2030", value=f"{id}StrategicStrategie2030"),
        cl.Action(name="strategisches_thema_selection_button", label="Finanzierung Energiewende", value=f"{id}StrategicFinanzierungEnergiewende"),
        cl.Action(name="strategisches_thema_selection_button", label="E-Mobilität", value=f"{id}StrategicEMobilitaet"),
        cl.Action(name="strategisches_thema_selection_button", label="Performancekultur", value=f"{id}StrategicPerformancekultur"),
        cl.Action(name="strategisches_thema_selection_button", label="Vernetze Energiewelt", value=f"{id}StrategicVernetzeEnergiewelt"),
        cl.Action(name="strategisches_thema_selection_button", label="Transformation Gasnetze/Wasserstoff", value=f"{id}StrategicTransformationGasnetzeWasserstoff"),
        cl.Action(name="strategisches_thema_selection_button", label="Erneuerbare Energien", value=f"{id}StrategicErneuerbareEnergien"),
        cl.Action(name="strategisches_thema_selection_button", label="Disponible Erzeugung", value=f"{id}StrategicDisponibleErzeugung"),
        cl.Action(name="strategisches_thema_selection_button", label="Intelligente Stromnetze", value=f"{id}StrategicIntelligenteStromnetze"),
        cl.Action(name="strategisches_thema_selection_button", label="A als ArbeitgeberIn", value=f"{id}StrategicAAlsArbeitgeberIn"),
        cl.Action(name="strategisches_thema_selection_button", label="Nachhaltigkeit/CSR/ESG", value=f"{id}StrategicNachhaltigkeitCSRESG"),
        cl.Action(name="strategisches_thema_selection_button", label="Marke A", value=f"{id}StrategicMarkeA"),
    ]


@cl.header_auth_callback
async def header_auth_callback(headers):
    """
    Handle authentication using request headers from the Azure App Service identity provider.

    If the environment variable 'ENVIRONMENT' is set to 'dev', the authentication is skipped and
    a Testuser is logged in.
    """
    if os.environ.get("ENVIRONMENT", None) == 'dev':
        return cl.User(identifier="Tester", metadata={'provider': 'header'})

    user_name = headers.get("x-ms-client-principal-name", None)
    client_principal = headers.get("x-ms-client-principal", None)
    group_id = os.environ.get("GROUP_ID", None)

    # print('principal', client_principal)
    # print('token:', token)

    decoded_principal = base64.b64decode(
        client_principal.encode('ascii')
    ).decode('ascii')
    
    # print('decoded:', decoded_principal)
    
    json_principal = json.loads(decoded_principal)

    # print('json', json_principal)


    for claim in json_principal['claims']:
        if claim['typ'] == "groups" and claim['val'] == group_id:
            return cl.User(identifier=user_name, metadata={'provider': 'header'})
    else:
        return None


@cl.on_logout
def on_logout(request: Request, response: Response):
    """
    Handle user logout by deleting the AppService Authentication Cookie.
    """
    response.delete_cookie("AppServiceAuthSession")


@cl.on_chat_start
async def on_chat_start():
    """
    At the start of a new session:
        - The chat history is set to an empty list.
        - Settings are configured.
        - Intro message is send to the user (not part of the chat history).
    """
    cl.user_session.set("chat_history", [])

    settings = await cl.ChatSettings(
        [
            Slider(
                id='history_len',
                label='Lenght of the chat history',
                initial=5,
                min=0,
                max=20,
                step=1,
            )
        ]
    ).send()

    cl.user_session.set("prepared_questions", await query_prepared_questions())

    await setup_agent(settings)


@cl.on_chat_resume
async def on_chat_resume(thread):
    print('thread:', thread)


@cl.on_settings_update
async def setup_agent(settings):
    """
    Settings listener.
    """
    cl.user_session.set("history_len", settings['history_len'])


@cl.step(type="promptflow", name="the Database and OpenAI to generate an answer")
async def promptflow(req_body):
    """
    Send request to the promptlow endpoint.
    Necessary environment variables
            - MODEL_ENDPOINT
            - DATABRICKS_TOKEN
    """
    url = os.environ['MODEL_ENDPOINT']
    api_key = os.environ['DATABRICKS_TOKEN']
    if not api_key:
        raise Exception("A key should be provided to invoke the endpoint")

    headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
    
    try:

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=req_body, headers=headers) as resp:
                result = await resp.text()

        answer = json.loads(result)['content']
    except Exception as e:
        answer = str(e)
        print('res ', result)
        print("Exception: ", e)
        
    return answer


@cl.action_callback("themenbereich_selection_button")
async def on_action(action):
    prepared_questions, id_lookup = cl.user_session.get('prepared_questions')
    answer = prepared_questions[id_lookup[action.value]].Answer
    await cl.Message(content=answer).send()
    chat_history = cl.user_session.get('chat_history')
    chat_history.append({
        'role': 'user',
        'content': id_lookup[action.value]
    })
    chat_history.append({
        'role': 'assistant',
        'content': answer
    })
    await cl.Message(content="Wähle einen Bereich:", actions=get_actions_themenbereiche()).send()


@cl.action_callback("strategisches_thema_selection_button")
async def on_action(action):
    prepared_questions, id_lookup = cl.user_session.get('prepared_questions')
    answer = prepared_questions[id_lookup[action.value]].Answer
    await cl.Message(content=answer).send()
    chat_history = cl.user_session.get('chat_history')
    chat_history.append({
        'role': 'user',
        'content': id_lookup[action.value]
    })
    chat_history.append({
        'role': 'assistant',
        'content': answer
    })
    await cl.Message(content="Wähle einen Bereich:", actions=get_actions_strategische_themen()).send()


@cl.on_message
async def main(message: cl.Message):
    """
    On each message:
        - configure request data (with history limited to the 'history_len' setting)
        - update chat history
        - send promptflow answer to user
    """
    await cl.sleep(2)
    
    prepared_questions, id_lookup = cl.user_session.get('prepared_questions')

    chat_profile = cl.user_session.get("chat_profile")

    is_prepared = message.content in prepared_questions
    if is_prepared:
        cl.user_session.set("prepared_questions_id", prepared_questions[message.content].ID)
    else:
        cl.user_session.set("prepared_questions_id", None)

    if chat_profile == 'Themenbereiche' and is_prepared:
        actions = get_actions_themenbereiche()
        await cl.Message(content="Wähle einen Themenbereich:", actions=actions).send()
    elif chat_profile == 'Strategische Themen' and is_prepared:
        actions = get_actions_strategische_themen()
        await cl.Message(content="Wähle einen Themenbereich:", actions=actions).send()
    else:
        chat_history = cl.user_session.get('chat_history')
        chat_history.append({
            'role': 'user',
            'content': message.content
        })
        if is_prepared:
            answer = prepared_questions[message.content].Answer
        else:
            body = str.encode(json.dumps({ "messages": chat_history }))
            answer = await promptflow(body)

        answer_msg = cl.Message(
            content=answer
        )

        id = await answer_msg.send()

        chat_history.append({
            'role': 'assistant',
            'content': answer
        })


@cl.set_chat_profiles
async def chat_profile():
    return [
        cl.ChatProfile(
            name="Allgemeiner Chat",
            markdown_description="Quick Buttons werden für die Allgemeine Fragen angepasst.",
            # icon="https://picsum.photos/250",
        ),
        cl.ChatProfile(
            name="Themenbereiche",
            markdown_description="Quick Buttons werden für die Themenbereiche angepasst.",
            # icon="https://picsum.photos/200",
        ),
        cl.ChatProfile(
            name="Strategische Themen",
            markdown_description="Quick Buttons werden für die Strategische Themen angepasst.",
            # icon="https://picsum.photos/250",
        ),
    ]


@cl.set_starters
async def set_starters():    
    prepared_questions, id_lookup = await query_prepared_questions()
    return [ 
        cl.Starter(
            prepared_questions[r].ShortName, prepared_questions[r].Question, prepared_questions[r].Icon
        )
        for r in prepared_questions if prepared_questions[r].StarterButton
    ]
