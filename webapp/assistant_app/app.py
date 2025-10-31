from dotenv import load_dotenv
load_dotenv('.env')
import base64
import os
import json
import plotly
from openai import AsyncAssistantEventHandler, AzureOpenAI, AsyncAzureOpenAI
from literalai.helper import utc_now
import chainlit as cl
from chainlit.config import config
from chainlit.context import local_steps
from openai.types.beta.threads.runs import RunStep
import semantic_search
from chainlit.types import ThreadDict



from datalayer import CustomDataLayer
import chainlit.data as cl_data

async_openai_client = AsyncAzureOpenAI(api_key=os.environ.get("OPENAI_ASSISTANT_KEY"),
                                       azure_endpoint=os.environ.get("OPENAI_ASSISTANT_ENDPOINT"),
                                       api_version="2024-05-01-preview"
                                       )

sync_openai_client = AzureOpenAI(api_key=os.environ.get("OPENAI_ASSISTANT_KEY"),
                                       azure_endpoint=os.environ.get("OPENAI_ASSISTANT_ENDPOINT"),
                                       api_version="2024-05-01-preview"
                                       )

assistant = sync_openai_client.beta.assistants.retrieve(
    os.environ.get("OPENAI_ASSISTANT_ID")
)


@cl.data_layer
def get_data_layer():
    return CustomDataLayer()

#UI Konfiguration
config.ui.name = "A WattsUp Chatbot"
config.ui.logo = {
    "light": "/logo_light.svg",
    "dark": "/logo_dark.svg"
}
config.ui.favicon = "public/favicon_1.png"

class EventHandler(AsyncAssistantEventHandler):
    def __init__(self, assistant_name: str) -> None:
        super().__init__()
        #self.last_user_message = cl.user_session.get("last_user_query", "")
        self.current_message: cl.Message = None
        self.current_step: cl.Step = None
        self.current_tool_call = None
        self.assistant_name = assistant_name
        previous_steps = local_steps.get() or []
        parent_step = previous_steps[-1] if previous_steps else None
        if parent_step:
            self.parent_id = parent_step.id

    async def on_run_step_created(self, run_step: RunStep) -> None:
        cl.user_session.set("run_step", run_step)

    async def on_text_created(self, text) -> None:
        actions = [
            cl.Action(
                name="open_powerbi",
                label="Zum Power BI Dashboard",
                type="url",
                payload={"url": "https://app.powerbi.com/groups/593e8ebf-3466-4c11-8125-171f49718f40/reports/16e33f3a-cf8c-404a-8e24-61343876b74e/59cee26fa5bd8a1e10aa?experience=power-bi"}
            ),
            cl.Action(
                name="show_info",
                label="Datengrundlage & Metriken",
                type="run",
                payload={"value": "info"}
            ),]
        self.current_message = await cl.Message(
            author=self.assistant_name, content="", actions=actions
        ).send()
        self.parent_id = self.current_message.id

        # Erzeuge neuen Assistant-Step
        self.current_step = cl.Step(
            name=self.assistant_name,
            type="assistant_message",
            parent_id=self.parent_id,
        )
        await self.current_step.send()

        # Aktualisiere Parent für Folge-Schritte
        self.parent_id = self.current_step.id

    async def on_text_delta(self, delta, snapshot):
        if delta.value:
            await self.current_message.stream_token(delta.value)
    
    # Behandelt Annotations wie Grafiken, File_Paths und Downloadlinks für Grafiken.
    async def on_text_done(self, text):
        await self.current_message.update()
        #print(text)
        #print(self.current_step)
        #print(self.current_step.type)
        #if self.current_step:
            #self.current_step.output = text.value
            #self.current_step.language = "markdown"
            #await self.current_step.update()
        if text.annotations:
            for annotation in text.annotations:
                if annotation.type == "file_path":
                    response = (
                        await async_openai_client.files.with_raw_response.content(
                            annotation.file_path.file_id
                        )
                    )
                    file_name = annotation.text.split("/")[-1]
                    try:
                        fig = plotly.io.from_json(response.content)
                        element = cl.Plotly(name=file_name, figure=fig)
                        await cl.Message(content="", elements=[element]).send()
                    except Exception as e:
                        element = cl.File(content=response.content, name=file_name)
                        await cl.Message(content="", elements=[element]).send()
                    # Hack to fix links
                    if (
                        annotation.text in self.current_message.content
                        and element.chainlit_key
                    ):
                        self.current_message.content = self.current_message.content.replace(
                            annotation.text,
                            f"/project/file/{element.chainlit_key}?session_id={cl.context.session.id}",
                        )
                        await self.current_message.update()
    
    # Wird aufgerufen, wenn ein Tool gestartet wird und startet einen neuen Tool Step.
    async def on_tool_call_created(self, tool_call):
        self.current_tool_call = tool_call.id
        self.current_step = cl.Step(
            name=tool_call.type, type="tool", parent_id=self.parent_id
        )
        self.current_step.show_input = "python"
        self.current_step.start = utc_now()
        await self.current_step.send()
    
    # Wird aufgerufen, wenn sich der Status eines laufenden Tools verändert. Gibt die einzelnen Schritt des Code Interpreters aus.
    async def on_tool_call_delta(self, delta, snapshot):
        if snapshot.id != self.current_tool_call:
            self.current_tool_call = snapshot.id
            self.current_step = cl.Step(
                name=delta.type, type="tool", parent_id=self.parent_id
            )
            self.current_step.start = utc_now()
            if snapshot.type == "code_interpreter":
                self.current_step.show_input = "python"
            if snapshot.type == "function":
                self.current_step.name = snapshot.function.name
                self.current_step.language = "json"
            await self.current_step.send()

        if delta.type == "function":
            pass

        if delta.type == "code_interpreter":
            if delta.code_interpreter.outputs:
                for output in delta.code_interpreter.outputs:
                    if output.type == "logs":
                        self.current_step.output += output.logs
                        self.current_step.language = "markdown"
                        self.current_step.end = utc_now()
                        await self.current_step.update()
                    elif output.type == "image":
                        self.current_step.language = "json"
                        self.current_step.output = output.image.model_dump_json()
            else:
                if delta.code_interpreter.input:
                    await self.current_step.stream_token(
                        delta.code_interpreter.input, is_input=True
                    )

    # Behandlung von Events.
    async def on_event(self, event) -> None:
        if event.event == "error":
            return await cl.ErrorMessage(content=str(event.data.message)).send()
        # Erkennen, ob ein Tool-Aufruf nötig ist
        if event.event == "thread.run.requires_action":
            thread_id = event.data.thread_id
            run_id = event.data.id
            await self.handle_requires_action(event.data, thread_id, run_id)

    # Ausführen von Custom Tool Aufrufen des Assistant.
    async def handle_requires_action(self, data, thread_id, run_id):
        tool_outputs = []
        for tool in data.required_action.submit_tool_outputs.tool_calls:
            if tool.function.name == "semantic_search":
                arguments = json.loads(tool.function.arguments) 
                user_query = arguments.get("user_query", "")
                search_results = await semantic_search.semantic_search(user_query)
                tool_outputs.append({
                    "tool_call_id": tool.id,
                    "output": search_results
                })

        await self.submit_tool_outputs(thread_id, run_id, tool_outputs)

    # Rückgabe der Custom Tool Outputs an Assistant.
    async def submit_tool_outputs(self, thread_id, run_id, tool_outputs):
        new_handler = EventHandler(assistant_name=assistant.name)
        async with async_openai_client.beta.threads.runs.submit_tool_outputs_stream(
            thread_id=thread_id,
            run_id=run_id,
            tool_outputs=tool_outputs,
            event_handler=new_handler
        ) as stream:
            await stream.until_done()
        
    # Behandlung von Python Exceptions.
    async def on_exception(self, exception: Exception) -> None:
        return cl.ErrorMessage(content=str(exception)).send()
    
    # Aktualisiert den Status eines Tools, wenn es fertig ist.
    async def on_tool_call_done(self, tool_call):
        self.current_step.end = utc_now()
        await self.current_step.update()

# Läuft, wenn ein neuer Chat gestartet wird.
@cl.on_chat_start
async def start_chat():
    # Create a Thread
    thread = await async_openai_client.beta.threads.create()
    # Store thread ID in user session for later use
    cl.user_session.set("thread_id", thread.id)

    await cl.Message(
        content="👋 Willkommen beim A WattsUp Chatbot!",
        actions=[
            cl.Action(
                name ="open_powerbi",
                label = "Zum Power BI Dashboard",
                type = "url",
                payload= {"url": "https://app.powerbi.com/groups/593e8ebf-3466-4c11-8125-171f49718f40/reports/16e33f3a-cf8c-404a-8e24-61343876b74e/59cee26fa5bd8a1e10aa?experience=power-bi"}
            ),
            cl.Action(
                name="show_info",  # Muss mit dem Callback-Namen übereinstimmen
                label="Datengrundlage & Metriken",
                type="run",
                payload={"value": "info"}
            )
        ]
    ).send()

@cl.action_callback("open_powerbi")
async def on_open_powerbi(action):
    await cl.Message(
        content="[🔗 Hier geht’s zum Power BI Dashboard](https://app.powerbi.com/groups/593e8ebf-3466-4c11-8125-171f49718f40/reports/16e33f3a-cf8c-404a-8e24-61343876b74e/59cee26fa5bd8a1e10aa?experience=power-bi)"
    ).send()

@cl.action_callback("show_info")
async def on_show_info(action):
    value = action.payload.get("value")
    if value == "info":
        await cl.Message(
            content=(
                "ℹ️ **Info zur Datengrundlage:**\n\n"
                "Die zugrunde liegenden Daten bestehen aus täglich aktualisierten Social-Media- und Eco-Journal-Daten ab dem 01.01.2025. Sie liegen als vollständig analysierbare CSV-Dateien vor.\n\n"
                "Die **EngagementRate** wird nach einer festen, kanalübergreifenden Formel berechnet.\n"
                "🔎 **Details zur Datenstruktur und Formel** findest du in der Readme."
            )
        ).send()

# Läuft, wenn der User den Stopp Button betätigt.
@cl.on_stop
async def stop_chat():
    current_run_step: RunStep = cl.user_session.get("run_step")
    if current_run_step:
        await async_openai_client.beta.threads.runs.cancel(
            thread_id=current_run_step.thread_id, run_id=current_run_step.run_id
        )

#Läuft, wenn eine Nachricht vom User gesendet wird.
@cl.on_message
async def main(message: cl.Message):
    thread_id = cl.user_session.get("thread_id")

    # Speichern der aktuellen Frage
    cl.user_session.set("last_user_query", message.content)

    # Add a Message to the Thread
    oai_message = await async_openai_client.beta.threads.messages.create(
        thread_id=thread_id,
        role="user",
        content=message.content,
    )

    # Create and Stream a Run
    async with async_openai_client.beta.threads.runs.stream(
        thread_id=thread_id,
        assistant_id=assistant.id,
        event_handler=EventHandler(assistant_name=assistant.name),
    ) as stream:
        await stream.until_done()

@cl.on_chat_resume
async def on_chat_resume(thread: ThreadDict):
    print("[on_chat_resume] received thread:", thread)

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