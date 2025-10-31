import os
import uuid
import asyncio
from sqlalchemy import create_engine, MetaData, Table, Column, String, select, text, insert
import chainlit as cl
from chainlit import data as cl_data
from chainlit.types import Feedback
from chainlit.element import Element, ElementDict
from chainlit.types import ThreadDict, PageInfo, PaginatedResponse
from literalai.observability.step import StepDict
from literalai.helper import utc_now

def get_databricks_engine():
    catalog = os.environ.get("CATALOG")
    schema = os.environ.get("SCHEMA", "04_to_ai_assistant")
    conn = f"databricks://token:{os.environ['DATABRICKS_TOKEN']}@{os.environ['DATABRICKS_SERVER_HOSTNAME']}?http_path={os.environ['DATABRICKS_HTTP_PATH']}&catalog={catalog}&schema={schema}"
    return create_engine(url=conn)

# Define metadata and tables
metadata = MetaData()

threads_table = Table(
    "chainlit_threads", metadata,
    Column("id", String(50), primary_key=True),
    Column("createdAt", String(30)),
    Column("name", String(100)),
    Column("userId", String(50)),
    Column("userIdentifier", String(100)),
    Column("tags", String(200)),
)

steps_table = Table(
    "chainlit_steps", metadata,
    Column("id", String(50), primary_key=True),
    Column("threadId", String(50)),
    Column("parentId", String(50)),
    Column("name", String(100)),
    Column("type", String(50)),
    Column("input", String),
    Column("output", String),
    Column("createdAt", String(30)),
)

class CustomDataLayer(cl_data.BaseDataLayer):
    """
    Custom DataLayer for Chainlit storing threads, steps and feedback in Databricks via SQL.
    """
    def __init__(self):
        super().__init__()
        self.engine = get_databricks_engine()
        # Ensure tables exist
        with self.engine.connect() as conn:
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS chainlit_threads (
                    id STRING, createdAt STRING, name STRING,
                    userId STRING, userIdentifier STRING, tags STRING
                )
                """))
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS chainlit_steps (
                    id STRING, threadId STRING, parentId STRING,
                    name STRING, type STRING, input STRING,
                    output STRING, createdAt STRING
                )
                """))
            conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS chainlit_feedback (
                    id STRING, forId STRING, value INT,
                    threadId STRING, comment STRING
                )
                """))
            conn.commit()

    async def get_user(self, identifier: str):
        return cl.PersistedUser(id=identifier, createdAt=utc_now(), identifier=identifier)

    async def update_thread(self, thread_id: str, name: str | None = None, user_id: str | None = None, metadata: cl.Dict | None = None, tags = None):
        def f(thread_id, name, user_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            new = True
            with self.engine.connect() as conn:
                result = conn.execute(stmt).first()
                if result is not None:
                    new = False

            if new:
                stmt = insert(threads_table).values(id=thread_id, createdAt=utc_now(), name=name, userId=user_id, userIdentifier=user_id)
            else:
                stmt = threads_table.update().where(threads_table.c.id == thread_id).values(name=name, userId=user_id)

            
            with self.engine.connect() as conn:
                conn.execute(stmt)

        await asyncio.to_thread(f, thread_id=thread_id, name=name, user_id=user_id)

    @cl_data.queue_until_user_message()
    async def create_step(self, step_dict):
        def f(step_dict):
            if not 'input' in step_dict:
                step_dict['input'] = ''
            if not 'output' in step_dict:
                step_dict['output'] = ''
            with self.engine.connect() as con:
                result = con.execute(
                    text(f"insert into chainlit_steps (name, type, id, threadId, parentId, input, output, createdAt) values (:name, :type, :id, :threadId, :parentId, :input, :output, :createdAt)"),
                    dict(name=step_dict['name'], type=step_dict['type'], id=step_dict['id'], threadId=step_dict['threadId'], parentId=step_dict['parentId'], input=step_dict['input'], output=step_dict['output'], createdAt=utc_now())
                )
                con.commit()

        await asyncio.to_thread(f, step_dict=step_dict)

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        print(f"[CustomDataLayer] get_thread called with thread_id={thread_id}")
        def f(thread_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            with self.engine.connect() as conn:
                thread = conn.execute(stmt).first()
                if thread is None:
                    return None
                stmt = select(steps_table).where(steps_table.c.threadId == thread.id).order_by(steps_table.c.createdAt.desc())
                l_steps = []
                for row_steps in conn.execute(stmt):
                    sd = StepDict(name=row_steps.name, type=row_steps.type, id=row_steps.id, output=row_steps.output)
                    l_steps.append(sd)
                return ThreadDict(id=thread.id, createdAt=thread.createdAt, name=thread.name, userId=thread.userId, steps=l_steps)
        result = await asyncio.to_thread(f, thread_id=thread_id)
        print(f"[CustomDataLayer] get_thread returning {result}")
        return result

    async def upsert_feedback(self, feedback: Feedback) -> str:
        def db_op():
            fid = feedback.id or str(uuid.uuid4())
            if not feedback.id:
                ins = text(
                    "INSERT INTO chainlit_feedback (id, forId, value, threadId, comment)"
                    " VALUES (:id, :forId, :value, :threadId, :comment)"
                )
                params = dict(
                    id=fid,
                    forId=feedback.forId,
                    value=feedback.value,
                    threadId=feedback.threadId,
                    comment=feedback.comment or ''
                )
                with self.engine.connect() as conn:
                    conn.execute(ins, params)
                    conn.commit()
            else:
                upd = text(
                    "UPDATE chainlit_feedback SET value=:value, comment=:comment WHERE id=:id"
                )
                with self.engine.connect() as conn:
                    conn.execute(upd, dict(id=fid, value=feedback.value, comment=feedback.comment))
                    conn.commit()
            return fid
        return await asyncio.to_thread(db_op) 
    
    async def get_thread_author(self, thread_id: str) -> str:
        def f(thread_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            with self.engine.connect() as conn:
                result = conn.execute(stmt).first()
                if result is None:
                    return None
                return result.userId
            
        return await asyncio.to_thread(f, thread_id=thread_id)
            
    async def list_threads(self, pagination, filters):
        def f():
            l = []
            stmt = select(threads_table).where(threads_table.c.userId == filters.userId).order_by(threads_table.c.createdAt.desc())
            with self.engine.connect() as conn:
                for row in conn.execute(stmt):
                    l.append(ThreadDict(id=row.id, createdAt=row.createdAt, name=row.name, userId=row.userId))

            return PaginatedResponse(
                data=l,
                pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None),
            )
            
        return await asyncio.to_thread(f)

    async def build_debug_url(self) -> str:
        return await super().build_debug_url()
    
    async def create_element(self, element: Element):
        return await super().create_element(element)
    
    async def create_user(self, user: cl.User) -> cl.PersistedUser | None:
        return await super().create_user(user)
    
    async def delete_element(self, element_id: str, thread_id: str | None = None):
        return await super().delete_element(element_id, thread_id)
    
    async def delete_feedback(self, feedback_id: str) -> bool:
        return await super().delete_feedback(feedback_id)
    
    async def delete_step(self, step_id: str):
        return await super().delete_step(step_id)
    
    async def delete_thread(self, thread_id: str):
        return await super().delete_thread(thread_id)
    
    async def get_element(self, thread_id: str, element_id: str) -> ElementDict | None:
        return await super().get_element(thread_id, element_id)
    
    async def update_step(self, step_dict: StepDict):
        return await super().update_step(step_dict)