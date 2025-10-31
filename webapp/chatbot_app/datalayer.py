import uuid

import chainlit as cl
from chainlit import data as cl_data
from chainlit.element import Element, ElementDict
from chainlit.types import Feedback, ThreadDict, PageInfo, PaginatedResponse
from literalai.step import StepDict
from literalai.helper import utc_now

import asyncio

from sqlalchemy import (
    create_engine,
    text,
    MetaData,
    select,
    Table,
    Column,
    Integer,
    String,
    insert,
    update,
)
import os


def get_04_ai_sql_engine():
    catalog = os.environ.get("CATALOG", None)
    schema = os.environ.get("SCHEMA", "04_ai")

    sql_alchemy_conninfo = f"databricks://token:{os.environ['DATABRICKS_TOKEN']}@{os.environ['DATABRICKS_SERVER_HOSTNAME']}?http_path={os.environ['DATABRICKS_HTTP_PATH']}&catalog={catalog}&schema={schema}"

    engine = create_engine(
        url=sql_alchemy_conninfo
    )
    return engine


metadata_obj = MetaData()

threads_table = Table(
    "chainlit_threads",
    metadata_obj,
    Column("id", String(30), primary_key=True),
    Column("createdAt", String(30)),
    Column("name", String),
    Column("userId", String),
    Column("userIdentifier", String(30)),
    Column("tags", String),
)

steps_table = Table(
    "chainlit_step",
    metadata_obj,
    Column("name", String(30)),
    Column("type", String(30)),
    Column("id", String(30), primary_key=True),
    Column("threadId", String(30)),
    Column("parentId", String(30)),
    Column("input", String),
    Column("output", String),
    Column("createdAt", String(30)),
)



user = {}

class CustomDataLayer(cl_data.BaseDataLayer):

    engine = get_04_ai_sql_engine()

    async def get_user(self, identifier: str):
        user[identifier] = cl.PersistedUser(id=identifier, createdAt=utc_now(), identifier=identifier)
        return user[identifier]

    async def upsert_feedback(self, feedback: Feedback):
        def f(feedback):
            with self.engine.connect() as con:
                con.execute(text("create table if not exists chainlit_feedback (id STRING, forId STRING, value INT, threadId STRING, comment STRING);"))
                if not feedback.id:
                    feedback.id = str(uuid.uuid4())
                    result = con.execute(text(f"insert into chainlit_feedback (id, forId, value, threadId, comment) values (\'{feedback.id}\', \'{feedback.forId}\', {feedback.value}, \'{feedback.threadId}\', \'{feedback.comment}\')"))
                else:
                    con.execute(text(f"update chainlit_feedback set value = {feedback.value}, comment = \'{feedback.comment}\' where id = \'{feedback.id}\'"))
                con.commit()
            return feedback
        
        feedback = await asyncio.to_thread(f, feedback=feedback)
        return feedback.id

    @cl_data.queue_until_user_message()
    async def create_step(self, step_dict):
        def f(step_dict):
            if not 'input' in step_dict:
                step_dict['input'] = ''
            if not 'output' in step_dict:
                step_dict['output'] = ''
            with self.engine.connect() as con:
                result = con.execute(
                    text(f"insert into chainlit_step (name, type, id, threadId, parentId, input, output, createdAt) values (:name, :type, :id, :threadId, :parentId, :input, :output, :createdAt)"),
                    dict(name=step_dict['name'], type=step_dict['type'], id=step_dict['id'], threadId=step_dict['threadId'], parentId=step_dict['parentId'], input=step_dict['input'], output=step_dict['output'], createdAt=utc_now())
                )
                con.commit()

        await asyncio.to_thread(f, step_dict=step_dict)

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        def f(thread_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            with self.engine.connect() as conn:
                thread = conn.execute(stmt).first()
                if thread is None:
                    return None
                stmt = select(steps_table).where(steps_table.c.threadId == thread.id).order_by(steps_table.c.createdAt.asc())
                l_steps = []
                for row_steps in conn.execute(stmt):
                    sd = StepDict(name=row_steps.name, type=row_steps.type, id=row_steps.id, output=row_steps.output)
                    l_steps.append(sd)
                return ThreadDict(id=thread.id, createdAt=thread.createdAt, name=thread.name, userId=thread.userId, steps=l_steps)
            
        return await asyncio.to_thread(f, thread_id=thread_id)

    async def update_thread(self, thread_id: str, name: str | None = None, user_id: str | None = None, metadata: cl.Dict | None = None, tags = None):
        def f(thread_id, name, user_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            new = True
            with self.engine.connect() as conn:
                result = conn.execute(stmt).first()
                if result is not None:
                    new = False

            if new:
                stmt = insert(threads_table).values(id=thread_id, createdAt=utc_now(), name=name, userId=user_id)
            
            with self.engine.connect() as conn:
                conn.execute(stmt)

        await asyncio.to_thread(f, thread_id=thread_id, name=name, user_id=user_id)

    async def list_threads(self, pagination, filters):
        return PaginatedResponse(data=[], pageInfo=PageInfo(hasNextPage=False, startCursor=None, endCursor=None))
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
    
    async def get_thread_author(self, thread_id: str) -> str:
        def f(thread_id):
            stmt = select(threads_table).where(threads_table.c.id == thread_id)
            with self.engine.connect() as conn:
                result = conn.execute(stmt).first()
                if result is None:
                    return None
                return result.userId
            
        return await asyncio.to_thread(f, thread_id=thread_id)
    
    async def update_step(self, step_dict: StepDict):
        return await super().update_step(step_dict)
