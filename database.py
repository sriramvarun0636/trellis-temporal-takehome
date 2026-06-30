import asyncpg
import os

DB_URL = os.getenv("DATABASE_URL", "postgresql://temporal:temporal@localhost:5432/temporal")

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(DB_URL)
        return self.pool

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def init_db(self):
        await self.connect()
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
        with open(schema_path, "r") as f:
            schema = f.read()
        async with self.pool.acquire() as conn:
            await conn.execute(schema)

db = Database()

async def get_db_pool():
    if not db.pool:
        await db.connect()
    return db.pool
