import asyncio
import random
import json
from typing import Dict, Any
from temporalio import activity
from database import get_db_pool

async def flaky_call() -> None:
    """Either raise an error or sleep long enough to trigger an activity timeout."""
    rand_num = random.random()
    if rand_num < 0.33:
        raise RuntimeError("Forced failure for testing")

    if rand_num < 0.67:
        await asyncio.sleep(300)  # Expect the activity layer to time out before this completes

# --- Core Business Logic Functions ---

async def order_received_func(order_id: str) -> Dict[str, Any]:
    await flaky_call()
    pool = await get_db_pool()
    order_data = {"order_id": order_id, "items": [{"sku": "ABC", "qty": 1}]}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO orders (id, state, address_json)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO NOTHING
            """,
            order_id,
            "received",
            json.dumps({"city": "New York"})
        )
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "ORDER_RECEIVED", json.dumps(order_data)
        )
    return order_data

async def order_validated_func(order: Dict[str, Any]) -> bool:
    await flaky_call()
    if not order.get("items"):
        raise ValueError("No items to validate")
        
    pool = await get_db_pool()
    order_id = order["order_id"]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "validated", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "ORDER_VALIDATED", json.dumps({})
        )
    return True

async def payment_charged_func(order: Dict[str, Any], payment_id: str, db) -> Dict[str, Any]:
    await flaky_call()
    order_id = order["order_id"]
    amount = sum(i.get("qty", 1) for i in order.get("items", []))
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO payments (payment_id, order_id, status, amount)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (payment_id) DO NOTHING
            """,
            payment_id, order_id, "charged", amount
        )
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "paid", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "PAYMENT_CHARGED", json.dumps({"payment_id": payment_id, "amount": amount})
        )
    return {"status": "charged", "amount": amount}

async def order_shipped_func(order: Dict[str, Any]) -> str:
    await flaky_call()
    pool = await get_db_pool()
    order_id = order["order_id"]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "shipped", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "ORDER_SHIPPED", json.dumps({})
        )
    return "Shipped"

async def package_prepared_func(order: Dict[str, Any]) -> str:
    await flaky_call()
    pool = await get_db_pool()
    order_id = order["order_id"]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "package_prepared", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "PACKAGE_PREPARED", json.dumps({})
        )
    return "Package ready"

async def carrier_dispatched_func(order: Dict[str, Any]) -> str:
    await flaky_call()
    pool = await get_db_pool()
    order_id = order["order_id"]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "carrier_dispatched", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "CARRIER_DISPATCHED", json.dumps({})
        )
    return "Dispatched"

# --- Temporal Activities ---

@activity.defn
async def order_received(order_id: str) -> Dict[str, Any]:
    return await order_received_func(order_id)

@activity.defn
async def order_validated(order: Dict[str, Any]) -> bool:
    return await order_validated_func(order)

@activity.defn
async def payment_charged(data: Dict[str, Any]) -> Dict[str, Any]:
    order = data["order"]
    payment_id = data["payment_id"]
    pool = await get_db_pool()
    return await payment_charged_func(order, payment_id, pool)

@activity.defn
async def order_shipped(order: Dict[str, Any]) -> str:
    return await order_shipped_func(order)

@activity.defn
async def package_prepared(order: Dict[str, Any]) -> str:
    return await package_prepared_func(order)

@activity.defn
async def carrier_dispatched(order: Dict[str, Any]) -> str:
    return await carrier_dispatched_func(order)

@activity.defn
async def update_address_activity(data: Dict[str, Any]) -> None:
    order_id = data["order_id"]
    new_address = data["address"]
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET address_json = $1 WHERE id = $2",
            json.dumps(new_address), order_id
        )
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "ADDRESS_UPDATED", json.dumps(new_address)
        )
        
@activity.defn
async def cancel_order_activity(order_id: str) -> None:
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET state = $1 WHERE id = $2", "cancelled", order_id)
        await conn.execute(
            "INSERT INTO events (order_id, type, payload_json) VALUES ($1, $2, $3)",
            order_id, "ORDER_CANCELLED", json.dumps({})
        )
