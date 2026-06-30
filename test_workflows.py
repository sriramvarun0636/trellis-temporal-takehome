import asyncio
import uuid
import time
import pytest
from temporalio.client import Client
from temporalio.worker import Worker
# We use the real local Temporal server (localhost:7233) started via docker-compose instead of start_local()

from workflows import OrderWorkflow, ShippingWorkflow
from activities import (
    order_received,
    order_validated,
    payment_charged,
    order_shipped,
    package_prepared,
    carrier_dispatched,
    update_address_activity,
    cancel_order_activity
)

# Requires the database to be running locally via docker-compose!
@pytest.mark.asyncio
async def test_order_workflow_success_path():
    start_time = time.time()
    
    # Initialize DB schema
    from database import db, Database
    test_db = Database()
    await test_db.init_db()

    client = await Client.connect("localhost:7233")
    
    # Start both workers in the test environment
    order_worker = Worker(
        client,
        task_queue="order-tq",
        workflows=[OrderWorkflow],
        activities=[
            order_received,
            order_validated,
            payment_charged,
            order_shipped,
            update_address_activity,
            cancel_order_activity
        ]
    )
    
    shipping_worker = Worker(
        client,
        task_queue="shipping-tq",
        workflows=[ShippingWorkflow],
        activities=[
            package_prepared,
            carrier_dispatched
        ]
    )
    
    order_worker_task = asyncio.create_task(order_worker.run())
    shipping_worker_task = asyncio.create_task(shipping_worker.run())
    
    try:
        order_id = str(uuid.uuid4())
        payment_id = str(uuid.uuid4())
        
        # Start workflow
        handle = await client.start_workflow(
            OrderWorkflow.run,
            {"order_id": order_id, "payment_id": payment_id},
            id=f"order-{order_id}",
            task_queue="order-tq"
        )
        
        # Wait briefly to let it reach validation
        await asyncio.sleep(0.5)
        
        # Simulate human approval
        await handle.signal("ApproveOrder")
        
        # Wait for workflow completion
        result = await handle.result()
        assert result == "Completed"
    finally:
        order_worker_task.cancel()
        shipping_worker_task.cancel()
        await test_db.close()
            
    end_time = time.time()
    duration = end_time - start_time
    print(f"Workflow completed in {duration:.2f} seconds")
    
    # Assert it completed within 15 seconds
    assert duration < 15.0

@pytest.mark.asyncio
async def test_order_workflow_cancellation():
    # Initialize DB schema
    from database import db, Database
    test_db = Database()
    await test_db.init_db()

    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="order-tq",
        workflows=[OrderWorkflow],
        activities=[
            order_received,
            order_validated,
            payment_charged,
            order_shipped,
            update_address_activity,
            cancel_order_activity
        ]
    )
    
    worker_task = asyncio.create_task(worker.run())
    try:
        order_id = str(uuid.uuid4())
        handle = await client.start_workflow(
            OrderWorkflow.run,
            {"order_id": order_id, "payment_id": "pay-123"},
            id=f"order-{order_id}",
            task_queue="order-tq"
        )
        
        # Immediately send cancellation signal
        await handle.signal("CancelOrder")
        
        result = await handle.result()
        assert result == "Cancelled"
    finally:
        worker_task.cancel()
        await test_db.close()
