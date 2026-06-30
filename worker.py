import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
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
from workflows import OrderWorkflow, ShippingWorkflow
from database import get_db_pool

async def main():
    logging.basicConfig(level=logging.INFO)
    
    # Initialize DB (and run schema migration)
    from database import db
    await db.init_db()
    
    # Connect client
    client = await Client.connect("localhost:7233")
    
    # Worker for OrderWorkflow
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
    
    # Worker for ShippingWorkflow
    shipping_worker = Worker(
        client,
        task_queue="shipping-tq",
        workflows=[ShippingWorkflow],
        activities=[
            package_prepared,
            carrier_dispatched
        ]
    )
    
    logging.info("Starting workers on order-tq and shipping-tq...")
    
    # Run both workers concurrently
    await asyncio.gather(
        order_worker.run(),
        shipping_worker.run()
    )

if __name__ == "__main__":
    asyncio.run(main())
