from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
from temporalio.client import Client, WorkflowFailureError
from temporalio.exceptions import WorkflowAlreadyStartedError
from workflows import OrderWorkflow

app = FastAPI()

class StartOrderRequest(BaseModel):
    payment_id: str

class UpdateAddressRequest(BaseModel):
    address: Dict[str, Any]

async def get_temporal_client():
    return await Client.connect("localhost:7233")

@app.post("/orders/{order_id}/start")
async def start_order(order_id: str, req: StartOrderRequest):
    client = await get_temporal_client()
    try:
        handle = await client.start_workflow(
            OrderWorkflow.run,
            {"order_id": order_id, "payment_id": req.payment_id},
            id=f"order-{order_id}",
            task_queue="order-tq"
        )
        return {"message": "Workflow started", "workflow_id": handle.id}
    except WorkflowAlreadyStartedError:
        raise HTTPException(status_code=400, detail="Workflow already running")

@app.post("/orders/{order_id}/signals/cancel")
async def cancel_order(order_id: str):
    client = await get_temporal_client()
    handle = client.get_workflow_handle(f"order-{order_id}")
    await handle.signal("CancelOrder")
    return {"message": "Cancel signal sent"}

@app.post("/orders/{order_id}/signals/approve")
async def approve_order(order_id: str):
    client = await get_temporal_client()
    handle = client.get_workflow_handle(f"order-{order_id}")
    await handle.signal("ApproveOrder")
    return {"message": "Approve signal sent"}

@app.post("/orders/{order_id}/signals/address")
async def update_address(order_id: str, req: UpdateAddressRequest):
    client = await get_temporal_client()
    handle = client.get_workflow_handle(f"order-{order_id}")
    await handle.signal("UpdateAddress", req.address)
    return {"message": "UpdateAddress signal sent"}

@app.get("/orders/{order_id}/status")
async def get_order_status(order_id: str):
    client = await get_temporal_client()
    handle = client.get_workflow_handle(f"order-{order_id}")
    try:
        description = await handle.describe()
        # Query the custom status method we added
        custom_status = await handle.query("status")
        return {
            "workflow_id": description.id,
            "status": description.status.name,
            "custom_state": custom_status
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
