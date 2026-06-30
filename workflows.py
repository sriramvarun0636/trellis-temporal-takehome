import asyncio
from datetime import timedelta
from typing import Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
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

@workflow.defn
class ShippingWorkflow:
    @workflow.run
    async def run(self, order: Dict[str, Any]) -> str:
        # Aggressive retries for package preparation
        default_retry = RetryPolicy(
            initial_interval=timedelta(seconds=0.1),
            maximum_interval=timedelta(seconds=0.5)
        )
        
        # For dispatch, we restrict attempts to show the signal logic
        dispatch_retry = RetryPolicy(
            initial_interval=timedelta(milliseconds=50),
            maximum_interval=timedelta(milliseconds=200),
            maximum_attempts=3
        )
        
        try:
            await workflow.execute_activity(
                package_prepared,
                order,
                start_to_close_timeout=timedelta(seconds=0.1),
                retry_policy=default_retry
            )
            
            await workflow.execute_activity(
                carrier_dispatched,
                order,
                start_to_close_timeout=timedelta(seconds=0.1),
                retry_policy=dispatch_retry
            )
            return "Shipped"
        except Exception as e:
            # Dispatch failed, signal parent
            parent = workflow.info().parent
            if parent:
                # Signal parent asking to retry
                parent_handle = workflow.get_external_workflow_handle(parent.workflow_id, run_id=parent.run_id)
                await parent_handle.signal("DispatchFailed", str(e))
            raise ApplicationError("Shipping failed") from e

@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.is_cancelled = False
        self.is_approved = False
        self.dispatch_failed = False
        self.pending_address_update = None
        
    @workflow.signal(name="CancelOrder")
    async def cancel_order(self) -> None:
        workflow.logger.info("Received CancelOrder signal")
        self.is_cancelled = True
        
    @workflow.signal(name="ApproveOrder")
    async def approve_order(self) -> None:
        workflow.logger.info("Received ApproveOrder signal")
        self.is_approved = True
        
    @workflow.signal(name="UpdateAddress")
    async def update_address(self, new_address: Dict[str, Any]) -> None:
        workflow.logger.info("Received UpdateAddress signal", extra={"new_address": new_address})
        self.pending_address_update = new_address
        
    @workflow.signal(name="DispatchFailed")
    async def handle_dispatch_failed(self, reason: str) -> None:
        workflow.logger.error("Received DispatchFailed signal", extra={"reason": reason})
        self.dispatch_failed = True

    @workflow.query(name="status")
    def status(self) -> Dict[str, Any]:
        return {
            "is_cancelled": self.is_cancelled,
            "is_approved": self.is_approved,
            "pending_address_update": self.pending_address_update
        }

    @workflow.run
    async def run(self, payload: Dict[str, Any]) -> str:
        order_id = payload["order_id"]
        payment_id = payload["payment_id"]
        
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=0.1),
            maximum_interval=timedelta(seconds=0.5)
        )
        activity_timeout = timedelta(seconds=0.1)
        
        # 1. Receive Order
        workflow.logger.info("Starting order_received activity")
        order = await workflow.execute_activity(
            order_received,
            order_id,
            start_to_close_timeout=activity_timeout,
            retry_policy=retry_policy
        )
        
        if self.is_cancelled:
            workflow.logger.info("Order was cancelled after receipt")
            await self._run_cancel(order_id, retry_policy)
            return "Cancelled"
            
        # 2. Validate Order
        workflow.logger.info("Starting order_validated activity")
        await workflow.execute_activity(
            order_validated,
            order,
            start_to_close_timeout=activity_timeout,
            retry_policy=retry_policy
        )
        
        if self.is_cancelled:
            workflow.logger.info("Order was cancelled after validation")
            await self._run_cancel(order_id, retry_policy)
            return "Cancelled"
            
        # 3. Wait for Manual Review
        workflow.logger.info("Waiting for manual review approval...")
        try:
            await workflow.wait_condition(
                lambda: self.is_approved or self.is_cancelled,
                timeout=timedelta(seconds=8) # 8 seconds max wait for testing purposes
            )
        except asyncio.TimeoutError:
            # In a real app this might cancel or escalate; here we will auto-approve for the test if it times out
            # Actually, let's just proceed as if approved if timeout is hit, or fail. 
            # We will raise ApplicationError for strictness.
            raise ApplicationError("Manual review timed out")
        
        if self.is_cancelled:
            workflow.logger.info("Order was cancelled during manual review")
            await self._run_cancel(order_id, retry_policy)
            return "Cancelled"
            
        # Process pending address update before payment/shipping
        if self.pending_address_update:
            workflow.logger.info("Processing pending address update before payment")
            await workflow.execute_activity(
                update_address_activity,
                {"order_id": order_id, "address": self.pending_address_update},
                start_to_close_timeout=activity_timeout,
                retry_policy=retry_policy
            )
            
        # 4. Charge Payment
        workflow.logger.info("Starting payment_charged activity")
        await workflow.execute_activity(
            payment_charged,
            {"order": order, "payment_id": payment_id},
            start_to_close_timeout=activity_timeout,
            retry_policy=retry_policy
        )
        
        # 5. Shipping Workflow (Child)
        workflow.logger.info("Starting ShippingWorkflow")
        shipping_success = False
        attempt = 1
        while not shipping_success:
            self.dispatch_failed = False
            workflow.logger.info(f"ShippingWorkflow attempt {attempt}")
            try:
                # Add attempt to workflow ID to avoid ID reuse issues in child workflow retries
                await workflow.execute_child_workflow(
                    ShippingWorkflow.run,
                    order,
                    id=f"shipping-{order_id}-{attempt}",
                    task_queue="shipping-tq"
                )
                shipping_success = True
            except ChildWorkflowError:
                # Wait for the signal to be processed
                try:
                    await workflow.wait_condition(lambda: self.dispatch_failed, timeout=timedelta(seconds=1))
                except asyncio.TimeoutError:
                    pass
                
                if not self.dispatch_failed:
                    raise ApplicationError("Shipping failed for unknown reason")
                
                attempt += 1
                await asyncio.sleep(0.1) # brief pause before retry
                
        # 6. Mark Order as Shipped
        workflow.logger.info("Starting order_shipped activity")
        await workflow.execute_activity(
            order_shipped,
            order,
            start_to_close_timeout=activity_timeout,
            retry_policy=retry_policy
        )

        workflow.logger.info("OrderWorkflow completed successfully")
        return "Completed"
        
    async def _run_cancel(self, order_id: str, retry_policy: RetryPolicy):
        await workflow.execute_activity(
            cancel_order_activity,
            order_id,
            start_to_close_timeout=timedelta(seconds=0.1),
            retry_policy=retry_policy
        )
