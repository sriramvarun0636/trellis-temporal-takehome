import argparse
import subprocess
import requests
import json
import time

API_URL = "http://localhost:8000"

def start_infra():
    print("Starting Temporal server and database...")
    subprocess.run(["docker-compose", "up", "-d"], check=True)
    print("Infrastructure started. Waiting a few seconds for DB to be ready...")
    time.sleep(5)

def start_workers():
    print("Starting workers...")
    subprocess.Popen(["python", "worker.py"])
    print("Workers started.")

def start_api():
    print("Starting API server...")
    subprocess.Popen(["uvicorn", "api:app", "--port", "8000"])
    print("API server started.")

def trigger_workflow(order_id, payment_id):
    resp = requests.post(
        f"{API_URL}/orders/{order_id}/start",
        json={"payment_id": payment_id}
    )
    print(resp.json())

def send_signal(order_id, signal_type, payload=None):
    if signal_type == "cancel":
        resp = requests.post(f"{API_URL}/orders/{order_id}/signals/cancel")
    elif signal_type == "approve":
        resp = requests.post(f"{API_URL}/orders/{order_id}/signals/approve")
    elif signal_type == "address":
        resp = requests.post(
            f"{API_URL}/orders/{order_id}/signals/address",
            json={"address": payload or {"city": "New Address"}}
        )
    else:
        print("Unknown signal type")
        return
    print(resp.json())

def inspect_state(order_id):
    resp = requests.get(f"{API_URL}/orders/{order_id}/status")
    print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trellis Temporal CLI")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("start-infra", help="Start Temporal and Postgres")
    subparsers.add_parser("start-workers", help="Start Temporal Workers")
    subparsers.add_parser("start-api", help="Start FastAPI Server")
    
    start_parser = subparsers.add_parser("start-order", help="Start an order workflow")
    start_parser.add_argument("order_id")
    start_parser.add_argument("payment_id")

    signal_parser = subparsers.add_parser("signal", help="Send a signal")
    signal_parser.add_argument("order_id")
    signal_parser.add_argument("type", choices=["cancel", "approve", "address"])

    inspect_parser = subparsers.add_parser("inspect", help="Inspect workflow state")
    inspect_parser.add_argument("order_id")

    args = parser.parse_args()

    if args.command == "start-infra":
        start_infra()
    elif args.command == "start-workers":
        start_workers()
    elif args.command == "start-api":
        start_api()
    elif args.command == "start-order":
        trigger_workflow(args.order_id, args.payment_id)
    elif args.command == "signal":
        send_signal(args.order_id, args.type)
    elif args.command == "inspect":
        inspect_state(args.order_id)
    else:
        parser.print_help()
