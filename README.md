<div align="center">

<h1>📦 Trellis Temporal Orchestration</h1>

<p>
  <strong>Fault-Tolerant, Deterministic Order Lifecycle System</strong><br/>
  <em>Engineered for Resiliency, Idempotent Execution, and Sub-15s SLAs.</em>
</p>

<p>
  <a href="https://www.python.org/">
    <img src="https://img.shields.io/badge/Python-3.11+-3776AB.svg?style=for-the-badge&logo=python&logoColor=white" alt="Python Version">
  </a>
  <img src="https://img.shields.io/badge/Temporal-SDK-000000?style=for-the-badge" alt="Temporal">
  <img src="https://img.shields.io/badge/Database-PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Testing-Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="Testing">
</p>

<hr style="border: 0; height: 1px; background: linear-gradient(to right, transparent, #3776AB, transparent);" />

</div>

> **Trellis Temporal Orchestration** is an **event-driven, highly reliable state machine** designed to manage an end-to-end e-commerce order lifecycle.
> Built on Temporal's durability engine, it dynamically handles asynchronous shipping coordination, human-in-the-loop approvals, and mathematically synthesizes reliable outcomes from wildly unstable 3rd-party dependencies.
>
> *An implementation of parent-child workflows, signaling, and strict SLA enforcement.*

---

### ⚡ Core Engineering Challenges Solved 

While executing basic linear tasks is trivial, the true achievement of this system is its ability to meet strict execution deadlines while relying on severely compromised infrastructure:
* **Deterministic SLA Enforcement:** Engineered a configuration pattern capable of bypassing random 5-minute third-party API hangs (`asyncio.sleep(300)`). By utilizing aggressive `StartToCloseTimeout` bounds (0.1s) and rapid exponential backoffs, the orchestrator guarantees a strict **<15s execution SLA** across the entire lifecycle.
* **Idempotent State Mutations:** External side effects (e.g., `payment_charged`) must survive aggressive Temporal retries without duplicating charges. Built a stateful integration via `asyncpg` utilizing `INSERT ... ON CONFLICT (payment_id) DO NOTHING` to guarantee absolute financial idempotency.
* **Asynchronous Human-in-the-Loop:** Replaced rigid polling with event-driven `wait_condition` signals, securely pausing the `OrderWorkflow` in a resource-free state until a human administrator issues an `ApproveOrder` signal via the API.
* **Parent-Child Self-Healing Loops:** Segregated logistics processing into an isolated `ShippingWorkflow` on a dedicated `shipping-tq` task queue. If the carrier fails permanently after 3 attempts, it triggers a `DispatchFailed` signal cascade back to the parent for organic compensation and recursive retries.

---

### 📊 Evaluation & SLA Constraints (Integration Matrix)

The system is evaluated against a strict integration harness (`test_workflows.py`) containing simulated unreliability. It is evaluated strictly on **End-to-End Completion** within a hard-capped time limit.

#### 1. Resolution Metrics (Reliability & Speed)
| Metric | Result | Notes |
| :--- | :--- | :--- |
| **Simulated Failure Rate** | **67%** | Each activity randomly crashes (33%) or hangs for 5 minutes (33%). |
| **Success SLA Target** | **< 15.0s** | The maximum allowed wall-clock time for the workflow to finish. |
| **Actual Time to Resolution** | **~6 - 11 seconds** | Achieved via immediate activity cancellation and rapid backoff parameters. |
| **Idempotency Leaks** | **0** | Verified 0 double-charges on payment or state corruption. |

#### 2. Workflow State Transitions
*   `received` ➔ `validated` ➔ `[WAIT FOR APPROVAL]` ➔ `paid` ➔ `[SPAWN CHILD WORKFLOW]` ➔ `package_prepared` ➔ `carrier_dispatched` ➔ `shipped`

---

### 🏗️ System Architecture: Resilient & Event-Driven

The application employs a decoupled architecture. The FastAPI layer acts purely as an ingestion layer, immediately returning a `run_id` to the client. The Temporal Server handles all persistence and state tracking, while Python Workers pull tasks and execute them.

```mermaid
%%{init: {'theme': 'dark', 'themeVariables': { 'primaryColor': '#1f2023', 'primaryTextColor': '#f0f0f0', 'lineColor': '#a0a0a0', 'secondaryColor': '#2a2a2e'}, 'themeCSS': '.mermaid svg { max-width: 800px; margin: 0 auto; }'}}%%
graph TD
    subgraph UI_Layer["External Interfaces"]
        A["CLI Tool (cli.py)"] <-->|REST| B["FastAPI Server (api.py)"]
        C["curl / HTTP Clients"] <-->|REST| B
    end

    subgraph Control_Plane["Temporal Infrastructure"]
        B -->|gRPC: Start & Signal| D["Temporal Server (Docker)"]
        D <--> E[("Temporal Internal DB")]
    end

    subgraph Execution_Engine["Python Workers (worker.py)"]
        D -.->|Task Queue: order-tq| F["OrderWorkflow (Parent)"]
        D -.->|Task Queue: shipping-tq| G["ShippingWorkflow (Child)"]
        
        F -->|wait_condition| H{"Human Approval Signal"}
        F -->|Spawn & Monitor| G
        G -.->|Signal: DispatchFailed| F
    end

    subgraph State_Layer["Application State"]
        F -->|Idempotent Writes| I[("PostgreSQL (App DB)")]
        G -->|Idempotent Writes| I
    end

    %% Styling
    style A fill:#0077c8,stroke:#fff
    style B fill:#009688,stroke:#fff
    style D fill:#673ab7,stroke:#fff
    style E fill:#333,stroke:#fff
    style F fill:#fb8c00,stroke:#fff
    style G fill:#e65100,stroke:#fff,stroke-width:2px,color:#fff
    style I fill:#336791,stroke:#fff
```

* **🌐 `FastAPI Server`:** Ingests workflow triggers, signals, and queries, communicating entirely via the Temporal gRPC client.
* **🧠 `OrderWorkflow` (Parent):** Manages the high-level state machine, idempotency, and asynchronous pauses.
* **📦 `ShippingWorkflow` (Child):** Isolated execution path for dispatching carriers, allowing failure isolation.
* **💾 `PostgreSQL`:** The final source of truth for business events, populated using `INSERT ON CONFLICT` blocks.

---

### 🛠️ Technology Stack & Key Libraries

<p align="left">
  <a href="https://www.python.org/" target="_blank"><img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"></a>
  <a href="https://temporal.io/" target="_blank"><img src="https://img.shields.io/badge/Temporal-SDK-000000?style=for-the-badge&logo=temporal&logoColor=white" alt="Temporal"></a>
  <a href="https://fastapi.tiangolo.com/" target="_blank"><img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI"></a>
  <a href="https://www.postgresql.org/" target="_blank"><img src="https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"></a>
  <a href="https://pytest.org/" target="_blank"><img src="https://img.shields.io/badge/Pytest-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white" alt="Pytest"></a>
  <a href="https://www.docker.com/" target="_blank"><img src="https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"></a>
</p>

---

### 🚀 Getting Started

1.  **Start Infrastructure (Temporal Server & PostgreSQL):**
    ```bash
    docker-compose up -d
    ```
    *Wait ~10 seconds for the PostgreSQL instance to initialize.*

2.  **Environment Setup:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Launch the System Components:**
    You must run the worker and API server in separate terminals (ensure the venv is activated in both).
    
    *Terminal 1 (Execution Engine):*
    ```bash
    python worker.py
    ```
    *(Note: This automatically provisions `schema.sql` into the local PostgreSQL instance).*
    
    *Terminal 2 (API Gateway):*
    ```bash
    uvicorn api:app --reload --port 8000
    ```

4.  **Using the Built-In CLI (`cli.py`):**
    We include a wrapper CLI for interacting with the system without raw HTTP requests.
    
    ```bash
    # 1. Start a new workflow
    python cli.py start-order ORDER_123 PAY_123

    # 2. Check its status (Will be paused waiting for approval)
    python cli.py inspect ORDER_123

    # 3. Issue the Human-in-the-loop Approval
    python cli.py signal ORDER_123 approve
    
    # 4. Issue a cancellation
    python cli.py signal ORDER_123 cancel
    ```

5.  **Running the Automated Test Harness:**
    This guarantees that the aggressive timeouts successfully resolve the simulated 5-minute hangs in under 15 seconds.
    ```bash
    pytest test_workflows.py -v -s
    ```

<div align="center">
<i>Building Unbreakable Software on Unreliable Infrastructure.</i>
</div>
