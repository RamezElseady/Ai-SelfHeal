# 🤖 AI-Driven Autonomous DevOps & Self-Healing System

> **The AI analyzes. The code decides.**

An automation and self-healing platform that runs inside Kubernetes. It receives live alerts from Prometheus Alertmanager, analyzes the cluster's operational context using Google Gemini models, automatically executes **safe** corrective actions, and sends instant audit reports to Slack.

---

## 🏗️ Architecture

```
Prometheus ──▶ Alertmanager ──▶ POST /webhook  (Flask, replies 202 Accepted instantly)
                                     │
                                     ▼
                    Pre-LLM Gatekeeper (ignore noise, check namespace, dedupe)
                                     │
                                     ▼
                  Cluster Context Collector (Deployments, Pods, logs, events)
                                     │
                                     ▼
                  Google Gemini  ──▶  structured JSON Diagnosis (Pydantic schema)
                                     │
                                     ▼
                       Deterministic Policy Engine (the code is the judge)
                          │              │               │
                       Execute        Propose          Block
                          └──────────────┴───────────────┘
                                         │
                                         ▼
                              Slack Audit Trail
```

---

## ✨ Core Engineering Features

- **Pre-LLM Gatekeeper:** Immediately ignores noisy administrative alerts (`Watchdog`, `InfoInhibitor`, `AlertmanagerFailedToSendAlerts`, `AlertmanagerClusterFailedToSendAlerts`) and verifies `ALLOWED_NAMESPACES` *before* invoking the AI, preventing unnecessary token consumption.
- **Deduplication & Concurrency:** Uses a `ThreadPoolExecutor` (max 3 workers) with thread synchronization (`Lock`) and a cooldown period (`AI_CALL_COOLDOWN_SECONDS`) to prevent processing the same alert multiple times concurrently.
- **Deep Cluster Context Collection:** Automatically pulls Deployment and Pod status, resource limits and requests, the last 20 log lines (`LOG_TAIL_LINES`) of failing containers, and the last 10 Warning events (capped at 7,000 characters) to give the AI full situational awareness.
- **Structured Output & Model Fallback:** Forces Gemini to answer with a strict JSON schema (`Diagnosis`, defined with Pydantic) and automatically retries or falls back between available flash models on API timeouts (504) or rate limits (429).
- **Deterministic Policy Engine:** The code, not the AI, is the final judge. Before any command runs, the engine verifies the AI's confidence against `MIN_CONFIDENCE`, checks the **live** cluster state, and enforces hard thresholds and hourly action caps.
- **Instant Slack Audit Trail:** Pushes a full report to Slack: severity, AI confidence, root cause, extracted evidence, chosen action, execution status, and manual steps recommended for human engineers.

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Kubernetes (AWS EKS) |
| Infrastructure as Code | Terraform |
| Monitoring & Alerting | Prometheus, Alertmanager (kube-prometheus-stack) |
| Backend | Python, Flask, Pydantic |
| AI | Google Gemini (`google-genai` SDK) |
| Notifications | Slack Incoming Webhooks |

---

## 📂 Repository Structure

```text
.
├── ai-devops-healing/        # Core Python webhook application and bot execution scripts
├── k8s-manifests/            # Core Kubernetes resource definitions and manifests
├── terraform/                # IaC files to provision AWS/EKS resources
├── alertmanager-values.yaml  # Alertmanager config that routes alerts to the webhook
├── demo-app-alerts.yaml      # Custom Prometheus alerting rules for application failures
├── test-oom-app.yaml         # Test deployment that simulates memory exhaustion (OOMKilled)
└── .gitignore                # Excludes system files, caches, large binaries, and private SSH keys
```

---

## 🛡️ Policy Engine & Supported Actions

The AI selects **exactly one** action from a closed list. The deterministic policy engine (`execute_with_policy`) then validates strict safety rules before applying any change:

| Action | Function | Enforced Safety Rules (Code Level) |
|---|---|---|
| `increase_memory` | Raises the container's memory limit | Requires concrete evidence of `OOMKilled`. The new limit never exceeds `MAX_MEMORY_MI`. Guarantees a minimum step increase of `MIN_MEMORY_STEP_MI`. |
| `scale_up` | Increases the replicas of a Deployment | Blocked if current replicas are `0` (intentional scale-down) or the Deployment already reached `MAX_REPLICAS`. |
| `restart` | Performs a rolling restart of the Deployment | Blocked if the Deployment is already healthy (`ready >= desired`). |
| `notify_human` | Analyzes and reports without auto-execution | Used for code bugs, bad images, low AI confidence, or when the AI service is unavailable (`safe_fallback`). |
| `none` | Takes no action | Used when the AI determines the alert is a false positive (`is_false_positive`) or the system is already stable. |

**Global guardrails applied to every action:** confidence ≥ `MIN_CONFIDENCE`, namespace allow-list, target Deployment must exist, per-Deployment cooldown (`ACTION_COOLDOWN_SECONDS`), cluster-wide hourly cap (`MAX_ACTIONS_PER_HOUR`), and an optional `DRY_RUN` mode.

**Typical outcomes**

| Situation | Expected AI decision |
|---|---|
| Container killed for exceeding its memory limit | `increase_memory` |
| Deployment unhealthy after a transient failure | `restart` |
| Crash caused by a code bug, bad image, or missing config | `notify_human` (with manual steps) |
| Alert fired but all replicas are healthy | `none` (false positive) |

---

## ⚙️ Environment Variables

Behavior, guardrails, and safety policies are configured through environment variables, with no source-code changes required.

| Variable | Default | Description |
|---|---|---|
| `GOOGLE_API_KEY` | **required** | Gemini API key (`GEMINI_API_KEY` is also accepted). |
| `GEMINI_MODELS` | `gemini-flash-lite-latest` | Comma-separated list of Gemini models used for fallback. |
| `SLACK_WEBHOOK_URL` | none | Slack webhook for audit reports. If unset, Slack reporting is disabled. |
| `DRY_RUN` | `false` | If `true`, the bot analyzes and proposes solutions but never modifies the cluster. |
| `ALLOWED_NAMESPACES` | `default` | Namespaces the bot is authorized to monitor and modify. |
| `MIN_CONFIDENCE` | `0.7` | Minimum AI confidence (0.0 to 1.0) required for auto-execution. |
| `MAX_REPLICAS` | `5` | Maximum replicas allowed when executing `scale_up`. |
| `SCALE_STEP` | `1` | Replicas added per `scale_up` action. |
| `MAX_MEMORY_MI` | `1024` | Absolute maximum memory limit (MiB) the bot may set. |
| `MEM_INCREASE_FACTOR` | `2.0` | Multiplier used to compute the new memory limit after an OOM crash. |
| `MIN_MEMORY_STEP_MI` | `32` | Minimum memory increment (MiB) so the pod can escape a crash loop. |
| `ACTION_COOLDOWN_SECONDS` | `300` | Cooldown between actions on the same Deployment. |
| `AI_CALL_COOLDOWN_SECONDS` | `120` | Cooldown that prevents duplicate AI calls for the same alert. |
| `MAX_ACTIONS_PER_HOUR` | `6` | Global cap on automated actions per hour across the cluster. |

---

## 🚀 API Endpoint

The Flask server listens on port `5000` and receives payloads from Prometheus Alertmanager.

- **Endpoint:** `POST /webhook`
- **Behavior:** Parses the JSON payload, filters for `firing` alerts, queues valid tasks into the background `ThreadPoolExecutor`, and immediately responds with `202 Accepted` so Alertmanager is never blocked.

---

## 🏁 Getting Started

### Prerequisites

- A Kubernetes cluster (AWS EKS) and `kubectl` access to it
- [Helm](https://helm.sh/) to install the monitoring stack
- [Terraform](https://www.terraform.io/) (optional, only to provision the infrastructure)
- A Google Gemini API key
- A Slack Incoming Webhook URL (optional, for audit reports)

### 1. Provision the infrastructure (optional)

```bash
cd terraform
terraform init && terraform apply
```

### 2. Install the monitoring stack with the Alertmanager routing

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f alertmanager-values.yaml
```

### 3. Load the alerting rules

```bash
kubectl apply -f demo-app-alerts.yaml
```

> The `release` label inside the rules file must match your Helm release name, otherwise Prometheus ignores the rules.

### 4. Create the secrets and deploy the agent

```bash
kubectl create secret generic ai-devops-secrets \
  --from-literal=GOOGLE_API_KEY=<your-gemini-key> \
  --from-literal=SLACK_WEBHOOK_URL=<your-slack-webhook>

kubectl apply -f k8s-manifests/
```

### Run locally (development)

The agent uses your local kubeconfig (or the in-cluster config when deployed).

```bash
cd ai-devops-healing
pip install flask requests kubernetes google-genai pydantic

export GOOGLE_API_KEY=<your-gemini-key>
export DRY_RUN=true
python3 webhook_receiver.py
```

> 💡 Start with `DRY_RUN=true` to review the AI's proposals in Slack before allowing real changes.

---

## 🧪 Testing

Simulate a memory-exhaustion incident:

```bash
kubectl apply -f test-oom-app.yaml
```

Once the OOM alert fires, the agent collects the cluster context, asks Gemini for a diagnosis, passes the decision through the policy engine, and posts the audit trail to Slack.

Or send a synthetic alert straight to the webhook:

```bash
curl -X POST http://localhost:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{"alerts":[{"status":"firing","labels":{"alertname":"ContainerOOMKilled","namespace":"default"}}]}'
```

Expected flow: alert → Gatekeeper → context collection → Gemini diagnosis → policy check → action (or proposal) → Slack audit trail.

---

## 🔐 Security Notes

- **The AI never executes anything.** It only proposes one action from a closed list, and the policy engine verifies every claim against the live cluster before acting.
- **Fail-safe by design:** if Gemini is unavailable, the system escalates to a human (`notify_human`) instead of guessing.
- **Blast-radius limits:** namespace allow-list, replica and memory caps, per-Deployment cooldown, and an hourly global action cap.
- **Least privilege:** grant the agent's ServiceAccount only `get`, `list`, and `patch` on the resources it needs, and never `delete`.
- **Secrets stay out of Git:** API keys and webhook URLs are provided through Kubernetes Secrets or environment variables.

---