from flask import Flask, request, jsonify
import logging
import os
import math
import time
import json
import re
import requests
from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import List, Literal
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel
from kubernetes import client, config
from google import genai
from google.genai import types, errors

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ===========================================================================
# 1) الإعدادات (كلها قابلة للتعديل بـ env vars)
# ===========================================================================
GEMINI_MODELS = [
    m.strip()
    for m in os.environ.get("GEMINI_MODELS", os.environ.get("GEMINI_MODEL", "gemini-flash-lite-latest")).split(",")
    if m.strip()
]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

# --- Safety policy: اللي الكود بيسمح بيه مهما قال الـ AI ---
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"          # true = يحلل ويقترح بس
ALLOWED_NAMESPACES = {n.strip() for n in os.environ.get("ALLOWED_NAMESPACES", "default").split(",") if n.strip()}
MIN_CONFIDENCE = float(os.environ.get("MIN_CONFIDENCE", "0.7"))          # أقل ثقة للتنفيذ التلقائي
MAX_REPLICAS = int(os.environ.get("MAX_REPLICAS", "5"))                  # سقف الـ scale up
SCALE_STEP = int(os.environ.get("SCALE_STEP", "1"))                      # الزيادة في كل مرة
MAX_MEMORY_MI = int(os.environ.get("MAX_MEMORY_MI", "1024"))             # سقف الذاكرة
MEM_INCREASE_FACTOR = float(os.environ.get("MEM_INCREASE_FACTOR", "2.0")) # ضاعفنا المعامل الافتراضي لـ 2.0
MIN_MEMORY_STEP_MI = int(os.environ.get("MIN_MEMORY_STEP_MI", "32"))     # أقل زيادة في المرة الواحدة 32Mi
ACTION_COOLDOWN_SECONDS = int(os.environ.get("ACTION_COOLDOWN_SECONDS", "300"))
AI_CALL_COOLDOWN_SECONDS = int(os.environ.get("AI_CALL_COOLDOWN_SECONDS", "120")) # حماية التوكنز من التكرار
MAX_ACTIONS_PER_HOUR = int(os.environ.get("MAX_ACTIONS_PER_HOUR", "6"))  # سقف عام لكل الـ cluster

# التنبيهات الإدارية اللي لازم نتجاهلها فوراً عشان ما تحرقش Tokens
IGNORED_ALERTS = {
    "Watchdog",
    "InfoInhibitor",
    "AlertmanagerFailedToSendAlerts",
    "AlertmanagerClusterFailedToSendAlerts",
}

MAX_CONTEXT_CHARS = 7000
LOG_TAIL_LINES = 20
LOG_CHARS_PER_CONTAINER = 700

# ===========================================================================
# 2) Kubernetes + Gemini clients
# ===========================================================================
try:
    config.load_kube_config(config_file="/root/.kube/config")
except Exception:
    try:
        config.load_incluster_config()
    except Exception as e:
        logging.error(f"Failed to load kubernetes config: {e}")
v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

client_ai = genai.Client(http_options=types.HttpOptions(timeout=20_000))

executor = ThreadPoolExecutor(max_workers=3)

_state_lock = Lock()
_last_action = {}
_action_times = deque()
_inflight_alerts = set()   # لمنع دخول أكثر من Thread لنفس المشكلة في نفس اللحظة
_last_ai_call = {}         # لمنع استدعاء Gemini بشكل متكرر لنفس المشكلة


# ===========================================================================
# 3) شكل الرد من Gemini (حرية في التحليل، قائمة مقفولة في الـ action)
# ===========================================================================
class Diagnosis(BaseModel):
    summary: str
    root_cause: str
    evidence: List[str]
    severity: Literal["low", "medium", "high", "critical"]
    confidence: float
    is_false_positive: bool
    action: Literal["none", "notify_human", "restart", "scale_up", "increase_memory"]
    target_deployment: str
    target_container: str
    reasoning: str
    manual_steps: List[str]


def safe_fallback(reason):
    return Diagnosis(
        summary=reason, root_cause="AI analysis unavailable", evidence=[],
        severity="medium", confidence=0.0, is_false_positive=False,
        action="notify_human", target_deployment="", target_container="",
        reasoning="Gemini unavailable; escalating to a human.",
        manual_steps=["Investigate the alert manually (kubectl describe / logs / events)."],
    )


# ===========================================================================
# 4) جمع السياق من الـ cluster (كل اللي مهندس DevOps كان هيبصله)
# ===========================================================================
def _selector(dep):
    labels = (dep.spec.selector.match_labels or {}) if dep.spec.selector else {}
    return ",".join(f"{k}={v}" for k, v in labels.items())


def _state_text(state):
    if state is None:
        return "n/a"
    if state.waiting:
        return f"waiting({state.waiting.reason})"
    if state.terminated:
        t = state.terminated
        return f"terminated({t.reason}, exit={t.exit_code})"
    if state.running:
        return "running"
    return "n/a"


def _fetch_logs(namespace, pod, container, previous=False):
    try:
        out = v1.read_namespaced_pod_log(
            name=pod, namespace=namespace, container=container,
            tail_lines=LOG_TAIL_LINES, previous=previous,
        )
        return out.strip()[-LOG_CHARS_PER_CONTAINER:].replace("\n", " | ")
    except Exception:
        return ""


def build_cluster_context(namespace):
    lines = []
    unhealthy_log_budget = 4
    try:
        deployments = apps_v1.list_namespaced_deployment(namespace=namespace).items
        if not deployments:
            lines.append("No deployments found in this namespace.")
        for d in deployments:
            desired = d.spec.replicas or 0
            ready = d.status.ready_replicas or 0
            avail = d.status.available_replicas or 0
            lines.append(f"DEPLOYMENT {d.metadata.name}: desired={desired} ready={ready} available={avail}")
            for c in d.spec.template.spec.containers:
                limits = (c.resources.limits if c.resources and c.resources.limits else {})
                reqs = (c.resources.requests if c.resources and c.resources.requests else {})
                lines.append(f"  container {c.name}: image={c.image} limits={limits} requests={reqs}")

            sel = _selector(d)
            pods = v1.list_namespaced_pod(namespace=namespace, label_selector=sel).items if sel else []
            for pod in pods:
                for cs in (pod.status.container_statuses or []):
                    lines.append(
                        f"  POD {pod.metadata.name}/{cs.name}: phase={pod.status.phase} ready={cs.ready} "
                        f"restarts={cs.restart_count} state={_state_text(cs.state)} last={_state_text(cs.last_state)}"
                    )
                    if (not cs.ready or cs.restart_count > 0) and unhealthy_log_budget > 0:
                        unhealthy_log_budget -= 1
                        txt = _fetch_logs(namespace, pod.metadata.name, cs.name, previous=cs.restart_count > 0) \
                            or _fetch_logs(namespace, pod.metadata.name, cs.name, previous=False)
                        lines.append(f"    logs: {txt or '(no logs)'}")
    except Exception as e:
        lines.append(f"Error collecting deployments/pods: {e}")

    try:
        events = [e for e in v1.list_namespaced_event(namespace=namespace).items if e.type == "Warning"]
        events.sort(key=lambda e: (e.last_timestamp or e.event_time or e.metadata.creation_timestamp))
        for e in events[-10:]:
            lines.append(f"EVENT {e.reason} {e.involved_object.kind}/{e.involved_object.name}: {(e.message or '')[:200]}")
    except Exception as e:
        lines.append(f"Error collecting events: {e}")

    ctx = "\n".join(lines)
    return ctx[:MAX_CONTEXT_CHARS]


# ===========================================================================
# 5) Gemini: تحليل حر + Structured output + Fallback بين الموديلات
# ===========================================================================
NON_TEXT_KEYWORDS = ("tts", "image", "audio", "live", "embed", "imagen", "veo",
                     "robotics", "computer", "native", "transcribe")


def list_flash_models():
    try:
        found = []
        for m in client_ai.models.list():
            name = m.name.replace("models/", "")
            if "flash" not in name or any(k in name for k in NON_TEXT_KEYWORDS):
                continue
            if "generateContent" in (m.supported_actions or []):
                found.append(name)
        return found
    except Exception as e:
        logging.warning(f"Could not list models: {e}")
        return []


def build_prompt(alert_name, alert_labels, context):
    return f"""You are a senior Site Reliability Engineer analysing a live Kubernetes incident.

ALERT: {alert_name}
ALERT LABELS: {json.dumps(alert_labels, ensure_ascii=False)}

CLUSTER SNAPSHOT (collected just now):
{context}

YOUR JOB
1. Analyse deeply. Correlate deployment status, pod/container states, exit codes, restart counts,
   resource limits, warning events and logs. Explain what is really happening and why.
   Cite concrete evidence from the snapshot. Think like an experienced engineer, not a pattern matcher.
2. Decide whether the alert is real or a false positive.
3. Pick exactly ONE automated action from this closed list (the system will independently verify it):
   - none            : nothing to do (e.g. false positive, or already healthy)
   - notify_human    : the problem needs a human (code bug, bad config, bad image, external dependency...)
   - restart         : rolling restart of a deployment (only sensible for a genuinely unhealthy deployment)
   - scale_up        : add replicas (only for capacity / load problems)
   - increase_memory : raise a container memory limit (only for confirmed OOMKilled)
4. Anything that needs other fixes (config change, image rollback, code fix, etc.) goes in manual_steps.
   You may recommend ANYTHING there, but you cannot execute it.

RULES
- target_deployment must be a deployment name that appears in the snapshot.
- For increase_memory, set target_container to the container name; otherwise use an empty string.
- Be honest about confidence (0.0-1.0). If the evidence is weak, lower it and prefer notify_human.
- Your action must be consistent with your root_cause. If you believe it is a false positive, choose none.
"""


def ask_gemini(alert_name, alert_labels, context):
    prompt = build_prompt(alert_name, alert_labels, context)
    schema_cfg = types.GenerateContentConfig(
        temperature=0.2, max_output_tokens=2048,
        response_mime_type="application/json", response_schema=Diagnosis,
    )
    plain_cfg = types.GenerateContentConfig(
        temperature=0.2, max_output_tokens=2048, response_mime_type="application/json",
    )

    models = list(GEMINI_MODELS)
    discovered = False
    i = 0
    while i < len(models):
        model = models[i]
        use_schema = True
        for attempt in range(3):
            try:
                resp = client_ai.models.generate_content(
                    model=model, contents=prompt, config=schema_cfg if use_schema else plain_cfg,
                )
                if use_schema and getattr(resp, "parsed", None):
                    return resp.parsed
                text = (resp.text or "").strip()
                match = re.search(r"\{.*\}", text, re.DOTALL)
                return Diagnosis.model_validate_json(match.group(0) if match else text)

            except errors.ClientError as e:
                if e.code == 404:
                    logging.warning(f"Model '{model}' not found, next...")
                    break
                if e.code == 429:
                    time.sleep(3 * (attempt + 1))
                    continue
                if e.code == 400 and use_schema:
                    logging.warning(f"'{model}' rejected schema, retrying without it")
                    use_schema = False
                    continue
                logging.error(f"Client error {e.code} on '{model}': {e}")
                break

            except errors.ServerError as e:
                if e.code == 504:
                    logging.warning(f"'{model}' timed out (504), skipping to next model")
                    break
                wait = 2 * (attempt + 1)
                logging.warning(f"'{model}' server error {e.code}, retry in {wait}s")
                time.sleep(wait)

            except Exception as e:
                logging.warning(f"'{model}' attempt {attempt + 1} failed: {e}")
                time.sleep(1)

        i += 1
        if i == len(models) and not discovered:
            discovered = True
            extra = [m for m in list_flash_models() if m not in models][:6]
            if extra:
                logging.info(f"Trying discovered models: {extra}")
                models += extra

    return safe_fallback("Gemini unavailable or timed out on all models.")


# ===========================================================================
# 6) Policy engine: الكود هو الحَكَم، مش الـ AI
# ===========================================================================
_MEM_UNITS = (("Ki", 1024), ("Mi", 1024 ** 2), ("Gi", 1024 ** 3), ("K", 1000), ("M", 1000 ** 2), ("G", 1000 ** 3))


def mem_to_mib(q):
    try:
        q = str(q).strip()
        for suffix, mult in _MEM_UNITS:
            if q.endswith(suffix):
                return float(q[:-len(suffix)]) * mult / (1024 ** 2)
        return float(q) / (1024 ** 2)
    except Exception:
        return None


def find_oom_container(dep, namespace):
    sel = _selector(dep)
    if not sel:
        return None
    for pod in v1.list_namespaced_pod(namespace=namespace, label_selector=sel).items:
        for cs in (pod.status.container_statuses or []):
            for st in (cs.state, cs.last_state):
                if st and st.terminated and st.terminated.reason == "OOMKilled":
                    return cs.name
    return None


def acquire_ai_slot(dedup_key):
    """يتأكد إن مفيش Thread تاني شغال على نفس التنبيه، وإننا مش في فترة Cooldown للـ AI"""
    now = time.time()
    with _state_lock:
        if dedup_key in _inflight_alerts:
            return False, f"already processing '{dedup_key}' in another thread"
        elapsed = now - _last_ai_call.get(dedup_key, 0)
        if elapsed < AI_CALL_COOLDOWN_SECONDS:
            return False, f"AI cooldown active for '{dedup_key}' ({int(elapsed)}s < {AI_CALL_COOLDOWN_SECONDS}s)"
        _inflight_alerts.add(dedup_key)
        _last_ai_call[dedup_key] = now
    return True, ""


def release_ai_slot(dedup_key):
    with _state_lock:
        _inflight_alerts.discard(dedup_key)


def reserve_action(key):
    """Cooldown لكل deployment + سقف عام للـ actions في الساعة"""
    now = time.time()
    with _state_lock:
        elapsed = now - _last_action.get(key, 0)
        if elapsed < ACTION_COOLDOWN_SECONDS:
            return False, f"cooldown active ({int(elapsed)}s since last action, need {ACTION_COOLDOWN_SECONDS}s)"
        while _action_times and now - _action_times[0] > 3600:
            _action_times.popleft()
        if len(_action_times) >= MAX_ACTIONS_PER_HOUR:
            return False, f"hourly action cap reached ({MAX_ACTIONS_PER_HOUR}/hour)"
        _last_action[key] = now
        _action_times.append(now)
    return True, ""


def execute_with_policy(d: Diagnosis, namespace):
    if d.is_false_positive or d.action in ("none", "notify_human"):
        return "proposed", "No automated action taken (analysis only / escalated to a human)."

    if d.confidence < MIN_CONFIDENCE:
        return "proposed", f"Confidence {d.confidence:.2f} < {MIN_CONFIDENCE}; not auto-executing. Escalated to a human."

    if namespace not in ALLOWED_NAMESPACES:
        return "blocked", f"Namespace '{namespace}' is not in the allowed list {sorted(ALLOWED_NAMESPACES)}."

    name = d.target_deployment
    try:
        dep = apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
    except Exception:
        return "blocked", f"Deployment '{name}' does not exist in '{namespace}'."

    desired = dep.spec.replicas or 0
    ready = dep.status.ready_replicas or 0
    apply = None
    description = ""

    if d.action == "restart":
        if desired > 0 and ready >= desired:
            return "blocked", f"'{name}' is healthy ({ready}/{desired} ready). Restart not needed."
        description = f"rolling restart of '{name}' ({ready}/{desired} ready)"
        body = {"spec": {"template": {"metadata": {"annotations": {
            "kubectl.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()}}}}}
        apply = lambda: apps_v1.patch_namespaced_deployment(name=name, namespace=namespace, body=body)

    elif d.action == "scale_up":
        if desired == 0:
            return "blocked", f"'{name}' is scaled to 0 (possibly intentional). Not scaling up."
        if desired >= MAX_REPLICAS:
            return "blocked", f"'{name}' already at max replicas ({desired}/{MAX_REPLICAS})."
        new_replicas = min(desired + SCALE_STEP, MAX_REPLICAS)
        description = f"scale '{name}' from {desired} to {new_replicas} replicas (cap {MAX_REPLICAS})"
        apply = lambda: apps_v1.patch_namespaced_deployment_scale(
            name=name, namespace=namespace, body={"spec": {"replicas": new_replicas}})

    elif d.action == "increase_memory":
        container = find_oom_container(dep, namespace)
        if not container:
            return "blocked", "No OOMKilled evidence in the cluster for this deployment. Memory change refused."
        spec_c = next((c for c in dep.spec.template.spec.containers if c.name == container), None)
        limit = spec_c.resources.limits.get("memory") if spec_c and spec_c.resources and spec_c.resources.limits else None
        current_mi = mem_to_mib(limit) if limit else None
        if current_mi is None:
            return "blocked", f"Container '{container}' has no readable memory limit; refusing to guess."
        if current_mi >= MAX_MEMORY_MI:
            return "blocked", f"Memory limit already at/above cap ({current_mi:.0f}Mi >= {MAX_MEMORY_MI}Mi)."
        # ضمان قفزة كافية (على الأقل +32Mi أو الضعف) لتفادي OOMKilled المتكرر في القيم الصغيرة
        calculated_mi = max(math.ceil(current_mi * MEM_INCREASE_FACTOR), current_mi + MIN_MEMORY_STEP_MI, 64)
        new_mi = int(min(calculated_mi, MAX_MEMORY_MI))
        description = f"raise memory limit of '{name}/{container}' from {current_mi:.0f}Mi to {new_mi}Mi (cap {MAX_MEMORY_MI}Mi)"
        body = {"spec": {"template": {"spec": {"containers": [
            {"name": container, "resources": {"limits": {"memory": f"{new_mi}Mi"}}}]}}}}
        apply = lambda: apps_v1.patch_namespaced_deployment(name=name, namespace=namespace, body=body)

    else:
        return "blocked", f"Action '{d.action}' is not in the allowed list."

    if DRY_RUN:
        return "dry_run", f"[DRY-RUN] Would {description}."

    ok, why = reserve_action(f"{namespace}/{name}")
    if not ok:
        return "blocked", f"Rate limit: {why}."

    try:
        apply()
        return "executed", f"Executed: {description}."
    except Exception as e:
        return "failed", f"Failed to {description}: {e}"


# ===========================================================================
# 7) Slack audit trail
# ===========================================================================
STATUS_EMOJI = {"executed": "✅", "proposed": "💡", "blocked": "🛡️", "dry_run": "🧪", "failed": "❌"}
SEVERITY_EMOJI = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}


def send_slack_report(alert_name, namespace, d: Diagnosis, status, detail):
    if not SLACK_WEBHOOK_URL:
        return
    evidence = "\n".join(f"  • {e}" for e in d.evidence[:6]) or "  • (none)"
    manual = "\n".join(f"  • {s}" for s in d.manual_steps[:6]) or "  • (none)"
    text = (
        f"🤖 *AI Autonomous DevOps Report*\n"
        f"• *Alert:* `{alert_name}`  |  *Namespace:* `{namespace}`\n"
        f"• *Severity:* {SEVERITY_EMOJI.get(d.severity, '')} `{d.severity}`  |  "
        f"*Confidence:* `{d.confidence:.2f}`  |  *False positive:* `{d.is_false_positive}`\n\n"
        f"*🔍 Analysis:*\n{d.summary}\n\n"
        f"*🎯 Root cause:*\n{d.root_cause}\n\n"
        f"*📎 Evidence:*\n{evidence}\n\n"
        f"*🧠 AI decision:* `{d.action}` on `{d.target_deployment or '-'}`\n_{d.reasoning}_\n\n"
        f"*{STATUS_EMOJI.get(status, '')} Execution ({status}):*\n{detail}\n\n"
        f"*🧑‍🔧 Manual steps (not auto-executed):*\n{manual}"
    )
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        logging.error(f"Slack error: {e}")


# ===========================================================================
# 8) الـ Workflow + الـ Webhook (مع بوابة الحماية Pre-LLM Gatekeeper)
# ===========================================================================
def process_alert(alert_name, namespace, alert_labels, dedup_key):
    try:
        logging.info(f"1️⃣ Collecting cluster context for [{dedup_key}]...")
        context = build_cluster_context(namespace)
        logging.info(f"2️⃣ Context ready ({len(context)} chars). Asking Gemini for deep analysis...")

        diagnosis = ask_gemini(alert_name, alert_labels, context)
        logging.info(f"3️⃣ Diagnosis: {json.dumps(diagnosis.model_dump(), ensure_ascii=False)}")

        status, detail = execute_with_policy(diagnosis, namespace)
        logging.info(f"4️⃣ Policy result -> {status}: {detail}")

        send_slack_report(alert_name, namespace, diagnosis, status, detail)
    except Exception:
        logging.exception("process_alert crashed")
    finally:
        release_ai_slot(dedup_key)


@app.route('/webhook', methods=['POST'])
def receive_alert():
    data = request.get_json(silent=True) or {}
    global_status = data.get('status', 'firing')
    queued_count = 0

    for alert in data.get('alerts', []):
        if (alert.get('status') or global_status) != "firing":
            continue

        labels = alert.get('labels', {})
        alert_name = labels.get('alertname', 'unknown')
        namespace = labels.get('namespace', 'default')

        # 1. تجاهل التنبيهات الإدارية والـ Heartbeat فوراً
        if alert_name in IGNORED_ALERTS:
            logging.info(f"⏭️ Ignoring noisy/system alert: {alert_name}")
            continue

        # 2. تجاهل الـ Namespaces غير المسموح بها قبل استهلاك أي Tokens
        if namespace not in ALLOWED_NAMESPACES:
            logging.info(f"⏭️ Skipping alert '{alert_name}' in non-allowed namespace '{namespace}'")
            continue

        # 3. منع التكرار والـ Race Condition قبل إرسال المهمة للـ ThreadPool
        dedup_key = f"{namespace}/{alert_name}"
        ok, reason = acquire_ai_slot(dedup_key)
        if not ok:
            logging.info(f"🛡️ Suppressed duplicate alert: {reason}")
            continue

        logging.info(f"🚨 Queuing autonomous workflow for [{dedup_key}]...")
        queued_count += 1
        executor.submit(process_alert, alert_name, namespace, labels, dedup_key)

    return jsonify({"status": "accepted", "queued": queued_count}), 202


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


    