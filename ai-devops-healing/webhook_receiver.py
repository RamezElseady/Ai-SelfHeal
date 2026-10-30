from flask import Flask, request, jsonify
import logging
import os
import time
import json
import requests
from datetime import datetime, timezone
from kubernetes import client, config
import google.generativeai as genai

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# تهيئة إعدادات الـ Kubernetes API
try:
    config.load_kube_config()
except Exception:
    config.load_incluster_config()

v1 = client.CoreV1Api()
apps_v1 = client.AppsV1Api()

# تهيئة عميل Google Gemini
api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
genai.configure(api_key=api_key)
# استخدام إعدادات الـ JSON Response لو متاح، أو التوجيه الصارم في الـ Prompt
gemini_model = genai.GenerativeModel('gemini-3.8-flash')

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")

def send_slack_notification(alert_name, namespace, diagnosis, action_taken):
    """إرسال تقرير التشخيص والقرار كـ Audit Trail إلى Slack"""
    if not SLACK_WEBHOOK_URL:
        return

    slack_payload = {
        "text": f"🚨 *AI Autonomous DevOps Audit* 🚨\n"
                f"• *Alert:* `{alert_name}`\n"
                f"• *Namespace:* `{namespace}`\n\n"
                f"*🤖 AI Root Cause & Decision:*\n{diagnosis}\n\n"
                f"*🔧 Autonomous Action:*\n`{action_taken}`"
    }
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=10)
    except Exception as e:
        logging.error(f"Error sending Slack notification: {str(e)}")

def get_optimized_pod_logs(namespace="default"):
    """سحب اللوجات المركزة والمختصرة لتوفير التوكنز بأقصى درجة"""
    try:
        pods = v1.list_namespaced_pod(namespace=namespace)
        logs_summary = ""
        for pod in pods.items:
            pod_name = pod.metadata.name
            phase = pod.status.phase
            
            containers_to_check = [c.name for c in pod.spec.containers] if pod.spec.containers else []
            for container_name in containers_to_check:
                try:
                    # تقليل الأسطر إلى 10 أسطر أيرة فقط لتوفير التوكنز
                    pod_logs = v1.read_namespaced_pod_log(
                        name=pod_name, 
                        namespace=namespace, 
                        container=container_name, 
                        tail_lines=10
                    )
                    if pod_logs.strip():
                        logs_summary += f"\n[{pod_name}/{container_name}|{phase}]: {pod_logs[-300:]}"
                except Exception:
                    pass
                        
        return logs_summary if logs_summary else "No logs captured."
    except Exception as e:
        return f"Error fetching logs: {str(e)}"

def ask_gemini_for_structured_decision(alert_name, logs):
    """جعل جيمناي يحلل ويرجع JSON دقيق يحدد الأكشن والسبب"""
    prompt = f"""
    You are an expert AI DevOps Engineer. An alert '{alert_name}' fired in Kubernetes.
    Logs:
    {logs}

    Analyze the root cause and decide the best remediation action. 
    You MUST return a valid JSON object ONLY with this exact structure, no markdown formatting outside JSON if possible, just pure JSON:
    {{
      "root_cause": "short technical reason of failure",
      "action": "restart",
      "target_deployment": "extracted-deployment-name-or-default-app"
    }}
    Allowed actions are strictly: 'restart', 'none'.
    """
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            response = gemini_model.generate_content(
                prompt,
                request_options={"timeout": 10}
            )
            if response and response.text:
                text = response.text.strip()
                # تنظيف الـ Markdown code blocks لو جيميناي حطهم
                if text.startswith("```json"):
                    text = text[7:]
                if text.endswith("```"):
                    text = text[:-3]
                
                decision_json = json.loads(text.strip())
                return decision_json
        except Exception as e:
            logging.warning(f"Gemini JSON attempt {attempt + 1} failed: {str(e)}")
            time.sleep(1)
            
    # Fallback JSON ذكي وآمن لو حصلت مشكلة في الاتصال أو الـ Quota
    logging.warning("⚠️ Gemini unavailable/Quota exceeded. Using Safe Fallback JSON Decision.")
    return {
        "root_cause": "Smart Fallback: API quota exceeded or timeout. Assuming transient pod crash.",
        "action": "restart",
        "target_deployment": "demo-app"
    }

def execute_autonomous_action(decision, namespace):
    """تنفيذ القرار الوارد من الذكاء الاصطناعي مع وجود Guardrails صارمة"""
    action = decision.get("action")
    deployment_name = decision.get("target_deployment", "demo-app")
    root_cause = decision.get("root_cause", "Unknown")
    
    logging.info(f"🧠 AI Decision Received -> Action: '{action}' | Target: '{deployment_name}' | Cause: {root_cause}")
    
    try:
        # حارس الأمان (Guardrail): السماح فقط بالأكشنز الآمنة المعتمدة
        if action == "restart":
            body = {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kubectl.kubernetes.io/restartedAt": datetime.now(timezone.utc).isoformat()
                            }
                        }
                    }
                }
            }
            apps_v1.patch_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=body
            )
            msg = f"Successfully executed AI-driven Rolling Restart for deployment '{deployment_name}'."
            logging.info(f"✅ {msg}")
            return msg
        else:
            msg = f"AI decided no action needed or action '{action}' is restricted."
            logging.info(f"🛡️ {msg}")
            return msg
            
    except Exception as e:
        err_msg = f"Failed to execute autonomous action on '{deployment_name}': {str(e)}"
        logging.error(f"❌ {err_msg}")
        return err_msg

@app.route('/webhook', methods=['POST'])
def receive_alert():
    try:
        data = request.json
        logging.info("🚨 Received Alert! Requesting Autonomous AI Decision...")
        
        for alert in data.get('alerts', []):
            alert_name = alert.get('labels', {}).get('alertname')
            status = alert.get('status')
            namespace = alert.get('labels', {}).get('namespace', 'default')
            
            if status == "firing":
                # 1. سحب اللوجات المختصرة
                logs = get_optimized_pod_logs(namespace)
                
                # 2. أخذ القرار المهيكل من الذكاء الاصطناعي (JSON)
                ai_decision = ask_gemini_for_structured_decision(alert_name, logs)
                
                # 3. تنفيذ القرار بناءً على الـ AI Decision مع حماية الـ Guardrails
                remediation_result = execute_autonomous_action(ai_decision, namespace)
                
                # 4. إرسال الأوديت لـ Slack
                diagnosis_summary = f"• *Cause:* {ai_decision.get('root_cause')}\n• *Suggested Action:* `{ai_decision.get('action')}`"
                send_slack_notification(alert_name, namespace, diagnosis_summary, remediation_result)
        
        return jsonify({"status": "success", "message": "Autonomous workflow executed successfully"}), 200

    except Exception as e:
        logging.error(f"Error in webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
