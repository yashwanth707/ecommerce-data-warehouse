"""
Alerting Plugin — Slack & PagerDuty Integration
================================================
Provides callback functions and utility classes for
pipeline alerting and observability.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

import requests
from airflow.plugins_manager import AirflowPlugin


logger = logging.getLogger(__name__)


class SlackAlerter:
    """Sends formatted messages to Slack via webhook."""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    def send_alert(
        self,
        title: str,
        message: str,
        severity: str = "info",
        fields: Optional[dict] = None,
    ):
        """
        Send a formatted Slack message.

        Args:
            title: Alert title
            message: Alert body
            severity: info | warning | critical
            fields: Additional key-value pairs to display
        """
        if not self.webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured, skipping alert")
            return

        color_map = {
            "info": "#36a64f",
            "warning": "#ff9900",
            "critical": "#ff0000",
        }
        emoji_map = {
            "info": "✅",
            "warning": "⚠️",
            "critical": "🚨",
        }

        attachment_fields = []
        if fields:
            for key, value in fields.items():
                attachment_fields.append(
                    {"title": key, "value": str(value), "short": True}
                )

        payload = {
            "attachments": [
                {
                    "color": color_map.get(severity, "#36a64f"),
                    "title": f"{emoji_map.get(severity, '')} {title}",
                    "text": message,
                    "fields": attachment_fields,
                    "footer": "E-Commerce Data Pipeline",
                    "ts": int(datetime.now().timestamp()),
                }
            ]
        }

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"Slack alert sent: {title}")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")


class PagerDutyAlerter:
    """Triggers PagerDuty incidents for critical failures."""

    EVENTS_API_URL = "https://events.pagerduty.com/v2/enqueue"

    def __init__(self, service_key: Optional[str] = None):
        self.service_key = service_key or os.getenv("PAGERDUTY_SERVICE_KEY")

    def trigger_incident(
        self,
        summary: str,
        severity: str = "critical",
        source: str = "ecommerce_pipeline",
        details: Optional[dict] = None,
    ):
        """
        Trigger a PagerDuty incident.

        Args:
            summary: Incident summary
            severity: critical | error | warning | info
            source: Source of the incident
            details: Additional context
        """
        if not self.service_key:
            logger.warning(
                "PAGERDUTY_SERVICE_KEY not configured, skipping incident"
            )
            return

        payload = {
            "routing_key": self.service_key,
            "event_action": "trigger",
            "payload": {
                "summary": summary,
                "severity": severity,
                "source": source,
                "custom_details": details or {},
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        }

        try:
            response = requests.post(
                self.EVENTS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            logger.info(f"PagerDuty incident triggered: {summary}")
        except Exception as e:
            logger.error(f"Failed to trigger PagerDuty incident: {e}")


# Airflow Callback Helpers

def _get_ai_alert_summary(task_id: str, dag_id: str, exception: str) -> Optional[str]:
    """
    Call the Ollama NL API to generate a human-readable alert summary.
    Returns None if the API is unavailable (graceful fallback).
    """
    nl_api_url = os.getenv("NL_API_URL", "http://localhost:8000")
    try:
        payload = {
            "failures": [
                {
                    "task": task_id,
                    "dag": dag_id,
                    "error": exception[:300] if exception else "Unknown error",
                }
            ],
            "pipeline_run": dag_id,
        }
        resp = requests.post(
            f"{nl_api_url}/api/dq_summary",
            json=payload,
            timeout=60,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            return resp.json().get("summary", "")
    except Exception as e:
        logger.warning(f"AI alert summary unavailable (Ollama offline?): {e}")
    return None


def slack_failure_alert(context):
    """Airflow on_failure_callback for Slack — with optional AI-powered summary."""
    alerter = SlackAlerter()
    ti = context["task_instance"]
    exception_str = str(context.get("exception", ""))

    # Try to get an AI-formatted plain-English summary from Ollama
    ai_summary = _get_ai_alert_summary(ti.task_id, ti.dag_id, exception_str)

    # Use AI summary if available, else fall back to raw error
    if ai_summary:
        message = f"🤖 *AI Summary:* {ai_summary}\n\n_Raw error:_ `{exception_str[:200]}`"
    else:
        message = (
            f"Task `{ti.task_id}` in DAG `{ti.dag_id}` failed.\n"
            f"Error: `{exception_str[:300]}`"
        )

    alerter.send_alert(
        title="Pipeline Task Failed",
        message=message,
        severity="critical",
        fields={
            "Task": ti.task_id,
            "DAG": ti.dag_id,
            "Execution Date": str(context["execution_date"]),
            "Log URL": ti.log_url,
        },
    )



def pagerduty_critical_alert(context):
    """Airflow on_failure_callback for PagerDuty (Gold layer only)."""
    alerter = PagerDutyAlerter()
    ti = context["task_instance"]
    alerter.trigger_incident(
        summary=f"CRITICAL: {ti.task_id} failed in {ti.dag_id}",
        severity="critical",
        details={
            "task_id": ti.task_id,
            "dag_id": ti.dag_id,
            "execution_date": str(context["execution_date"]),
            "exception": str(context.get("exception", ""))[:500],
        },
    )


# Plugin Registration

class AlertingPlugin(AirflowPlugin):
    name = "alerting_plugin"
