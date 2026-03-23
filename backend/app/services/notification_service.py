from __future__ import annotations

import json
import logging
import smtplib
from email.message import EmailMessage
from urllib import request

from app.core.config import Settings

logger = logging.getLogger(__name__)


class NotificationService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def notify(self, title: str, message: str, details: dict | None = None) -> None:
        payload = {"title": title, "message": message, "details": details or {}}
        self._send_webhook(payload)
        self._send_email(payload)

    def _send_webhook(self, payload: dict) -> None:
        if not self.settings.alert_webhook_url:
            return
        webhook_kind = (self.settings.alert_webhook_kind or "generic").lower().strip()
        if webhook_kind == "slack":
            body = {"text": f"*{payload['title']}*\n{payload['message']}"}
            if payload.get("details"):
                body["text"] += f"\n```{json.dumps(payload['details'], ensure_ascii=True)}```"
        elif webhook_kind == "teams":
            body = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": payload["title"],
                "themeColor": "0076D7",
                "title": payload["title"],
                "text": payload["message"],
                "sections": [{"facts": [{"name": k, "value": str(v)} for k, v in (payload.get("details") or {}).items()]}],
            }
        else:
            body = payload

        data = json.dumps(body).encode("utf-8")
        req = request.Request(
            self.settings.alert_webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=10) as resp:
                if getattr(resp, "status", 200) >= 400:
                    logger.warning("Alert webhook returned status %s", resp.status)
        except Exception as exc:
            logger.warning("Failed to send webhook alert: %s", exc)

    def _send_email(self, payload: dict) -> None:
        recipients = self.settings.alert_email_recipients
        if not recipients or not self.settings.smtp_host:
            return
        msg = EmailMessage()
        msg["Subject"] = payload["title"]
        msg["From"] = self.settings.smtp_from or self.settings.smtp_user or "wematics@localhost"
        msg["To"] = ", ".join(recipients)
        text = payload["message"]
        if payload.get("details"):
            text += f"\n\nDetails:\n{json.dumps(payload['details'], indent=2, ensure_ascii=True)}"
        msg.set_content(text)

        try:
            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=10) as server:
                server.ehlo()
                try:
                    server.starttls()
                    server.ehlo()
                except Exception:
                    pass
                if self.settings.smtp_user and self.settings.smtp_password:
                    server.login(self.settings.smtp_user, self.settings.smtp_password)
                server.send_message(msg)
        except Exception as exc:
            logger.warning("Failed to send email alert: %s", exc)
