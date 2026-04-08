"""
Failure notifier for smk-dash.

Sends one email per failed rule, fired from the UI refresh tick.
Never raises — a mail failure is logged to the dashboard but never
crashes the TUI.

Delivery chain (tried in order):
  1. SMTP relay at smtp_host:smtp_port  (default: smtp.ox.ac.uk:25,
     reachable unauthenticated from Oxford HPC login nodes)
  2. localhost:25                        (sendmail / postfix MTA fallback)
  3. `mail` binary in $PATH             (last resort, most HPC nodes have it)
  4. Warn in dashboard log and give up  (silent failure — never crash)
"""
from __future__ import annotations

import shutil
import smtplib
import socket
import subprocess
from datetime import datetime
from email.message import EmailMessage
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import WorkflowState


# ── public class ──────────────────────────────────────────────────────────────

class FailureNotifier:
    """
    Call check_and_notify(state) on every UI refresh tick.
    Fires exactly one email per failed rule name, never repeats.
    """

    def __init__(
        self,
        recipient: str,
        workflow_name: str,
        log_path: str | None = None,
        smtp_host: str = "smtp.ox.ac.uk",
        smtp_port: int = 25,
    ) -> None:
        self.recipient     = recipient
        self.workflow_name = workflow_name
        self.log_path      = log_path
        self.smtp_host     = smtp_host
        self.smtp_port     = smtp_port

        # Rules we've already emailed — never alert twice for the same rule
        self._alerted_rules: set[str] = set()
        # Accumulates delivery status messages for the dashboard log
        self.delivery_log: list[str] = []

    def check_and_notify(self, state: "WorkflowState") -> None:
        """
        Inspect state for newly-failed rules and fire emails.
        Safe to call every 1–2 seconds; does network I/O only when a new
        failure is detected.
        """
        for rule_name, rule in state.rules.items():
            if rule.failed > 0 and rule_name not in self._alerted_rules:
                self._alerted_rules.add(rule_name)
                self._send(rule_name, rule, state)

    # ── internals ─────────────────────────────────────────────────────────────

    def _send(self, rule_name: str, rule, state: "WorkflowState") -> None:
        msg = self._build_message(rule_name, rule, state)
        error = self._try_smtp(msg, self.smtp_host, self.smtp_port)
        if error and self.smtp_host != "localhost":
            error = self._try_smtp(msg, "localhost", 25)
        if error:
            error = self._try_mail_binary(rule_name, msg)
        if error:
            self._warn(f"[smk-dash] Could not send failure email for rule '{rule_name}': {error}")
        else:
            self._warn(f"[smk-dash] Failure email sent → {self.recipient} (rule: {rule_name})")

    def _build_message(self, rule_name: str, rule, state: "WorkflowState") -> EmailMessage:
        hostname = socket.gethostname()
        now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Find Slurm IDs associated with this failed rule
        failed_slurm_ids = [
            sid for sid, job in state.slurm_jobs.items()
            if job.rule_name == rule_name and job.state == "FAILED"
        ]
        slurm_str = ", ".join(failed_slurm_ids) if failed_slurm_ids else "unknown"

        body = f"""\
smk-dash failure alert
══════════════════════════════════════════════════
Workflow  : {self.workflow_name}
Host      : {hostname}
Time      : {now}
Elapsed   : {state.elapsed_str}

Failed rule : {rule_name}
  Jobs failed   : {rule.failed}
  Jobs done     : {rule.done}
  Slurm IDs     : {slurm_str}

Progress at time of failure
  Done    : {state.total_done}
  Running : {state.total_running}
  Failed  : {state.total_failed}
  Total   : {state.total_expected or '?'}

Log file  : {self.log_path or 'unknown'}
══════════════════════════════════════════════════
Sent by smk-dash (BMRC / KIR Research Computing)
"""
        sender = f"smk-dash@{hostname}"
        msg = EmailMessage()
        msg["Subject"] = f"[smk-dash] FAILED rule '{rule_name}' — {self.workflow_name}"
        msg["From"]    = sender
        msg["To"]      = self.recipient
        msg.set_content(body)
        return msg

    def _try_smtp(self, msg: EmailMessage, host: str, port: int) -> str | None:
        """Return None on success, error string on failure."""
        try:
            with smtplib.SMTP(host, port, timeout=10) as smtp:
                smtp.send_message(msg)
            return None
        except Exception as exc:
            return str(exc)

    def _try_mail_binary(self, rule_name: str, msg: EmailMessage) -> str | None:
        """Fallback: pipe to the `mail` binary."""
        mail_bin = shutil.which("mail") or shutil.which("sendmail")
        if not mail_bin:
            return "no mail/sendmail binary found"
        try:
            subject = msg["Subject"]
            body    = msg.get_content()
            proc = subprocess.run(
                [mail_bin, "-s", subject, self.recipient],
                input=body,
                text=True,
                capture_output=True,
                timeout=15,
            )
            if proc.returncode != 0:
                return proc.stderr.strip() or f"exit code {proc.returncode}"
            return None
        except Exception as exc:
            return str(exc)

    def _warn(self, message: str) -> None:
        self.delivery_log.append(message)
