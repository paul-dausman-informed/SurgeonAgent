"""
Email sender for SurgeonAgent — sends the Consultation Summary PDF via Resend.

Environment variables:
  - RESEND_API_KEY:  required. Get from https://resend.com/api-keys
  - EMAIL_FROM:      sender address. Default: "onboarding@resend.dev" (Resend sandbox).
                     For production, verify a domain and use e.g. "noreply@yourdomain.com".
  - EMAIL_FROM_NAME: display name. Default: "Informed SurgeonAgent".
"""

import os
import re
import base64
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Basic-but-reasonable email regex. Not RFC-perfect, catches typos.
_EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)

DEFAULT_FROM = os.environ.get("EMAIL_FROM", "onboarding@resend.dev")
DEFAULT_FROM_NAME = os.environ.get("EMAIL_FROM_NAME", "Informed SurgeonAgent")


def validate_email(address: str) -> bool:
    """Return True if the address looks structurally valid."""
    if not address or len(address) > 254:
        return False
    return bool(_EMAIL_REGEX.match(address.strip()))


def _build_html_body(procedure_name: str, surgeon_name: str) -> str:
    """Build the branded HTML email body."""
    # Note: brand colors from the app — dark bg with cyan accents.
    return f"""\
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#F5F7FA;color:#1E1E28;">
  <div style="max-width:600px;margin:0 auto;padding:24px;">
    <div style="background:#0B0F19;padding:18px 22px;border-radius:10px 10px 0 0;">
      <h1 style="margin:0;font-size:18px;color:#FFFFFF;letter-spacing:0.02em;">
        <span style="color:#00D4FF;">Informed</span> SurgeonAgent
      </h1>
      <div style="color:#94A3B8;font-size:12px;margin-top:2px;">Consultation Summary</div>
    </div>

    <div style="background:#FFFFFF;padding:24px;border:1px solid #E5E7EB;border-top:none;border-radius:0 0 10px 10px;">
      <p style="margin:0 0 14px 0;font-size:15px;line-height:1.6;">Hi,</p>

      <p style="margin:0 0 14px 0;font-size:15px;line-height:1.6;">
        Your <strong>SurgeonAgent consultation summary</strong> is attached to this email.
        It covers the procedure we discussed
        {f'(<strong>{procedure_name}</strong>)' if procedure_name else ''},
        the recommended surgeon
        {f'(<strong>{surgeon_name}</strong>)' if surgeon_name else ''},
        a comparison of top surgeons in your area, and a worksheet of questions
        to bring to your appointment.
      </p>

      <p style="margin:0 0 14px 0;font-size:14px;line-height:1.6;color:#555;">
        This document is informational only and is not a substitute for professional
        medical advice. Please discuss any decisions about your care directly with
        your surgeon and primary care provider.
      </p>

      <hr style="border:none;border-top:1px solid #E5E7EB;margin:20px 0;">

      <p style="margin:0;font-size:12px;color:#94A3B8;">
        You are receiving this email because you requested a consultation summary
        through SurgeonAgent. We do not store your email address after sending.
      </p>
    </div>
  </div>
</body>
</html>
"""


def send_consultation_summary(
    to_email: str,
    pdf_path: str,
    procedure_name: str = "",
    surgeon_name: str = "",
) -> dict:
    """Send the consultation summary PDF as an attachment via Resend.

    Returns:
        {"success": bool, "message": str, "id": Optional[str]}
    """
    # --- Validate inputs --------------------------------------------------
    if not validate_email(to_email):
        return {"success": False, "message": f"Invalid email address: {to_email}", "id": None}

    if not os.path.isfile(pdf_path):
        return {"success": False, "message": f"PDF not found at {pdf_path}", "id": None}

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        return {
            "success": False,
            "message": "Email service is not configured (missing RESEND_API_KEY).",
            "id": None,
        }

    # --- Import resend lazily so missing dep doesn't break module load ---
    try:
        import resend  # type: ignore
    except ImportError:
        return {
            "success": False,
            "message": "Email library not installed (pip install resend).",
            "id": None,
        }

    resend.api_key = api_key

    # --- Read and encode PDF attachment ----------------------------------
    try:
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        # Resend expects a list of ints or base64 string for content
        pdf_b64 = base64.b64encode(pdf_bytes).decode("ascii")
    except Exception as e:
        return {"success": False, "message": f"Could not read PDF: {e}", "id": None}

    filename = os.path.basename(pdf_path)

    # --- Build subject ----------------------------------------------------
    subject = "Your SurgeonAgent Consultation Summary"
    if procedure_name:
        subject = f"Your Consultation Summary — {procedure_name}"

    # --- Build from header ------------------------------------------------
    from_header = f"{DEFAULT_FROM_NAME} <{DEFAULT_FROM}>"

    # --- Send -------------------------------------------------------------
    try:
        params = {
            "from": from_header,
            "to": [to_email.strip()],
            "subject": subject,
            "html": _build_html_body(procedure_name, surgeon_name),
            "attachments": [
                {
                    "filename": filename,
                    "content": pdf_b64,
                }
            ],
        }
        response = resend.Emails.send(params)
        email_id = response.get("id") if isinstance(response, dict) else None
        logger.info(f"Email sent to {to_email} (id={email_id})")
        return {
            "success": True,
            "message": f"Email sent to {to_email}",
            "id": email_id,
        }
    except Exception as e:
        logger.error(f"Resend send failure: {e}")
        return {
            "success": False,
            "message": f"Could not send email: {e}",
            "id": None,
        }


if __name__ == "__main__":
    # Quick CLI test:  python email_sender.py someone@example.com path/to.pdf
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 3:
        print("Usage: python email_sender.py <to_email> <pdf_path>")
        sys.exit(1)
    result = send_consultation_summary(
        to_email=sys.argv[1],
        pdf_path=sys.argv[2],
        procedure_name="Cholecystectomy",
        surgeon_name="Dr. John Smith",
    )
    print(result)
