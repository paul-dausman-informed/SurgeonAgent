"""
Consultation Summary PDF Generator

Creates a branded PDF summarizing the patient consultation:
  - Procedure overview
  - Recommended surgeon details
  - Top 5 surgeon comparison grid
  - "Questions for the Surgeon" worksheet
"""

import os
import json
from datetime import date

from fpdf import FPDF

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOGO_PATH = os.path.join(BASE_DIR, "static", "logo.png")
QUESTIONS_PATH = os.path.join(BASE_DIR, "rules", "Surgeon Questions.md")

# If the white logo doesn't exist, try the images directory
if not os.path.exists(LOGO_PATH):
    alt = os.path.join(BASE_DIR, "images", "INFORM_logo_White_01.png")
    if os.path.exists(alt):
        LOGO_PATH = alt
    else:
        LOGO_PATH = None

# Brand colors (RGB tuples)
DARK_BG = (11, 15, 25)         # #0B0F19 — header background
CYAN = (0, 212, 255)            # #00D4FF — primary accent
MAGENTA = (224, 64, 251)        # #E040FB — secondary accent
WHITE = (255, 255, 255)
LIGHT_GRAY = (245, 247, 250)    # row alternating
MID_GRAY = (148, 163, 184)      # #94A3B8 — muted text
DARK_TEXT = (30, 30, 40)         # near-black body text
SECTION_BG = (17, 24, 39)       # #111827 — section header bg


def _load_questions() -> list[dict]:
    """Load questions from the Surgeon Questions markdown file.

    Returns a list of {"question": str, "hint": str} dicts.
    """
    if not os.path.exists(QUESTIONS_PATH):
        return []

    questions = []
    with open(QUESTIONS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("##"):
                continue
            # Lines are formatted as "Question text? Optional hint."
            # Split on first "?" to get question and hint
            if "?" in line:
                parts = line.split("?", 1)
                q = parts[0].strip() + "?"
                hint = parts[1].strip() if len(parts) > 1 else ""
            else:
                q = line
                hint = ""
            questions.append({"question": q, "hint": hint})
    return questions


class ConsultationPDF(FPDF):
    """Custom PDF class with Informed branding."""

    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="letter")
        self.set_auto_page_break(auto=True, margin=20)
        self._register_fonts()

    def _register_fonts(self):
        """Use built-in Helvetica (no external font files needed)."""
        pass  # FPDF includes Helvetica by default

    def header(self):
        """Branded header with dark background and logo."""
        # Dark header bar
        self.set_fill_color(*DARK_BG)
        self.rect(0, 0, self.w, 22, "F")

        # Logo
        if LOGO_PATH and os.path.exists(LOGO_PATH):
            try:
                self.image(LOGO_PATH, x=8, y=4, h=14)
            except Exception:
                pass

        # "Consultation Summary" text on the right
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*WHITE)
        self.set_xy(self.w - 70, 7)
        self.cell(62, 8, "Consultation Summary", align="R")

        self.ln(24)

    def footer(self):
        """Page footer with branding."""
        self.set_y(-15)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*MID_GRAY)
        self.cell(0, 10, f"Informed  |  Page {self.page_no()}/{{nb}}  |  {date.today().strftime('%B %d, %Y')}", align="C")

    def _section_header(self, title: str):
        """Draw a branded section header with cyan accent."""
        self.ln(4)
        # Cyan accent bar
        self.set_fill_color(*CYAN)
        self.rect(self.l_margin, self.get_y(), 3, 8, "F")
        # Title
        self.set_x(self.l_margin + 6)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*DARK_BG)
        self.cell(0, 8, title)
        self.ln(10)
        # Thin line under header
        self.set_draw_color(*CYAN)
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def _body_text(self, text: str):
        """Write body text paragraph."""
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*DARK_TEXT)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def _label_value(self, label: str, value: str, bold_value: bool = False):
        """Write a label: value line."""
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(*MID_GRAY)
        x_start = self.get_x()
        self.cell(45, 6, f"{label}:", align="L")
        style = "B" if bold_value else ""
        self.set_font("Helvetica", style, 10)
        self.set_text_color(*DARK_TEXT)
        self.cell(0, 6, value)
        self.ln(7)


def generate_consultation_summary(data: dict, output_dir: str = "") -> str:
    """Generate a branded consultation summary PDF.

    Args:
        data: dict with keys:
            - procedure_name: str
            - procedure_description: str
            - recommended_surgeon: dict with name, npi, credential, specialty,
              informed_score, cases, complication_free_rate, avg_90_day_cost,
              facilities, city, state, medical_school, davinci_status
            - top_surgeons: list of dicts (same shape, up to 5)
            - patient_city: str
            - patient_state: str
        output_dir: directory to save the PDF (default: output/)

    Returns:
        Absolute path to the generated PDF file.
    """
    if not output_dir:
        output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)

    pdf = ConsultationPDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    # ------------------------------------------------------------------
    # Date line
    # ------------------------------------------------------------------
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*MID_GRAY)
    pdf.cell(0, 6, f"Generated {date.today().strftime('%B %d, %Y')}", align="R")
    pdf.ln(6)

    # ------------------------------------------------------------------
    # Section 1: Procedure Overview
    # ------------------------------------------------------------------
    procedure_name = data.get("procedure_name", "Surgical Procedure")
    procedure_desc = data.get("procedure_description", "")

    pdf._section_header("Procedure Overview")
    pdf.set_font("Helvetica", "B", 12)
    pdf.set_text_color(*DARK_BG)
    pdf.cell(0, 7, procedure_name)
    pdf.ln(9)

    if procedure_desc:
        pdf._body_text(procedure_desc)

    # Patient location
    patient_city = data.get("patient_city", "")
    patient_state = data.get("patient_state", "")
    if patient_city:
        location = f"{patient_city}, {patient_state}" if patient_state else patient_city
        pdf._label_value("Search Location", location)
    pdf.ln(2)

    # ------------------------------------------------------------------
    # Section 2: Recommended Surgeon
    # ------------------------------------------------------------------
    surgeon = data.get("recommended_surgeon", {})
    if surgeon:
        pdf._section_header("Recommended Surgeon")

        name = surgeon.get("name", surgeon.get("full_name", ""))
        credential = surgeon.get("credential", "")
        display_name = f"{name}, {credential}" if credential else name

        pdf.set_font("Helvetica", "B", 14)
        pdf.set_text_color(*DARK_BG)
        pdf.cell(0, 8, display_name)
        pdf.ln(9)

        # Key metrics in two columns
        metrics = [
            ("Specialty", surgeon.get("specialty", "")),
            ("Informed Score", str(surgeon.get("informed_score", ""))),
            ("Cases Performed", str(surgeon.get("cases", ""))),
            ("Complication-Free Rate", surgeon.get("complication_free_rate", "")),
            ("Avg 90-Day Cost", surgeon.get("avg_90_day_cost", "")),
            ("City", f"{surgeon.get('city', '')}, {surgeon.get('state', '')}"),
            ("Medical School", surgeon.get("medical_school", "")),
        ]

        for label, value in metrics:
            if value and value not in ("", ","):
                pdf._label_value(label, value, bold_value=(label == "Informed Score"))

        # Facilities
        facilities = surgeon.get("facilities", [])
        if facilities:
            if isinstance(facilities, list):
                fac_str = ", ".join(facilities) if isinstance(facilities[0], str) else ", ".join(f.get("name", str(f)) for f in facilities)
            else:
                fac_str = str(facilities)
            pdf._label_value("Hospital(s)", fac_str)

        # Da Vinci status
        davinci = surgeon.get("davinci_status", {})
        if isinstance(davinci, dict) and davinci.get("listed"):
            pdf.ln(2)
            # Highlight box
            y_start = pdf.get_y()
            pdf.set_fill_color(0, 212, 255, )  # cyan tint
            pdf.set_fill_color(230, 248, 255)   # very light cyan
            pdf.rect(pdf.l_margin, y_start, pdf.w - pdf.l_margin - pdf.r_margin, 12, "F")
            pdf.set_draw_color(*CYAN)
            pdf.rect(pdf.l_margin, y_start, pdf.w - pdf.l_margin - pdf.r_margin, 12, "D")

            pdf.set_xy(pdf.l_margin + 3, y_start + 2)
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*CYAN)
            pdf.cell(0, 8, "Robotic-Assisted Surgery Certified", align="L")
            pdf.ln(14)

        pdf.ln(2)

    # ------------------------------------------------------------------
    # Section 3: Top Surgeons Comparison Grid
    # ------------------------------------------------------------------
    top_surgeons = data.get("top_surgeons", [])
    if top_surgeons:
        pdf._section_header("Top Surgeons Considered")

        # Table header
        col_widths = [52, 22, 18, 28, 28, 48]  # name, score, cases, CF%, cost, facility
        headers = ["Surgeon", "Score", "Cases", "CF Rate", "90-Day Cost", "Facility"]

        # Header row
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_fill_color(*DARK_BG)
        pdf.set_text_color(*WHITE)
        for i, h in enumerate(headers):
            pdf.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        pdf.ln()

        # Data rows
        for idx, s in enumerate(top_surgeons[:5]):
            if idx % 2 == 1:
                pdf.set_fill_color(*LIGHT_GRAY)
                fill = True
            else:
                pdf.set_fill_color(*WHITE)
                fill = True

            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(*DARK_TEXT)

            name = s.get("name", "")
            score = str(s.get("informed_score", ""))
            cases = str(s.get("cases", ""))
            cf = s.get("complication_free_rate", "")
            cost = s.get("avg_90_day_cost", "")
            facs = s.get("facilities", [])
            if isinstance(facs, list):
                fac = facs[0] if facs else ""
                if isinstance(fac, dict):
                    fac = fac.get("name", "")
            else:
                fac = str(facs)
            # Truncate long facility names
            if len(fac) > 28:
                fac = fac[:26] + ".."

            pdf.cell(col_widths[0], 6, name[:30], border=1, fill=fill)
            pdf.cell(col_widths[1], 6, score, border=1, fill=fill, align="C")
            pdf.cell(col_widths[2], 6, cases, border=1, fill=fill, align="C")
            pdf.cell(col_widths[3], 6, cf, border=1, fill=fill, align="C")
            pdf.cell(col_widths[4], 6, cost, border=1, fill=fill, align="C")
            pdf.cell(col_widths[5], 6, fac, border=1, fill=fill)
            pdf.ln()

        pdf.ln(4)

    # ------------------------------------------------------------------
    # Section 4: Questions for the Surgeon
    # ------------------------------------------------------------------
    questions = _load_questions()
    if questions:
        # Force new page if not enough space
        if pdf.get_y() > 200:
            pdf.add_page()

        pdf._section_header("Questions for Your Surgeon")

        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*MID_GRAY)
        pdf.multi_cell(0, 5, "Use this worksheet to prepare for your consultation. "
                       "Write down your answers and any additional questions below.")
        pdf.ln(4)

        for i, q in enumerate(questions, 1):
            question = q["question"]
            hint = q.get("hint", "")

            # Question text
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*DARK_BG)
            pdf.cell(0, 6, f"{i}. {question}")
            pdf.ln(7)

            # Hint text (if any)
            if hint:
                pdf.set_font("Helvetica", "I", 9)
                pdf.set_text_color(*MID_GRAY)
                pdf.cell(0, 5, f"   {hint}")
                pdf.ln(6)

            # Blank lines for writing
            pdf.set_draw_color(200, 200, 210)
            pdf.set_line_width(0.2)
            for _ in range(3):
                y = pdf.get_y()
                pdf.line(pdf.l_margin + 5, y, pdf.w - pdf.r_margin, y)
                pdf.ln(7)

            pdf.ln(3)

            # Check if we need a new page
            if pdf.get_y() > 250:
                pdf.add_page()

    # ------------------------------------------------------------------
    # Notes section at the end
    # ------------------------------------------------------------------
    if pdf.get_y() > 230:
        pdf.add_page()

    pdf._section_header("Additional Notes")
    pdf.set_draw_color(200, 200, 210)
    pdf.set_line_width(0.2)
    for _ in range(8):
        y = pdf.get_y()
        pdf.line(pdf.l_margin + 5, y, pdf.w - pdf.r_margin, y)
        pdf.ln(8)

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------
    # Build filename
    surgeon_name = data.get("recommended_surgeon", {}).get("name", "consultation")
    safe_name = "".join(c if c.isalnum() or c in " -_" else "" for c in surgeon_name).strip().replace(" ", "_")
    filename = f"Consultation_Summary_{safe_name}.pdf"
    filepath = os.path.join(output_dir, filename)

    pdf.output(filepath)
    return filepath


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sample = {
        "procedure_name": "Cholecystectomy (Gallbladder Removal)",
        "procedure_description": (
            "A cholecystectomy is a surgical procedure to remove the gallbladder, "
            "a small organ beneath the liver that stores bile. It is most commonly "
            "performed to treat gallstones that cause pain or inflammation. The "
            "procedure can be performed using traditional open surgery, laparoscopic "
            "techniques, or robotic-assisted surgery."
        ),
        "patient_city": "Dallas",
        "patient_state": "TX",
        "recommended_surgeon": {
            "name": "John Smith",
            "credential": "M.D.",
            "specialty": "General Surgery",
            "informed_score": 95,
            "cases": 506,
            "complication_free_rate": "98.2%",
            "avg_90_day_cost": "$12,450",
            "city": "Dallas",
            "state": "TX",
            "medical_school": "University Of Texas Southwestern",
            "facilities": ["Baylor University Medical Center", "Texas Health Dallas"],
            "davinci_status": {"listed": True, "details": "Robotic-assisted certified"},
        },
        "top_surgeons": [
            {"name": "John Smith", "informed_score": 95, "cases": 506, "complication_free_rate": "98.2%", "avg_90_day_cost": "$12,450", "facilities": ["Baylor University Medical Center"]},
            {"name": "Jane Doe", "informed_score": 93, "cases": 412, "complication_free_rate": "97.8%", "avg_90_day_cost": "$11,200", "facilities": ["UT Southwestern"]},
            {"name": "Bob Wilson", "informed_score": 91, "cases": 289, "complication_free_rate": "96.5%", "avg_90_day_cost": "$13,100", "facilities": ["Methodist Dallas"]},
        ],
    }

    path = generate_consultation_summary(sample)
    print(f"Generated: {path}")
