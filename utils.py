import os
import json
import time
from dotenv import load_dotenv
from google import genai
from google.genai import types

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()


# ==============================
# BUILD PROMPT
# ==============================

def build_job_parser_prompt(job_description: str) -> str:
    prompt = f"""
You are an expert job data extraction AI for German-speaking job markets (DE, AT, CH).

Your task is to extract structured job information from raw job descriptions.

====================
CRITICAL OUTPUT RULES
====================
- Return ONLY valid JSON
- No explanations
- No markdown
- Follow schema EXACTLY
- Use "" or [] or 0 if missing
- Do NOT invent data
- Normalize enum values exactly as specified
- Skills must be lowercase
- Remove duplicates
- Summary max 3 sentences in German

====================
ENUM NORMALIZATION
====================

seniority_level:
intern, junior, mid, senior, lead, manager

employment_type:
full-time, part-time, contract, internship, temporary

remote_type:
on-site, hybrid, remote

company_type:
employer, recruiter

company_size:
micro, small, medium, large, enterprise

====================
WORKLOAD RULES
====================
- Extract percentage if mentioned (e.g. 80–100%)
- workload_min = lowest %
- workload_max = highest %
- If not mentioned → 0

====================
EXPERIENCE RULES
====================
- Extract min/max years if range exists
- If "mehrjährige Erfahrung" → 3 minimum
- If unclear → 0

====================
MANAGEMENT RULES
====================
management_responsibility = true if:
- Teamleitung
- Führungsverantwortung
- Leitung
- Head of
Otherwise false

home_office_possible = true if:
- Homeoffice
- Hybrid
- Remote
Otherwise false

====================
OUTPUT JSON SCHEMA
====================

{{
  "title": "",
  "summary": "",

  "company": {{
    "name": "",
    "industry": "",
    "company_type": "",
    "company_size": ""
  }},

  "category": {{
    "main_category": "",
    "sub_category": ""
  }},

  "location": {{
    "country": "",
    "state": "",
    "city": "",
    "postal_code": ""
  }},

  "seniority_level": "",
  "experience_min_years": 0,
  "experience_max_years": 0,

  "employment_type": "",
  "workload_min": 0,
  "workload_max": 0,

  "remote_type": "",
  "management_responsibility": false,
  "home_office_possible": false,

  "education_level": "",
  "languages": [],

  "required_skills": [],
  "preferred_skills": [],

  "published_at": ""
}}

====================
JOB DESCRIPTION
====================

{job_description}
"""
    return prompt


# ==============================
# GEMINI CALL
# ==============================

def generate(job_description: str) -> str:
    """
    Sends prompt to Gemini model
    """

    prompt = build_job_parser_prompt(job_description)

    client = genai.Client(
        api_key=os.getenv("GEMINI_API_KEY")
    )

    model = "gemini-2.5-flash"

    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt)
            ]
        )
    ]

    response = client.models.generate_content(
        model=model,
        contents=contents
    )

    return response.text


# ==============================
# LLM RETRY LOGIC
# ==============================

def call_llm_with_retry(job_text: str, retries: int = 3, delay: int = 5):
    """
    Call Gemini with retry logic.

    Stops scraper if:
    - quota exceeded
    - retries exhausted
    """

    for attempt in range(1, retries + 1):

        try:
            print(f"🤖 LLM call attempt {attempt}")

            response = generate(job_text)

            if response and response.strip():
                return response

        except Exception as e:

            error_message = str(e).lower()
            print(f"⚠ LLM error: {e}")

            # Stop scraper if quota exceeded
            if "quota" in error_message or "rate limit" in error_message:
                print("❌ Gemini quota exceeded. Stop scraper.")
                return None

        # Retry delay
        if attempt < retries:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

    print("❌ LLM failed after maximum retries")
    return None


# ==============================
# PARSE LLM RESPONSE
# ==============================

def parse_llm_response(llm_response: str) -> dict:
    """
    Safely parse JSON response from LLM
    """

    try:
        cleaned = llm_response.strip()

        # Remove markdown if present
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            cleaned = cleaned.rsplit("```", 1)[0]

        return json.loads(cleaned.strip())

    except json.JSONDecodeError as e:
        print(f"✗ Failed to parse LLM response: {e}")
        return {}