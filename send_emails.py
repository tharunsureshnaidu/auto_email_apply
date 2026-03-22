import json
import smtplib
import time
import os
import re
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

# ================= CONFIG =================
EMAIL = os.getenv("EMAIL")
APP_PASSWORD = os.getenv("APP_PASSWORD")  # Replace with your actual 16-character Gmail App Password

START_ID = 29
END_ID = 200         # Range of IDs to process
MAX_PER_DAY = 200    # Max emails to send per run
DELAY_SECONDS = 30   # Delay to avoid Gmail spam filters / rate limits

INPUT_FILE = "emails.json"
RESUME_FILE = "Tharun J S_quant_dev.pdf"  # Place your resume in the same folder with this exact name
DRIVE_LINK = "https://docs.google.com/document/d/1zX57qUb2vQTt0oWPfxwQvHTNtlF3NybCHo4cmkzrpLI/edit?tab=t.0"    # Put your drive link here
# ==========================================


# --------- Helpers ---------

def clean_email_string(email):
    """Removes common web-scraping artifacts from the email string."""
    return email.replace("u003e", "").replace("u003c", "").replace("%20", "").strip()


def is_valid_email(email):
    email = email.lower()
    if "@" not in email:
        return False

    # Strictly validate format (drops weird trailing xxx, or .If, or huge hashes)
    if not re.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$", email):
        return False

    # Avoid huge hash-like local parts from crash reporters etc.
    local_part = email.split("@")[0]
    if len(local_part) > 25 and re.match(r"^[a-f0-9]+$", local_part):
        return False

    # Skip generic extensions and obvious test domains
    blacklist_extensions = [
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".pdf", 
        "sentry.io", "example.com", "domain.com", "mail.com", "company.com"
    ]
    if any(email.endswith(ext) for ext in blacklist_extensions):
        return False

    return True


def extract_company(email):
    try:
        domain = email.split("@")[1].lower()
        
        # Ignore common generic domains
        generic = ["gmail", "yahoo", "hotmail", "outlook", "protonmail", "icloud", "yandex", "mail"]
        if any(g in domain for g in generic):
            return "your company"
            
        # Proper casing for well-known quant/tech/banks
        known_companies = {
            "janestreet": "Jane Street",
            "deshaw": "D. E. Shaw",
            "twosigma": "Two Sigma",
            "jumptrading": "Jump Trading",
            "tower-research": "Tower Research",
            "akunacapital": "Akuna Capital",
            "drwholdings": "DRW",
            "drw": "DRW",
            "hudson-trading": "Hudson River Trading",
            "point72": "Point72",
            "worldquant": "WorldQuant",
            "optiver": "Optiver",
            "citadelsecurities": "Citadel Securities",
            "citadel": "Citadel",
            "gs.com": "Goldman Sachs",
            "goldmansachs": "Goldman Sachs",
            "db.com": "Deutsche Bank",
            "jpmorgan": "JPMorgan Chase",
            "jpmchase": "JPMorgan Chase",
            "morganstanley": "Morgan Stanley",
            "barclays": "Barclays",
            "man.com": "Man Group",
            "mangroup": "Man Group",
            "bwater": "Bridgewater Associates",
            "rentec": "Renaissance Technologies",
            "aqr": "AQR Capital",
            "virtu": "Virtu Financial",
            "robinhood": "Robinhood",
            "coinbase": "Coinbase",
            "systematica": "Systematica Investments",
            "belvederetrading": "Belvedere Trading",
            "pdtpartners": "PDT Partners",
            "gresearch": "G-Research",
            "deepmind": "Google DeepMind",
            "flowtraders": "Flow Traders",
            "fiverings": "Five Rings"
        }
        
        for key, proper_name in known_companies.items():
            if key in domain:
                return proper_name
                
        # Fallback: Just take the highest level subdomain
        domain_name = domain.split(".")[-2] if len(domain.split(".")) > 1 else domain.split(".")[0]
        return domain_name.capitalize()
    except Exception:
        return "your company"


def extract_name(email):
    # Extracts proper names avoiding numbers and generic terms
    local_part = email.split("@")[0]
    
    # Reject names with numbers entirely (e.g. john123)
    if bool(re.search(r'\d', local_part)):
        return None
        
    # Split by dot, underscore, or hyphen
    parts = re.split(r'[._-]', local_part)
    
    # If 2 or 3 parts (First Last), capitalize them
    if 1 < len(parts) <= 3:
        if all(p.isalpha() for p in parts if p):
            return " ".join(p.capitalize() for p in parts if p)
            
    # If it's a single word without digits, optionally use as name if it's long enough
    if len(parts) == 1 and local_part.isalpha() and len(local_part) > 2:
        return local_part.capitalize()
        
    return None


def generate_message(email):
    company = extract_company(email)
    person_name = extract_name(email)
    
    # Customize the greeting
    if person_name and company != "your company":
        # Expanded list of non-name terms commonly found in the emails list
        invalid_names = [
            "hr", "info", "admin", "recruiting", "contact", "support", "careers",
            "team", "sales", "jobs", "hiring", "hello", "press", "marketing", 
            "security", "privacy", "legal", "investor", "investors", "help", "office", 
            "enquiries", "media", "compliance", "recruit", "talent", "campus", 
            "communications", "student", "alumni", "relations", "partnership", 
            "ventures", "data", "ops", "desk", "user", "name", "firstname", 
            "test", "complaints", "fraud", "academy", "puzzles", "sponsorship",
            "inquiries", "social", "events"
        ]
        
        # Check if the extracted name contains any of the invalid words
        if not any(invalid in person_name.lower() for invalid in invalid_names):
            greeting = f"Hi {person_name},"
        else:
            greeting = f"Hi {company} Team,"
    elif company != "your company":
        greeting = f"Hi {company} Team,"
    else:
        greeting = "Hi Hiring Team,"
        
    company_mention = f"at {company}" if company != "your company" else "with your team"

    body = f"""{greeting}

I am reaching out to express my interest in Quantitative Trading, Quantitative Research, or Low-Latency Software Engineering roles {company_mention}.

I am a Computer Science graduate (CGPA: 8.7/10) with professional experience in low-latency systems, quantitative modeling, and backend engineering.

In my recent projects and roles, I have:
• Built a low-latency trading engine in Rust (RustForge Terminal) with real-time data ingestion and multi-agent market simulations.
• Developed a Quantitative Trading & Backtesting System in Python for pairs trading and mean-reversion strategies, utilizing cointegration and limit order book simulations.
• Implemented an Options Pricing & Risk Modeling Engine implementing Black-Scholes, Monte Carlo simulations, and Greeks.
• Designed scalable microservices and built a sub-200ms retrieval engine directly impacting performance during my time as a Software Development Engineer at TripFactory.

I have attached my resume to this email for your review. You can also view my portfolio and additional documents here: {DRIVE_LINK}

I would love the opportunity to learn more about the work being done {company_mention} and discuss how my background in high-performance systems and quantitative finance could be a great fit.

Looking forward to hearing from you.

Best regards,
Tharun J S
Bengaluru, Karnataka
9019842811 | tharunjs012003@gmail.com
LinkedIn: https://www.linkedin.com/in/tharun-js-b52574361/ | GitHub: https://github.com/tharunsureshnaidu
"""
    return body.strip()


def send_email(to_email):
    msg = EmailMessage()
    msg["Subject"] = "Application for Quantitative / Low-Latency Engineering Roles - Tharun J S"
    msg["From"] = EMAIL
    msg["To"] = to_email

    msg.set_content(generate_message(to_email))

    # Attach Resume PDF
    if os.path.exists(RESUME_FILE):
        with open(RESUME_FILE, 'rb') as f:
            pdf_data = f.read()
        msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=RESUME_FILE)
    else:
        print(f"   [WARNING] {RESUME_FILE} not found. Sending without attachment!")

    # Connect to SMTP server and send email
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL, APP_PASSWORD)
        smtp.send_message(msg)


# --------- Main ---------

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please run the converter script first.")
        return

    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    sent_count = 0

    print(f"Starting email blitz from ID {START_ID} to {END_ID}...")

    for entry in data:
        id_ = entry.get("id", 0)
        raw_email = entry.get("email", "")
        
        # Clean email artifacts straight from the source
        email = clean_email_string(raw_email)

        if id_ < START_ID or id_ > END_ID:
            continue

        if sent_count >= MAX_PER_DAY:
            print("\nReached daily limit of", MAX_PER_DAY, "emails.")
            break

        if not is_valid_email(email):
            print(f"[{id_}] Skipping invalid email: {email}")
            continue

        try:
            print(f"[{id_}] Sending to {email}...", end=" ", flush=True)
            send_email(email)
            sent_count += 1
            print("Success!")
            
            # Wait between sends to dodge spam filters
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            print(f"Failed. Error: {e}")

    print(f"\nDone. Successfully sent {sent_count} emails.")


if __name__ == "__main__":
    main()
