import streamlit as st
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="BPO Keyword Scraper",
    page_icon="🤖",
    layout="centered",
)

# --- CONFIG & CONSTANTS ---
KEYWORDS = [
    'bpo', 'business process outsourcing', 'customer', 'CX', 'support', 'CSAT',
    'Chief Customer', 'Head of Customer', 'VP of Customer', 'Director of Support',
    'Chief Customer Officer', 'VP of Support & Service Operations', 'Chief Customer Officer (CCO)',
    'Chief Experience Officer (CXO)', 'Vice President (VP) of Customer Support', 'VP of Customer Service',
    'VP of Customer Experience', 'Head of Customer Support', 'Head of Customer Service',
    'Head of Customer Experience', 'Director of Customer Support', 'Director of Customer Service',
    'Director of Customer Experience', 'Director of Support Operations', 'Director of Customer Care',
    'Head of Support Operations', 'VP of Customer Care', 'Director of Client Services',
    'Director of Support Strategy', 'Director of Procurement', 'Head of Procurement',
    'Vice President (VP) of Procurement', 'Chief Procurement Officer (CPO)', 'Senior Procurement Manager',
    'Director of Sourcing', 'Head of Strategic Sourcing', 'VP of Strategic Sourcing',
    'Director of Purchasing', 'Head of Global Sourcing', 'VP of Purchasing',
    'Director of Vendor Management', 'Head of Vendor Management', 'VP of Vendor Management',
    'Chief Vendor Management Officer', 'Director of Supplier Management',
    'Head of Supplier Relationship Management', 'VP of Supplier Management', 'Senior Vendor Relationship Manager',
    'Director of Partner Management', 'CFO (Chief Financial Officer)',
    'VP of Financial Planning & Analysis (FP&A)', 'VP of Corporate Finance', 'Head of Finance',
    'Head of FP&A', 'Head of Corporate Finance', 'Director of Finance',
    'Director of Financial Planning & Analysis', 'Director of Corporate Finance',
    'Controller / Financial Controller'
]
MAX_BYTES = 300_000  # Only fetch first ~300KB of the page for speed

# --- CORE FUNCTIONS ---

@st.cache_resource(ttl=3600) # Cache the connection for an hour
def authenticate_google_sheets():
    """Authenticates with Google Sheets using Streamlit's Secrets."""
    try:
        # Get the credentials from Streamlit's secrets
        creds_json = st.secrets["gcp_service_account"]
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(
            creds_json,
            scopes=scopes
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google Sheets authentication failed. Have you added your service account credentials to the Streamlit Secrets? Error: {e}")
        return None

def clean_domain(domain):
    """Ensure proper domain format and avoid double https:// errors."""
    domain = domain.strip()
    if not domain:
        return None
    domain = domain.replace("https://https://", "https://")
    domain = domain.replace("http://https://", "https://")

    if domain.startswith("http://") or domain.startswith("https://"):
        return domain
    else:
        return f"https://{domain}"

def scrape_website(domain):
    """Scrapes a single website for keywords."""
    url = clean_domain(domain)
    if not url:
        return "Empty Domain"
    try:
        with requests.Session() as s:
            with s.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}, stream=True) as r:
                r.raise_for_status()
                html = b""
                for chunk in r.iter_content(chunk_size=8192):
                    html += chunk
                    if len(html) > MAX_BYTES:
                        break

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True).lower()
        found_keywords = sorted(list(set([kw for kw in KEYWORDS if kw.lower() in text])))
        if found_keywords:
            return "YES: " + ", ".join(found_keywords)
        else:
            return "NO"
    except requests.exceptions.RequestException as e:
        return f"Error: Connection failed ({type(e).__name__})"
    except Exception as e:
        return f"Error: {str(e)}"

def update_sheet(sheet, results):
    """Updates a specific column in the Google Sheet with results."""
    if not results: return
    try:
        cell_range = f"B2:B{len(results)+1}"
        cells_to_update = sheet.range(cell_range)
        for i, cell in enumerate(cells_to_update):
            cell.value = results[i]
        sheet.update_cells(cells_to_update, value_input_option='RAW')
    except Exception as e:
        st.error(f"Failed to update sheet: {e}")

def load_domains(sheet):
    """Loads a list of domains from the first column of the sheet."""
    try:
        domains = sheet.col_values(1)[1:]
        return [d for d in domains if d.strip()]
    except Exception as e:
        st.error(f"Failed to load domains from sheet: {e}")
        return []

# --- STREAMLIT UI ---
st.title("🤖 BPO Keyword Scraper")
st.markdown("This tool reads domains from a Google Sheet, scrapes each site for keywords, and writes the results back to the sheet.")

with st.expander("⚙️ Configuration", expanded=True):
    sheet_name = st.text_input("1. Enter the name of your Google Sheet", "BPO Mentions Tracker")
    sheet_tab_index = st.number_input("2. Enter the sheet tab number (0 for first tab)", min_value=0, value=0)

if st.button("🚀 Start Scraping", type="primary", disabled=(not sheet_name)):
    client = authenticate_google_sheets()
    if client:
        try:
            sheet = client.open(sheet_name).get_worksheet(sheet_tab_index)
            st.info(f"Successfully connected to '{sheet_name}'.")
            domains = load_domains(sheet)
            if not domains:
                st.warning("No domains found in Column A of your sheet.")
            else:
                st.info(f"Found {len(domains)} domains. Starting scraping...")
                results = []
                progress_bar = st.progress(0, text="Initializing...")
                results_df = pd.DataFrame(columns=['Domain', 'Result'])
                for i, domain in enumerate(domains):
                    percent_complete = (i + 1) / len(domains)
                    progress_bar.progress(percent_complete, text=f"({i+1}/{len(domains)}) Scraping: {domain}")
                    result = scrape_website(domain)
                    results.append(result)
                    new_row = pd.DataFrame([{'Domain': domain, 'Result': result}])
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                
                update_sheet(sheet, results)
                progress_bar.progress(1.0, text="Process Complete!")
                st.success("✅ Done! The Google Sheet has been updated.")
                st.balloons()

                st.subheader("📊 Scraping Results")
                def style_results(val):
                    color = '#28a745' if "YES" in val else ('#dc3545' if "Error" in val else 'white')
                    return f'color: {color};'
                st.dataframe(results_df.style.applymap(style_results, subset=['Result']), use_container_width=True)
        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"Spreadsheet '{sheet_name}' not found. Check the name and that your service account has access.")
        except gspread.exceptions.WorksheetNotFound:
            st.error(f"Worksheet tab {sheet_tab_index} not found in '{sheet_name}'.")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
```

### Deployment Steps (All Free)

Follow these steps carefully to get your app online.

**Step 1: Create a `requirements.txt` File**
This file tells Streamlit's servers which Python libraries your app needs. Create a new text file named `requirements.txt` in the same folder as your `app.py` file. Its content should be exactly this:

```
streamlit
requests
beautifulsoup4
gspread
google-auth-oauthlib
pandas
```

**Step 2: Sign Up for GitHub**
If you don't have one already, create a free account at [GitHub.com](https://github.com/). GitHub is where you will store your code so Streamlit can access it.

**Step 3: Create a New GitHub Repository**
1.  On your GitHub homepage, click the `+` icon in the top-right and select **New repository**.
2.  Give it a name (e.g., `bpo-scraper-app`).
3.  Make sure it is set to **Public**. This is required for the free Streamlit hosting.
4.  Click **Create repository**.

**Step 4: Upload Your Files to GitHub**
1.  In your new repository, click the **Add file** button and select **Upload files**.
2.  Drag and drop both your `app.py` and `requirements.txt` files onto the page.
3.  Click **Commit changes**.

**Step 5: Sign Up for Streamlit Community Cloud**
1.  Go to [share.streamlit.io](https://share.streamlit.io/) and sign up for a free account. It's easiest to sign in with your GitHub account.
2.  Authorize the connection between Streamlit and your GitHub account.

**Step 6: Deploy Your App**
1.  From your Streamlit dashboard, click **New app**.
2.  Choose the GitHub repository you just created (`your-username/bpo-scraper-app`).
3.  The main file path should automatically be set to `app.py`.
4.  Click the **Advanced settings** link.

**Step 7: Add Your Secret Google Key**
This is the most important step for security.
1.  In the "Secrets" text box, you need to paste the contents of your `service_account.json` file.
2.  The format must be exactly like this:

    ```toml
    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "your-private-key-id"
    private_key = "-----BEGIN PRIVATE KEY-----\n...your-private-key...=\n-----END PRIVATE KEY-----\n"
    client_email = "your-client-email"
    client_id = "your-client-id"
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "your-client-cert-url"
    ```
    * Open your local `service_account.json` file in a text editor.
    * Carefully copy and paste each value from your JSON file into the corresponding field above. **Pay special attention to the `private_key`**, making sure to keep the `\n` characters exactly as they are in the JSON file.

3.  Click **Save**.

**Step 8: Launch!**
Click the **Deploy!** button. Streamlit will build your application and launch it. You will get a unique URL (like `bpo-scraper-app-yourname.streamlit.app`) that you can access from anywhere on the w
