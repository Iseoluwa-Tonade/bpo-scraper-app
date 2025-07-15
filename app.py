import streamlit as st
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, Playwright, TimeoutError as PlaywrightTimeoutError

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="BPO Keyword Scraper",
    page_icon="🤖",
    layout="centered",
)

# --- CONFIG & CONSTANTS ---
# Refined keyword list to be more efficient and avoid redundancy.
# The regex will handle variations.
KEYWORDS = [
    'bpo', 'business process outsourcing', 'customer', 'cx', 'support', 'csat',
    'chief customer', 'head of customer', 'vp of customer', 'director of support',
    'chief experience officer', 'c-x-o', # for CXO
    'vp of support', 'vp of service', 'vp of experience', 'vp of care',
    'head of support', 'head of service', 'head of experience', 'head of care',
    'director of customer', 'director of support', 'director of service', 'director of experience',
    'director of care', 'director of client services',
    'procurement', 'cpo', 'chief procurement officer', 'sourcing',
    'purchasing', 'vendor management', 'supplier management', 'partner management',
    'cfo', 'chief financial officer', 'fp&a', 'financial planning',
    'controller', 'subscribe', 'subscription', 'chat'
]
# Compile keywords into a single regex pattern for efficiency
# \b ensures we match whole words only. re.IGNORECASE makes it case-insensitive.
KEYWORD_PATTERN = re.compile(r'\b(' + '|'.join(re.escape(kw) for kw in KEYWORDS) + r')\b', re.IGNORECASE)

# --- CACHED RESOURCES ---

@st.cache_resource(ttl=3600)
def authenticate_google_sheets():
    """Authenticates with Google Sheets using Streamlit's Secrets."""
    try:
        creds_json = st.secrets["gcp_service_account"]
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"Google Sheets authentication failed: {e}. Ensure 'gcp_service_account' is in your Streamlit Secrets.")
        return None

@st.cache_resource(ttl=3600)
def get_requests_session():
    """Returns a cached requests.Session object."""
    return requests.Session()

# --- CORE FUNCTIONS ---

def clean_domain(domain: str) -> str | None:
    """Cleans and formats a domain string to a full URL."""
    domain = domain.strip()
    if not domain:
        return None
    # Avoid creating invalid URLs like https://https://example.com
    if domain.startswith(('http://', 'https://')):
        return domain
    return f"https://{domain}"

def process_html_for_keywords(html_content: str) -> str:
    """Parses HTML, extracts text, and finds keywords using regex."""
    soup = BeautifulSoup(html_content, "html.parser")
    # Remove tags that typically contain no useful, unique content
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    found_keywords = sorted(list(set(KEYWORD_PATTERN.findall(text.lower()))))

    if found_keywords:
        return "YES: " + ", ".join(found_keywords)
    return "NO"

def scrape_page_fast(domain: str, session: requests.Session) -> str:
    """Scrapes a single website using the fast 'requests' method."""
    url = clean_domain(domain)
    if not url:
        return "Empty Domain"
    try:
        # Use stream=True and iter_content to avoid downloading huge files
        with session.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
            stream=True
        ) as r:
            r.raise_for_status()
            html = r.content[:500_000] # Read up to 500KB
        return process_html_for_keywords(html)
    except requests.exceptions.RequestException as e:
        return f"Error: Connection failed ({type(e).__name__})"
    except Exception as e:
        return f"Error: {e}"

def scrape_page_deep(domain: str, playwright: Playwright) -> str:
    """Scrapes a single website using the deep 'Playwright' method to render JS."""
    url = clean_domain(domain)
    if not url:
        return "Empty Domain"

    browser = None # Initialize browser to None
    try:
        # Launch the browser with arguments for cloud environments
        browser = playwright.chromium.launch(
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = context.new_page()
        # Increased timeout to 60 seconds for slower sites
        page.goto(url, timeout=60000, wait_until='domcontentloaded')
        # Give the page a moment for any lazy-loaded content
        page.wait_for_timeout(2500)
        html = page.content()
        return process_html_for_keywords(html)
    except PlaywrightTimeoutError:
        return "Error: Page load timed out"
    except Exception as e:
        # Provide a more specific error message
        return f"Error: Playwright failed ({type(e).__name__})"
    finally:
        # Ensure the browser is always closed to free up resources
        if browser:
            browser.close()

def update_sheet(sheet, results_df: pd.DataFrame):
    """Updates a specific column in the Google Sheet with results."""
    if results_df.empty:
        return
    try:
        # Prepare the data for batch update (list of lists)
        update_data = results_df[['Result']].values.tolist()
        sheet.update(f'B2:B{len(update_data) + 1}', update_data, value_input_option='RAW')
    except Exception as e:
        st.error(f"Failed to update sheet: {e}")

def load_domains(sheet) -> list[str]:
    """Loads a list of domains from the first column of the sheet."""
    try:
        # Get all values from the first column, excluding the header
        all_domains = sheet.col_values(1)[1:]
        # Filter out any empty cells
        return [d.strip() for d in all_domains if d.strip()]
    except Exception as e:
        st.error(f"Failed to load domains from sheet: {e}")
        return []

# --- STREAMLIT UI ---
st.title("🤖 BPO Keyword Scraper")
st.markdown("This tool reads domains from a Google Sheet, scrapes each site for specific keywords, and writes the results back to the sheet.")

with st.expander("⚙️ **Configuration**", expanded=True):
    sheet_name = st.text_input("1. Enter the name of your Google Sheet", "BPO Mentions Tracker")
    sheet_tab_index = st.number_input("2. Enter the sheet tab number (0 for first tab)", min_value=0, value=0)
    scrape_mode = st.radio(
        "3. Select Scraping Mode",
        ('Deep', 'Fast'),
        index=0,
        horizontal=True,
        help="**Deep Mode**: Slower but more accurate (renders JavaScript). **Fast Mode**: Quicker but may miss keywords on modern websites."
    )
    concurrency = st.slider(
        "4. Set Parallel Workers",
        min_value=1,
        max_value=20,
        value=5,
        help="Number of websites to scrape simultaneously. Start with a low number (like 3-5) to avoid memory errors."
    )

if st.button("🚀 Start Scraping", type="primary", use_container_width=True, disabled=(not sheet_name)):
    client = authenticate_google_sheets()
    if client:
        try:
            sheet = client.open(sheet_name).get_worksheet(sheet_tab_index)
            st.info(f"✅ Successfully connected to '{sheet_name}'.")

            domains = load_domains(sheet)
            if not domains:
                st.warning("⚠️ No domains found in Column A of your sheet.")
            else:
                st.info(f"Found {len(domains)} domains. Starting {scrape_mode.lower()} scrape with {concurrency} workers...")

                results = {}
                progress_bar = st.progress(0, text="Initializing...")

                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    future_to_domain = {}
                    if scrape_mode == 'Deep':
                        # Using 'with' ensures Playwright is properly shut down
                        with sync_playwright() as p:
                            for domain in domains:
                                future = executor.submit(scrape_page_deep, domain, p)
                                future_to_domain[future] = domain
                    else: # Fast mode
                        session = get_requests_session()
                        for domain in domains:
                            future = executor.submit(scrape_page_fast, domain, session)
                            future_to_domain[future] = domain

                    completed_count = 0
                    for future in as_completed(future_to_domain):
                        domain = future_to_domain[future]
                        try:
                            results[domain] = future.result()
                        except Exception as e:
                            results[domain] = f"Error: Future failed ({e})"
                        completed_count += 1
                        percent_complete = completed_count / len(domains)
                        progress_bar.progress(percent_complete, text=f"({completed_count}/{len(domains)}) Scraped: {domain}")

                progress_bar.progress(1.0, text="Scraping complete! Updating Google Sheet...")

                # Reorder results to match the original domain list
                ordered_results = [results.get(domain, "Error: Not processed") for domain in domains]
                results_df = pd.DataFrame({'Domain': domains, 'Result': ordered_results})

                update_sheet(sheet, results_df)
                st.success("✅ Done! The Google Sheet has been updated.")
                st.balloons()

                st.subheader("📊 Scraping Results")
                def style_results(val):
                    color = 'white' # Default color for "NO"
                    if isinstance(val, str):
                        if "YES" in val:
                            color = '#28a745' # Green
                        elif "Error" in val:
                            color = '#dc3545' # Red
                    return f'color: {color};'

                st.dataframe(results_df.style.applymap(style_results, subset=['Result']), use_container_width=True)

        except gspread.exceptions.SpreadsheetNotFound:
            st.error(f"❌ Spreadsheet '{sheet_name}' not found. Check the name and that your service account has access.")
        except gspread.exceptions.WorksheetNotFound:
            st.error(f"❌ Worksheet tab {sheet_tab_index} not found in '{sheet_name}'.")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
