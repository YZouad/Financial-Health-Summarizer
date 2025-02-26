import re
from bs4 import BeautifulSoup
import os
import json
from sec_downloader import Downloader
from sec_downloader.types import RequestedFilings
#from sec_edgar_downloader import Downloader

def extract_revenue(text):
    # Define regex patterns for revenue
    revenue_patterns = [
        r'Total revenue[^\d]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
        r'Net sales[^\d]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
        r'Total net revenue[^\d]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
    ]
    
    for pattern in revenue_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1).replace(',', ''))
    
    # If regex fails, try parsing HTML
    soup = BeautifulSoup(text, 'html.parser')
    revenue_tags = soup.find_all(string=re.compile('revenue|sales', re.IGNORECASE))
    
    for tag in revenue_tags:
        parent = tag.parent
        if parent.name in ['td', 'th']:
            next_sibling = parent.find_next_sibling()
            if next_sibling:
                try:
                    return float(next_sibling.text.strip().replace(',', ''))
                except ValueError:
                    continue
    
    return None




def main():
    TICKER = "AAPL"
    FILING_TYPE = "10-K"
    AMOUNT_OF_FILINGS = 2

    with open("credentials.json", "r") as jsonfile:
        credentials = json.load(jsonfile)

    dl = Downloader(credentials["username"], credentials["company"])

    metadatas = dl.get_filing_metadatas(
        RequestedFilings(ticker_or_cik=TICKER, form_type=FILING_TYPE, limit=AMOUNT_OF_FILINGS)
    )

    base_dir = os.path.join(os.getcwd(), "sec-edgar-filings", TICKER, FILING_TYPE)
    os.makedirs(base_dir, exist_ok=True)

    for metadata in metadatas:
        accession = metadata.accession_number
        local_filename = os.path.join(base_dir, f"{accession}.txt")

        content_bytes = dl.download_filing(url=metadata.primary_doc_url)
        content_str = content_bytes.decode("utf-8", errors="ignore")

        with open(local_filename, "w", encoding="utf-8") as f:
            f.write(content_str)

        try:
            revenue = extract_revenue(content_str)
            print(f"Revenue: {revenue}")
        except Exception as e:
            print(f"Error parsing filing {accession}: {e}")
            continue

    # Assuming 'text' contains your 10-K filing content
    # revenue = extract_revenue(text)
    # print(f"Revenue: {revenue}")

if __name__ == "__main__":
    main()
