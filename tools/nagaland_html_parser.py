import json
import os
from bs4 import BeautifulSoup

def extract_apmc_prices(html_content: str) -> str:
    """
    Extracts the APMC price list table from the given HTML file and returns JSON data.
    """
    # with open(html_path, 'r', encoding='utf-8') as file:
    soup = BeautifulSoup(html_content, 'html.parser')

    table = soup.find('table')
    if not table:
        return json.dumps({"error": "No table found in the HTML file."})

    headers = [th.text.strip() for th in table.find_all('th')]
    
    # Remove the 'Telegram' header if it exists
    telegram_index = headers.index('Telegram') if 'Telegram' in headers else -1

    tbody = table.find('tbody')
    rows = tbody.find_all('tr') if tbody else table.find_all('tr')[1:]

    data = []
    for row in rows:
        cols = [td.text.strip() for td in row.find_all('td')]
        if len(cols) == len(headers):
            row_data = {}
            for i, col in enumerate(cols):
                if i != telegram_index:
                    row_data[headers[i]] = col
            data.append(row_data)

    return json.dumps(data, indent=4)

if __name__ == "__main__":
    # Get the absolute path to nagaland.html based on the current file's location
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    html_file_path = os.path.join(base_dir, 'nagaland.html')
    
    if os.path.exists(html_file_path):
        json_data = extract_apmc_prices(html_file_path)
        print(json_data)
    else:
        print(f"Error: Could not find {html_file_path}")
