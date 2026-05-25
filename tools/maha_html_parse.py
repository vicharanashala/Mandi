from bs4 import BeautifulSoup
import json

def maharashtra_parse(html_content,apmc):

    soup = BeautifulSoup(html_content, "html.parser")

    rows = soup.find_all("tr")

    data = []
    current_date = None

    for row in rows:
        cols = row.find_all("td")

        # Date row
        if len(cols) == 1:
            current_date = cols[0].get_text(strip=True)

        # Data row
        elif len(cols) == 7:
            item = {
                "date": current_date,
                "Market": apmc,
                "commodity": cols[0].get_text(strip=True),
                "variety": cols[1].get_text(strip=True),
                "unit": cols[2].get_text(strip=True),
                "arrival": cols[3].get_text(strip=True),
                "min_price": cols[4].get_text(strip=True),
                "max_price": cols[5].get_text(strip=True),
                "modal_price": cols[6].get_text(strip=True)
            }

            data.append(item)

    # Convert to JSON
    
    return (data)