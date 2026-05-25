"""
MEGAMB Daily Report Data Parser - Advanced Version using BeautifulSoup
Robust extraction of table data with proper handling of rowspan, colspan
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any
from pathlib import Path
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def parse_megamb_html_beautifulsoup(html_content: str, date: str =datetime.now().strftime('%d/%m/%Y')) -> List[Dict[str, Any]]:
    """
    Parse MEGAMB HTML using BeautifulSoup with robust handling of complex tables
    
    Args:
        html_content: Raw HTML content
        date: Date in DD-MM-YYYY format
    
    Returns:
        List of standardized records
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Error: BeautifulSoup4 not installed. Install with: pip install beautifulsoup4")
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []
    
    # Parse date
    try:
        date_obj = datetime.strptime(date, '%d-%m-%Y')
        formatted_date = date_obj.strftime('%d/%m/%Y')
    except:
        formatted_date = date
    
    # Find all tables
    tables = soup.find_all('table', {'class': 'table'})
    
    for table in tables:
        # Get header row
        header_row = table.find('tr')
        if not header_row:
            continue
        
        headers = []
        for th in header_row.find_all(['th', 'td']):
            headers.append(th.get_text(strip=True))
        
        if not headers or len(headers) < 5:
            continue
        
        # Get data rows
        data_rows = table.find_all('tr')[1:]
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            
            # Skip pagination rows
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            if any(kw in ' '.join(cell_texts) for kw in ['Next', 'Last', 'Previous', 'First']):
                continue
            
            # Skip if not enough columns
            if len(cell_texts) < len(headers):
                continue
            
            # Create row dictionary
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(cell_texts):
                    row_dict[header] = cell_texts[i]
            
            # Skip if no commodity data
            if not row_dict.get("Commodity Name", "").strip():
                continue
            
            # Convert to standard format
            standardized = {
                "Commodity": row_dict.get("Commodity Name", "").strip(),
                "Variety": row_dict.get("Variety", "").strip(),
                "Grade": row_dict.get("Grade", "").strip(),
                "Market": row_dict.get("Market", "").strip(),
                "Arrival": convert_to_number(row_dict.get("Arrivals (Quintals)", row_dict.get("Arrival (Quintals)", "0"))),
                "Unit": row_dict.get("Unit", "Quintal").strip(),
                "Min": convert_to_number(row_dict.get("Minimum Price (Rs./Quintal)", "0")),
                "Max": convert_to_number(row_dict.get("Maximum Price (Rs./Quintal)", "0")),
                "Modal": convert_to_number(row_dict.get("Modal Price (Rs./Quintal)", "0")),
                "Date": formatted_date
            }
            
            # Only add if has valid commodity
            if standardized["Commodity"]:
                results.append(standardized)
    
    return json.dumps(results, indent=4)


def convert_to_number(value: str) -> float:
    """Convert string to number, handling various formats"""
    if not value:
        return 0
    
    value = str(value).strip()
    
    # Remove commas and extra whitespace
    value = value.replace(',', '').strip()
    
    try:
        num = float(value)
        return int(num) if num == int(num) else num
    except ValueError:
        return 0

# Command line interface
if __name__ == "__main__":
    logging.info("Starting to parse Meghalaya data...")
    