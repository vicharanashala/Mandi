from bs4 import BeautifulSoup
import json
import datetime

def karnataka_mandi_parser(html_content, filter=False, commodity_name=None, report_date :str = datetime.date.today().strftime("%d/%m/%Y")):
    """
    Parses Karnataka agricultural market price HTML report
    and returns all table data as structured JSON.
    
    Args:
        html_content (str): Raw HTML content of the page
        filter (bool): If True, filter results by commodity_name
        commodity_name (str | list): Single commodity name or list of names to filter
                                     e.g. "Maize" or ["Maize", "Paddy", "Tomato"]
        report_date (str): Optional date string to add to each row
    
    Returns:
        dict: Grouped commodity data as JSON-serializable dict
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    result = {}
    main_data = []
    commodity_name = commodity_name.strip().lower().capitalize() if commodity_name else None
    # ── Validate filter args ───────────────────────────────────────────────────
    if filter and not commodity_name:
        raise ValueError("commodity_name must be provided when filter=True")

    # Normalize commodity_name to a lowercase set for fast lookup
    if filter:
        if isinstance(commodity_name, str):
            filter_set = {commodity_name.strip().lower()}
        elif isinstance(commodity_name, list):
            filter_set = {name.strip().lower() for name in commodity_name}
        else:
            raise TypeError("commodity_name must be a string or list of strings")

    # ── Locate the report div ──────────────────────────────────────────────────
    divprint = soup.find('div', id='divprint')
    if not divprint:
        raise ValueError("Could not find 'divprint' div in HTML")

    # ── Each group: <span>Group : Cereals</span><div><table>...</table></div> ──
    group_spans = divprint.find_all(
        'span',
        style=lambda s: s and 'DarkGreen' in s and 'font-weight:bold' in s
    )

    for span in group_spans:
        # "Group :  Cereals" -> "Cereals"
        group_text = span.get_text(strip=True)
        group_name = group_text.replace("Group :", "").strip()

        sibling_div = span.find_next_sibling('div')
        if not sibling_div:
            continue

        table = sibling_div.find('table')
        if not table:
            result[group_name] = []
            continue

        # Handle empty tables
        no_data_cell = table.find('td', string=lambda t: t and 'No Data' in t)
        if no_data_cell:
            result[group_name] = []
            continue

        # Extract column headers
        headers = [th.get_text(strip=True) for th in table.find_all('th')]

        rows = []
        for tr in table.find_all('tr'):
            cells = tr.find_all('td')
            if not cells or len(cells) != len(headers):
                continue

            row = {}
            for header, cell in zip(headers, cells):
                value = cell.get_text(strip=True)
                # Auto-cast numeric columns
                if header in ('Arrival', 'Min', 'Max', 'Modal'):
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                row[header] = value

            if report_date:
                row['Date'] = report_date

            # ── Apply commodity filter if enabled ──────────────────────────────
            if filter:
                commodity_value = row.get('Commodity', '').strip().lower()
                if commodity_value not in filter_set:
                    continue  # skip rows that don't match

            rows.append(row)

        # Only add group to result if it has matching rows
        if rows:
            result[group_name] = rows
            main_data.extend(rows)
    
    # return json.dumps(main_data)
    return main_data


# ── Helper wrappers ────────────────────────────────────────────────────────────

def parse_from_file(filepath, filter=False, commodity_name=None, report_date=None):
    with open(filepath, 'r', encoding='utf-8') as f:
        html_content = f.read()
    return karnataka_mandi_parser(html_content, filter=filter, commodity_name=commodity_name, report_date=report_date)


def parse_from_string(html_string, filter=False, commodity_name=None, report_date=None):
    return karnataka_mandi_parser(html_string, filter=filter, commodity_name=commodity_name, report_date=report_date)


# ── Example usage ──────────────────────────────────────────────────────────────
