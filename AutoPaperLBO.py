import os
import re
import json
from sec_downloader import Downloader
#from sec_edgar_downloader import Downloader
from lxml import etree
from functools import lru_cache
import io

def try_convert_to_float(value_str):
    try:
        return float(value_str.replace(',', '').strip())
    except Exception:
        return None

@lru_cache(maxsize=10)
def xbrl_parse_financial_data(file_path):
    """
    Non-iterparse version using fromstring.
    Wraps the extracted XBRL block in a dummy root to ensure well-formed XML.
    Removes any XML declarations before parsing.
    """
    xbrl_lines = []
    inside_xbrl = False
    with open(file_path, 'r', encoding='utf8', errors='ignore') as f:
        for line in f:
            if '<XBRL' in line:
                inside_xbrl = True
            if inside_xbrl:
                xbrl_lines.append(line)
            if '</XBRL>' in line and inside_xbrl:
                break

    if xbrl_lines:
        xbrl_content = ''.join(xbrl_lines)
    else:
        with open(file_path, 'r', encoding='utf8', errors='ignore') as f:
            xbrl_content = f.read()

    xbrl_content = xbrl_content.strip()
    # Remove any XML declarations.
    xbrl_content = re.sub(r'<\?xml[^>]+\?>', '', xbrl_content).strip()
    # Wrap in a dummy root.
    wrapped_content = f"<root>{xbrl_content}</root>"
    
    try:
        tree = etree.fromstring(wrapped_content.encode('utf8'))
    except Exception as e:
        print("Error parsing XBRL XML in non-iterparse version:", e)
        return {
            'Revenue': None,
            'Cost of Goods Sold': None,
            'Operating Income': None,
            'Depreciation': None,
            'Amortization': None,
            'Interest Expense': None,
            'Income Before Tax': None
        }

    ns = {'us-gaap': 'http://fasb.org/us-gaap/2024'}

    # Try several alternatives for Revenue.
    revenue = tree.xpath('string(//us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax)', namespaces=ns)
    if revenue.strip() == "":
        revenue = tree.xpath('string(//us-gaap:SalesRevenueNet)', namespaces=ns)
    if revenue.strip() == "":
        revenue = tree.xpath('string(//us-gaap:Revenues)', namespaces=ns)
    if revenue.strip() == "":
        revenue = tree.xpath('string(//us-gaap:NetSales)', namespaces=ns)
    if revenue.strip() == "":
        revenue = tree.xpath('string(//us-gaap:NetRevenue)', namespaces=ns)

    cogs = tree.xpath('string(//us-gaap:CostOfGoodsSold)', namespaces=ns)
    if cogs.strip() == "":
        cogs = tree.xpath('string(//us-gaap:CostOfRevenue)', namespaces=ns)

    operating_income = tree.xpath('string(//us-gaap:OperatingIncomeLoss)', namespaces=ns)
    if operating_income.strip() == "":
        operating_income = tree.xpath('string(//us-gaap:OperatingIncome)', namespaces=ns)

    dep_amort = tree.xpath('string(//us-gaap:DepreciationDepletionAndAmortization)', namespaces=ns)
    depreciation = None
    amortization = None
    if dep_amort.strip() != "":
        combined = try_convert_to_float(dep_amort)
        if combined is not None:
            depreciation = combined / 2
            amortization = combined / 2

    interest_expense = tree.xpath('string(//us-gaap:InterestExpense)', namespaces=ns)
    if interest_expense.strip() == "":
        interest_expense = tree.xpath('string(//us-gaap:InterestExpenseBenefit)', namespaces=ns)

    income_before_tax = tree.xpath('string(//us-gaap:IncomeBeforeTax)', namespaces=ns)
    if income_before_tax.strip() == "":
        income_before_tax = tree.xpath('string(//us-gaap:IncomeBeforeTaxExpenseBenefit)', namespaces=ns)

    data = {
        'Revenue': try_convert_to_float(revenue),
        'Cost of Goods Sold': try_convert_to_float(cogs),
        'Operating Income': try_convert_to_float(operating_income),
        'Depreciation': depreciation,
        'Amortization': amortization,
        'Interest Expense': try_convert_to_float(interest_expense),
        'Income Before Tax': try_convert_to_float(income_before_tax)
    }
    return data

@lru_cache(maxsize=10)
def xbrl_parse_financial_data_iterparse(file_path):
    """
    Optimized version using iterparse to stream through inline XBRL data.
    Wraps the extracted block in a dummy <root> element and removes XML declarations.
    """
    xbrl_lines = []
    inside_xbrl = False
    with open(file_path, 'r', encoding='utf8', errors='ignore') as f:
        for line in f:
            if '<XBRL' in line:
                inside_xbrl = True
            if inside_xbrl:
                xbrl_lines.append(line)
            if '</XBRL>' in line and inside_xbrl:
                break
    if xbrl_lines:
        xbrl_content = ''.join(xbrl_lines)
    else:
        with open(file_path, 'r', encoding='utf8', errors='ignore') as f:
            xbrl_content = f.read()

    xbrl_content = xbrl_content.strip()
    pattern = re.compile(r'(?s)<XBRL.*?</XBRL>')
    m = pattern.search(xbrl_content)
    if m:
        xbrl_content = m.group(0)
    xbrl_content = re.sub(r'<\?xml[^>]+\?>', '', xbrl_content).strip()
    wrapped_content = f"<root>{xbrl_content}</root>"
    stream = io.BytesIO(wrapped_content.encode('utf8'))
    
    target_names = {
        "us-gaap:SalesRevenueNet": "Revenue",
        "us-gaap:Revenues": "Revenue",
        "us-gaap:NetSales": "Revenue",
        "us-gaap:NetRevenue": "Revenue",
        "us-gaap:CostOfGoodsSold": "Cost of Goods Sold",
        "us-gaap:CostOfGoodsAndServicesSold": "Cost of Goods Sold",
        "us-gaap:CostOfRevenue": "Cost of Goods Sold",
        "us-gaap:OperatingIncomeLoss": "Operating Income",
        "us-gaap:OperatingIncome": "Operating Income",
        "us-gaap:DepreciationDepletionAndAmortization": "DepreciationAmortation",
        "us-gaap:InterestExpense": "Interest Expense",
        "us-gaap:InterestExpenseBenefit": "Interest Expense",
        "us-gaap:IncomeBeforeTax": "Income Before Tax",
        "us-gaap:IncomeBeforeTaxExpenseBenefit": "Income Before Tax",
        "us-gaap:ProfitBeforeTax": "Income Before Tax",
        "us-gaap:PreTaxIncome": "Income Before Tax"
    }
    data = {
        "Revenue": None,
        "Cost of Goods Sold": None,
        "Operating Income": None,
        "DepreciationAmortation": None,
        "Interest Expense": None,
        "Income Before Tax": None
    }
    inline_ns = "http://www.xbrl.org/2013/inlineXBRL"
    try:
        for event, elem in etree.iterparse(stream, events=('end',)):
            if elem.tag == f"{{{inline_ns}}}nonFraction":
                name = elem.attrib.get("name", "")
                if name in target_names:
                    key = target_names[name]
                    text = elem.text or ""
                    value = try_convert_to_float(text)
                    if value is not None:
                        if key == "Revenue":
                            if data["Revenue"] is None or name == "us-gaap:SalesRevenueNet":
                                data["Revenue"] = value
                        elif key == "Cost of Goods Sold":
                            if data["Cost of Goods Sold"] is None or name in ("us-gaap:CostOfGoodsSold", "us-gaap:CostOfGoodsAndServicesSold"):
                                data["Cost of Goods Sold"] = value
                        elif key == "Operating Income":
                            if data["Operating Income"] is None or name == "us-gaap:OperatingIncomeLoss":
                                data["Operating Income"] = value
                        elif key == "DepreciationAmortation":
                            if data["DepreciationAmortation"] is None:
                                data["DepreciationAmortation"] = value
                        elif key == "Interest Expense":
                            if data["Interest Expense"] is None or name == "us-gaap:InterestExpense":
                                data["Interest Expense"] = value
                        elif key == "Income Before Tax":
                            if data["Income Before Tax"] is None or name in ("us-gaap:IncomeBeforeTax", "us-gaap:ProfitBeforeTax", "us-gaap:PreTaxIncome"):
                                data["Income Before Tax"] = value
            elem.clear()
    except Exception as e:
        print("Error during iterparse:", e)
        return {}
    
    if data["Income Before Tax"] is None and data["Operating Income"] is not None:
        data["Income Before Tax"] = data["Operating Income"]
    
    depreciation = None
    amortization = None
    if data["DepreciationAmortation"] is not None:
        combined = data["DepreciationAmortation"]
        depreciation = combined / 2
        amortization = combined / 2

    final_data = {
        "Revenue": data["Revenue"],
        "Cost of Goods Sold": data["Cost of Goods Sold"],
        "Operating Income": data["Operating Income"],
        "Depreciation": depreciation,
        "Amortization": amortization,
        "Interest Expense": data["Interest Expense"],
        "Income Before Tax": data["Income Before Tax"]
    }
    return final_data

### NEW: Functions for Calculating Financial Health ###

def calculate_financial_ratios(data):
    """
    Computes a set of financial ratios indicative of financial health.
    """
    ratios = {}
    revenue = data.get("Revenue")
    cogs = data.get("Cost of Goods Sold")
    op_income = data.get("Operating Income")
    depreciation = data.get("Depreciation")
    amortization = data.get("Amortization")
    interest_expense = data.get("Interest Expense")
    income_before_tax = data.get("Income Before Tax")
    
    # Gross Profit and Gross Margin
    if revenue is not None and cogs is not None:
        gross_profit = revenue - cogs
        ratios["Gross Profit"] = gross_profit
        ratios["Gross Margin"] = gross_profit / revenue if revenue != 0 else None
    else:
        ratios["Gross Profit"] = None
        ratios["Gross Margin"] = None

    # Operating Margin
    if revenue is not None and op_income is not None:
        ratios["Operating Margin"] = op_income / revenue if revenue != 0 else None
    else:
        ratios["Operating Margin"] = None

    # EBITDA and EBITDA Margin
    if op_income is not None and depreciation is not None and amortization is not None:
        EBITDA = op_income + depreciation + amortization
        ratios["EBITDA"] = EBITDA
        ratios["EBITDA Margin"] = EBITDA / revenue if revenue and revenue != 0 else None
    else:
        ratios["EBITDA"] = None
        ratios["EBITDA Margin"] = None

    # Effective Tax Rate
    if income_before_tax is not None and op_income is not None:
        tax_expense = income_before_tax - op_income
        ratios["Effective Tax Rate"] = tax_expense / income_before_tax if income_before_tax != 0 else None
    else:
        ratios["Effective Tax Rate"] = None

    # Interest Coverage Ratio
    if op_income is not None and interest_expense is not None and interest_expense != 0:
        ratios["Interest Coverage Ratio"] = op_income / interest_expense
    else:
        ratios["Interest Coverage Ratio"] = None

    return ratios

def scale(value, low, high):
    """
    Linearly scale a value to a 1-10 score based on a range [low, high].
    Values at or below low score a 1; at or above high score a 10.
    """
    if value <= low:
        return 1
    if value >= high:
        return 10
    return 1 + (value - low) / (high - low) * 9

def compute_composite_health_score(ratios):
    """
    Computes a composite financial health score (scale 1 to 10) from key ratios.
    Each ratio is normalized to typical benchmark ranges.
    """
    scores = {}
    # Gross Margin: assume healthy companies might range from 10% to 70%
    gm = ratios.get("Gross Margin")
    scores["Gross Margin"] = scale(gm, 0.1, 0.7) if gm is not None else 5

    # Operating Margin: 0% to 30%
    om = ratios.get("Operating Margin")
    scores["Operating Margin"] = scale(om, 0.0, 0.3) if om is not None else 5

    # EBITDA Margin: 0% to 30%
    em = ratios.get("EBITDA Margin")
    scores["EBITDA Margin"] = scale(em, 0.0, 0.3) if em is not None else 5

    # Effective Tax Rate: lower is better; assume 20% is good, 40% is poor.
    etr = ratios.get("Effective Tax Rate")
    score_etr = scale(etr, 0.2, 0.4) if etr is not None else 5
    scores["Effective Tax Rate"] = 10 - score_etr  # invert so lower tax rates score higher

    # Interest Coverage Ratio: assume a ratio of 1 to 5 is our scale.
    icr = ratios.get("Interest Coverage Ratio")
    scores["Interest Coverage Ratio"] = scale(icr, 1, 5) if icr is not None else 5

    # Composite score as the average of the five scores.
    composite = sum(scores.values()) / len(scores)
    return composite, scores

def calculate_growth_trajectories(ratios_list):
    """
    Computes average year-over-year growth rates for each ratio based on historical data.
    Expects a list of ratio dictionaries (ordered chronologically).
    """
    trajectories = {}
    if len(ratios_list) < 2:
        return trajectories  # Not enough data for growth calculations.

    keys = ratios_list[0].keys()
    for key in keys:
        growth_rates = []
        for i in range(1, len(ratios_list)):
            prev = ratios_list[i-1].get(key)
            current = ratios_list[i].get(key)
            if prev is not None and current is not None and prev != 0:
                growth = (current - prev) / prev
                growth_rates.append(growth)
        if growth_rates:
            avg_growth = sum(growth_rates) / len(growth_rates)
            trajectories[key] = avg_growth
    return trajectories

def generate_manual_statement(ratios, composite_score):
    """
    Generates a formatted statement summarizing the financial health based on computed ratios.
    """
    def fmt_currency(val):
        return f"${val:,.0f}" if val is not None else "N/A"
    def fmt_percent(val):
        return f"{val:.1%}" if val is not None else "N/A"
    def fmt_float(val):
        return f"{val:.1f}" if val is not None else "N/A"
    
    statement = (
        f"Financial Health Overview:\n"
        f"- Gross Margin: {fmt_percent(ratios.get('Gross Margin'))}\n"
        f"- Operating Margin: {fmt_percent(ratios.get('Operating Margin'))}\n"
        f"- EBITDA Margin: {fmt_percent(ratios.get('EBITDA Margin'))}\n"
        f"- Effective Tax Rate: {fmt_percent(ratios.get('Effective Tax Rate'))}\n"
        f"- Interest Coverage Ratio: {fmt_float(ratios.get('Interest Coverage Ratio'))}\n\n"
        f"Composite Financial Health Score: {fmt_float(composite_score)} out of 10.\n"
        f"This score reflects the company's overall profitability, cost efficiency, and financial stability. "
        f"Higher scores indicate stronger financial health."
    )
    return statement

def main():
    ticker = "AAPL"
    filing_type = "10-K"

    with open("credentials.json", "r") as jsonfile:
        credentials = json.load(jsonfile)

    dl = Downloader(credentials["username"], credentials["company"])
    print(f"Downloading the latest {filing_type} for {ticker}...")
    # dl.get(filing_type, ticker, limit=1)
    metadatas = dl.get_filing_metadatas("1/"+ticker+"/"+filing_type)
    print(metadatas)

    base_dir = os.path.join(os.getcwd(), "sec-edgar-filings", ticker, filing_type)
    new_base_dir = os.path.join(os.getcwd(), "new-sec-filings", ticker)

    os.makedirs(os.path.dirname(new_base_dir), exist_ok=True)

    
    for metadata in metadatas:
        html = dl.download_filing(url=metadata.primary_doc_url).decode()
        os.makedirs(os.path.dirname(new_base_dir+"/"+filing_type+".txt"), exist_ok=True)
        metadata=open(new_base_dir+"/"+filing_type+".txt",'w')
        metadata.write(html)
        metadata.close()

    #print("Base directory:", base_dir)

    filing_folders = [folder for folder in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, folder))]
    if not filing_folders:
        print("No filings found.")
        return

    composite_score, ratio_scores = compute_composite_health_score(latest_ratios)
    print("\nComposite Financial Health Score (1 to 10): {:.2f}".format(composite_score))
    print("Individual Ratio Scores:")
    for key, value in ratio_scores.items():
        print(f"  {key}: {value:.2f}")

    # Generate a detailed financial health statement.
    statement = generate_manual_statement(composite_score)
    print("\nFinancial Health Statement:")
    print(statement)

if __name__ == "__main__":
    main()
