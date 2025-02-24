import os
import re
import json
from sec_edgar_downloader import Downloader
from lxml import etree
from functools import lru_cache
import io
from transformers import pipeline

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
    Wraps the extracted block in a dummy <root> element.
    Removes XML declarations before parsing.
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
    
    # Fallback: if Income Before Tax is still None, use Operating Income as proxy.
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

def calculate_metrics(data):
    """
    Computes common financial metrics and diagnostic ratios.
    """
    revenue = data.get("Revenue")
    cogs = data.get("Cost of Goods Sold")
    operating_income = data.get("Operating Income")
    depreciation = data.get("Depreciation")
    amortization = data.get("Amortization")
    interest_expense = data.get("Interest Expense")
    income_before_tax = data.get("Income Before Tax")
    
    EBIT = operating_income
    if EBIT is None and revenue is not None and cogs is not None:
        EBIT = revenue - cogs

    EBITDA = EBIT
    if EBITDA is not None:
        if depreciation is not None:
            EBITDA += depreciation
        if amortization is not None:
            EBITDA += amortization

    if income_before_tax is None and EBIT is not None:
        income_before_tax = EBIT
    EBT = income_before_tax

    diagnostics = {}
    if revenue is not None and cogs is not None and revenue != 0:
        gross_profit = revenue - cogs
        diagnostics["Gross Profit"] = gross_profit
        diagnostics["Gross Margin"] = gross_profit / revenue
        diagnostics["Cost Efficiency"] = cogs / revenue
    if revenue is not None and operating_income is not None and revenue != 0:
        diagnostics["Operating Margin"] = operating_income / revenue
    if revenue is not None and EBITDA is not None and revenue != 0:
        diagnostics["EBITDA Margin"] = EBITDA / revenue
    if revenue is not None and income_before_tax is not None and revenue != 0:
        diagnostics["Pre-tax Margin"] = income_before_tax / revenue
    if operating_income is not None and interest_expense not in (None, 0):
        diagnostics["Interest Coverage Ratio"] = operating_income / interest_expense
    if revenue is not None and depreciation is not None and revenue != 0:
        diagnostics["Depreciation Ratio"] = depreciation / revenue
    if revenue is not None and amortization is not None and revenue != 0:
        diagnostics["Amortization Ratio"] = amortization / revenue

    metrics = {
        "EBIT": EBIT,
        "EBITDA": EBITDA,
        "EBT": EBT,
        **diagnostics
    }
    return metrics

def compute_financial_health_score(metrics):
    """
    Computes a composite financial health score (scale 1 to 10) using weighted ratios.
    If any key metric is missing, neutral default values are used.
    """
    gm = metrics.get("Gross Margin", 0.6)          # Neutral ~60%
    om = metrics.get("Operating Margin", 0.15)      # Neutral ~15%
    ebm = metrics.get("EBITDA Margin", 0.15)         # Neutral ~15%
    ptm = metrics.get("Pre-tax Margin", 0.15)        # Neutral ~15%
    itr = metrics.get("Interest Coverage Ratio", 10) # Neutral ~10
    ce = metrics.get("Cost Efficiency", 0.35)        # Neutral ~35%

    def scale(value, low, high):
        score = (value - low) / (high - low) * 10
        return max(1, min(score, 10))

    score_gross = scale(gm, 0.4, 0.8)
    score_operating = scale(om, 0.0, 0.3)
    score_ebitda = scale(ebm, 0.0, 0.3)
    score_pretax = scale(ptm, 0.0, 0.3)
    score_interest = scale(itr, 1, 20)
    score_cost = scale(0.5 - ce, 0, 0.3)

    composite = (0.25 * score_gross +
                 0.20 * score_operating +
                 0.15 * score_ebitda +
                 0.15 * score_pretax +
                 0.15 * score_interest +
                 0.10 * score_cost)
    return composite

def generate_ai_statement(metrics, composite_score):
    """
    Uses a text-generation pipeline to generate a personalized financial health statement.
    If the generated text is blank, it falls back to a manual summary.
    """
    # Helper formatting functions.
    def fmt_currency(val):
        return f"${val:.0f}" if val is not None else "not available"
    def fmt_percent(val):
        return f"{val:.1%}" if val is not None else "not available"
    def fmt_float(val):
        return f"{val:.1f}" if val is not None else "not available"
    
    prompt = (
        "You are a seasoned financial analyst. Analyze the following metrics for the company "
        "and provide a concise, professional summary of its financial health. Focus on key strengths, "
        "weaknesses, and overall trends. Mention if certain key metrics are missing.\n\n"
        f"Operating Income: {fmt_currency(metrics.get('Operating Income'))}\n"
        f"EBITDA: {fmt_currency(metrics.get('EBITDA'))}\n"
        f"Gross Margin: {fmt_percent(metrics.get('Gross Margin'))}\n"
        f"Operating Margin: {fmt_percent(metrics.get('Operating Margin'))}\n"
        f"Interest Coverage Ratio: {fmt_float(metrics.get('Interest Coverage Ratio'))}\n"
        f"Pre-tax Margin: {fmt_percent(metrics.get('Pre-tax Margin'))}\n"
        f"Composite Financial Health Score: {fmt_float(composite_score)} out of 10\n\n"
        "Provide your detailed analysis in one or two sentences:"
    )
    generator = pipeline("text-generation", model="distilgpt2", truncation=True, max_length=220, temperature=0.2)
    result = generator(prompt, max_length=220, do_sample=True, temperature=0.2)
    generated_text = result[0]['generated_text']
    statement = generated_text[len(prompt):].strip()
    # Fallback: if the statement is empty, use a manual summary.
    if not statement:
        statement = (
            f"The company has an operating income of {fmt_currency(metrics.get('Operating Income'))} and an EBITDA of {fmt_currency(metrics.get('EBITDA'))}. "
            f"It achieves a gross margin of {fmt_percent(metrics.get('Gross Margin'))} and an operating margin of {fmt_percent(metrics.get('Operating Margin'))}, "
            f"resulting in a composite financial health score of {fmt_float(composite_score)} out of 10."
        )
    else:
        if "." in statement:
            statement = statement.split(".")[0].strip() + "."
    return statement

def main():
    ticker = "AAPL"
    filing_type = "10-K"

    with open("credentials.json", "r") as jsonfile:
        credentials = json.load(jsonfile)

    dl = Downloader(credentials["username"], credentials["company"])
    print(f"Downloading the latest {filing_type} for {ticker}...")
    # dl.get(filing_type, ticker, limit=1)

    base_dir = os.path.join(os.getcwd(), "sec-edgar-filings", ticker, filing_type)
    print("Base directory:", base_dir)

    filing_folders = [folder for folder in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, folder))]
    if not filing_folders:
        print("No filings found.")
        return

    first_filing_dir = os.path.join(base_dir, filing_folders[0])
    filing_files = os.listdir(first_filing_dir)
    if not filing_files:
        print("No files found in the first filing directory.")
        return

    latest_file = os.path.join(first_filing_dir, filing_files[0])
    print(f"Parsing file: {latest_file}")

    data = xbrl_parse_financial_data_iterparse(latest_file)
    # If revenue is still missing from iterparse, fall back to non-iterparse extraction.
    alt_data = xbrl_parse_financial_data(latest_file)
    if alt_data.get("Revenue") is not None:
        data["Revenue"] = alt_data.get("Revenue")

    print("\nExtracted Financial Data (via optimized XBRL parsing):")
    for key, value in data.items():
        print(f"{key}: {value}")

    metrics = calculate_metrics(data)
    print("\nCalculated Metrics and Diagnostics:")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    composite_score = compute_financial_health_score(metrics)
    print(f"\nComposite Financial Health Score (1 to 10): {composite_score:.2f}")

    statement = generate_ai_statement(metrics, composite_score)
    print("\nFinancial Health Statement:")
    print(statement)

if __name__ == "__main__":
    main()
