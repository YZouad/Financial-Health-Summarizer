import os
import re
from sec_edgar_downloader import Downloader
from lxml import etree
from functools import lru_cache
import io
from transformers import pipeline
from Credentials import Credentials

def try_convert_to_float(value_str):
    try:
        return float(value_str.replace(',', '').strip())
    except Exception:
        return None

@lru_cache(maxsize=10)
def xbrl_parse_financial_data(file_path):
    """
    Non-iterparse version using fromstring.
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
    m = re.search(r'(<\?xml[^>]+\?>)', xbrl_content)
    if m:
        xbrl_content = xbrl_content[m.start():]

    try:
        tree = etree.fromstring(xbrl_content.encode('utf8'))
    except Exception as e:
        print("Error parsing XBRL XML:", e)
        return {}

    ns = {'us-gaap': 'http://fasb.org/us-gaap/2024'}

    revenue = tree.xpath('string(//us-gaap:SalesRevenueNet)', namespaces=ns)
    if revenue.strip() == "":
        revenue = tree.xpath('string(//us-gaap:Revenues)', namespaces=ns)

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
    This version:
      - Extracts the first XBRL block,
      - Removes all XML declarations,
      - Wraps the block in a dummy <root> element,
      - Iterates over inline XBRL nonFraction elements checking their "name" attribute.
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
        "us-gaap:CostOfGoodsSold": "Cost of Goods Sold",
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
                            if data["Cost of Goods Sold"] is None or name == "us-gaap:CostOfGoodsSold":
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
    
    if data["Income Before Tax"] is None and data["Operating Income"] is not None and data["Interest Expense"] is not None:
        data["Income Before Tax"] = data["Operating Income"] - data["Interest Expense"]
    
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
    Computes common financial metrics and diagnostic ratios:
      - EBIT: Operating Income (or approximated as Revenue minus COGS)
      - EBITDA: EBIT plus Depreciation and Amortization
      - EBT: Income Before Tax (or EBIT less Interest Expense if not available)
      - Gross Profit: Revenue minus Cost of Goods Sold
      - Gross Margin: (Gross Profit / Revenue)
      - Operating Margin: (Operating Income / Revenue)
      - EBITDA Margin: (EBITDA / Revenue)
      - Pre-tax Margin: (Income Before Tax / Revenue)
      - Interest Coverage Ratio: (Operating Income / Interest Expense)
      - Depreciation Ratio: (Depreciation / Revenue)
      - Amortization Ratio: (Amortization / Revenue)
      - Cost Efficiency: (Cost of Goods Sold / Revenue)
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

    if income_before_tax is None and EBIT is not None and interest_expense is not None:
        income_before_tax = EBIT - interest_expense
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
    Computes a composite financial health score (scale 1 to 10) using a weighted average of:
      - Gross Margin (best if >=0.8, worst if <=0.4)
      - Operating Margin (0 to 0.2 scale)
      - EBITDA Margin (0 to 0.2 scale)
      - Pre-tax Margin (0 to 0.2 scale)
      - Interest Coverage Ratio (best if >=15, worst if <=1)
      - Cost Efficiency (best if <=0.2, worst if >=0.5)
    """
    gm = metrics.get("Gross Margin", 0.0)
    om = metrics.get("Operating Margin", 0.0)
    ebm = metrics.get("EBITDA Margin", 0.0)
    ptm = metrics.get("Pre-tax Margin", 0.0)
    itr = metrics.get("Interest Coverage Ratio", 0.0)
    ce = metrics.get("Cost Efficiency", 1.0)  # lower is better

    def scale(value, low, high):
        score = (value - low) / (high - low) * 10
        return max(1, min(score, 10))

    score_gross = scale(gm, 0.4, 0.8)
    score_operating = scale(om, 0.0, 0.2)
    score_ebitda = scale(ebm, 0.0, 0.2)
    score_pretax = scale(ptm, 0.0, 0.2)
    score_interest = scale(itr, 1, 15)
    # For cost efficiency, lower is better; invert the scale: best if <=0.2, worst if >=0.5.
    score_cost = scale(0.5 - ce, 0.5 - 0.5, 0.5 - 0.2)

    composite = (0.25 * score_gross +
                 0.20 * score_operating +
                 0.15 * score_ebitda +
                 0.15 * score_pretax +
                 0.15 * score_interest +
                 0.10 * score_cost)
    return composite

def generate_ai_statement(metrics, composite_score):
    """
    Uses a Hugging Face text-generation pipeline to generate a personalized
    financial health statement based on the metrics and composite score.
    """
    # Prepare a prompt with the key metrics.
    prompt = (
        "Based on the following financial metrics, generate a personalized statement about the company's financial health:\n"
        f"Gross Margin: {metrics.get('Gross Margin', 0):.1%}\n"
        f"Operating Margin: {metrics.get('Operating Margin', 0):.1%}\n"
        f"EBITDA Margin: {metrics.get('EBITDA Margin', 0):.1%}\n"
        f"Pre-tax Margin: {metrics.get('Pre-tax Margin', 0):.1%}\n"
        f"Interest Coverage Ratio: {metrics.get('Interest Coverage Ratio', 0):.1f}\n"
        f"Cost Efficiency (COGS/Revenue): {metrics.get('Cost Efficiency', 0):.2f}\n"
        f"Composite Financial Health Score: {composite_score:.1f} (scale 1 to 10)\n\n"
        "Write one or two sentences summarizing the overall financial health of the company:"
    )
    # Initialize the generator (this loads a self-hosted model such as GPT-2)
    generator = pipeline("text-generation", model="gpt2")
    result = generator(prompt, max_length=150, do_sample=True, temperature=0.8)
    generated_text = result[0]['generated_text']
    # Remove the prompt from the generated text.
    statement = generated_text[len(prompt):].strip()
    # Clean up the statement.
    statement = statement.split("\n")[0].strip()
    return statement

def main():
    ticker = "AAPL"
    filing_type = "10-K"

    dl = Downloader(Credentials.get_user, Credentials.get_company)
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