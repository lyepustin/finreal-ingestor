import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# Path to the HTML file containing CaixaBank transaction data
html_file_path = 'data/html/caixa/transactions/history_2025_29_june.html'
# Path where the CSV file will be saved
csv_file_path = 'data/csv/caixabank-transactions-2025.csv'

# List to store the extracted data
data = []

def parse_date(date_text):
    """Parse Spanish date format to standard date format"""
    # Clean the date text
    date_text = date_text.strip()
    
    # Handle "Hoy" (Today) - assuming the file is from June 29, 2025
    if "Hoy" in date_text:
        return "29/06/2025"
    
    # Handle other Spanish date formats like "Sáb 28 Jun", "Vie 27 Jun", etc.
    # Extract day and month
    day_month_match = re.search(r'(\d{1,2})\s+(\w{3})', date_text)
    if day_month_match:
        day = day_month_match.group(1).zfill(2)
        month_abbr = day_month_match.group(2)
        
        # Spanish month abbreviations to numbers
        month_map = {
            'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'
        }
        
        month = month_map.get(month_abbr, '06')  # Default to June if not found
        year = '2025'  # Assuming 2025 based on the filename
        
        return f"{day}/{month}/{year}"
    
    # If no pattern matches, return a default
    return "01/01/2025"

def parse_amount(amount_text):
    """Parse amount text and convert to float"""
    # Remove € symbol and strip whitespace
    amount_text = amount_text.replace('€', '').strip()
    
    # Handle negative amounts
    is_negative = amount_text.startswith('-')
    if is_negative:
        amount_text = amount_text[1:]
    
    # Handle European number format (thousands separator with dot, decimal with comma)
    # Examples: "3.335,15" -> 3335.15, "1.000,00" -> 1000.00, "26,46" -> 26.46
    if ',' in amount_text:
        # Split by comma to separate decimal part
        parts = amount_text.split(',')
        if len(parts) == 2:
            integer_part = parts[0].replace('.', '')  # Remove thousands separators
            decimal_part = parts[1]
            amount_text = f"{integer_part}.{decimal_part}"
        else:
            # If multiple commas, just remove dots (thousands separators) and replace last comma with dot
            amount_text = amount_text.replace('.', '').replace(',', '.')
    else:
        # No comma, but might have dots as thousands separators
        # If more than one dot or if dot is not in last 3 positions, treat as thousands separator
        dot_count = amount_text.count('.')
        if dot_count > 1:
            # Multiple dots - treat all as thousands separators
            amount_text = amount_text.replace('.', '')
        elif dot_count == 1:
            dot_pos = amount_text.rfind('.')
            # If dot is not in decimal position (last 1-3 chars), treat as thousands separator
            if len(amount_text) - dot_pos > 4:
                amount_text = amount_text.replace('.', '')
    
    try:
        result = float(amount_text)
        return -result if is_negative else result
    except ValueError:
        print(f"Warning: Could not parse amount '{amount_text}'")
        return 0.0

def extract_merchant_name(categoria_cell):
    """Extract merchant name from the categoria cell"""
    # Try to find text within span elements
    spans = categoria_cell.find_all('span')
    for span in spans:
        if span.get('class') and 'margin-right10' in span.get('class', []):
            text = span.get_text(strip=True)
            if text and not text.startswith('tipomov'):
                return text
    
    # Fallback: get all text and clean it
    text = categoria_cell.get_text(strip=True)
    # Remove extra whitespace and clean up
    text = re.sub(r'\s+', ' ', text)
    
    # Extract meaningful merchant name (usually the last meaningful part)
    lines = text.split('\n')
    for line in reversed(lines):
        line = line.strip()
        if line and not line.startswith('c-category') and len(line) > 2:
            return line
    
    return text[:50] if text else "Unknown"

def extract_account_info(service_cell):
    """Extract account information from the service cell"""
    if not service_cell:
        return "Unknown"
    
    # Get the text content
    text = service_cell.get_text(strip=True)
    
    # Clean up HTML entities and extra whitespace
    text = text.replace('\xa0', ' ').replace('&nbsp;', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Extract account patterns
    # Pattern 1: "Cuenta ...1433" or "MyCard ...5246"
    account_match = re.search(r'(Cuenta|MyCard|CYBERTARJETA)\s*\.{0,3}\s*(\d+)', text)
    if account_match:
        account_type = account_match.group(1)
        account_number = account_match.group(2)
        return f"{account_type} {account_number}"
    
    # If no pattern matches, return cleaned text
    return text if text else "Unknown"

def categorize_transaction(merchant_name, amount):
    """Categorize transaction based on merchant name and amount"""
    merchant_lower = merchant_name.lower()
    
    # Income transactions
    if amount > 0:
        if 'nomina' in merchant_lower or 'salary' in merchant_lower:
            return 'nomina'
        elif 'bizum recibido' in merchant_lower:
            return 'bizum recibido'
        elif 'transf' in merchant_lower and 'favor' in merchant_lower:
            return 'transferencia recibida'
        else:
            return 'ingreso'
    
    # Expense transactions
    else:
        if any(word in merchant_lower for word in ['mercadona', 'alcampo', 'super']):
            return 'compra supermercado'
        elif any(word in merchant_lower for word in ['zara', 'primark', 'lefties', 'massimo']):
            return 'compra ropa'
        elif any(word in merchant_lower for word in ['ikea', 'leroy']):
            return 'compra hogar'
        elif 'bizum enviado' in merchant_lower:
            return 'bizum enviado'
        elif any(word in merchant_lower for word in ['restaurante', 'gelateria']):
            return 'restaurante'
        elif 'farmacia' in merchant_lower:
            return 'farmacia'
        else:
            return 'gasto varios'

# Read and parse the HTML file
print("Reading HTML file...")
with open(html_file_path, 'r', encoding='utf-8') as file:
    html_content = file.read()

# Parse HTML with BeautifulSoup
soup = BeautifulSoup(html_content, 'html.parser')

# Find all transaction rows
transaction_rows = soup.find_all('tr', class_=['actividad', 'actividad noLeido'])

print(f"Found {len(transaction_rows)} transactions")

# Process each transaction
for row in transaction_rows:
    try:
        # Extract date
        fecha_cell = row.find('td', class_='fecha-cell')
        if fecha_cell:
            date_text = fecha_cell.get_text(strip=True)
            formatted_date = parse_date(date_text)
        else:
            continue
        
        # Extract category/merchant
        categoria_cell = row.find('td', class_='categoria-cell')
        if categoria_cell:
            merchant_name = extract_merchant_name(categoria_cell)
        else:
            merchant_name = "Unknown"
        
        # Extract account information
        service_cell = row.find('td', class_='activities__cell_service')
        if service_cell:
            account_info = extract_account_info(service_cell)
        else:
            account_info = "Unknown"
        
        # Extract amount
        precio_cell = row.find('td', class_='precio-cell')
        if precio_cell:
            amount_text = precio_cell.get_text(strip=True)
            amount = parse_amount(amount_text)
        else:
            continue
        
        # Categorize the transaction
        concepto = categorize_transaction(merchant_name, amount)
        
        # Add to data list
        data.append({
            'Fecha del movimiento': formatted_date,
            'Importe': amount,
            'Concepto': concepto,
            'Comercio': merchant_name,
            'Cuenta': account_info
        })
        
    except Exception as e:
        print(f"Error processing row: {e}")
        continue

print(f"Processed {len(data)} transactions successfully")

# Create DataFrame and save to CSV
if data:
    df = pd.DataFrame(data)
    
    # Sort by date (newest first, matching the original order)
    df['date_obj'] = pd.to_datetime(df['Fecha del movimiento'], format='%d/%m/%Y')
    df = df.sort_values('date_obj', ascending=False)
    df = df.drop('date_obj', axis=1)
    
    # Save to CSV with semicolon separator (matching RuralVia format)
    df.to_csv(csv_file_path, index=False, sep=';', encoding='utf-8')
    print(f"CSV file saved successfully to: {csv_file_path}")
    print(f"Total transactions: {len(df)}")
    
    # Show sample data
    print("\nSample of processed data:")
    print(df.head(10).to_string(index=False))
else:
    print("No transaction data was extracted!")
