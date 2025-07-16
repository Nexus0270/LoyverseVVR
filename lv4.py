# Install required packages if not already installed
try:
    import requests
    import pandas as pd
    from datetime import datetime
except ImportError as e:
    import subprocess
    import sys
    
    # Map import names to package names
    package_map = {
        'requests': 'requests',
        'pandas': 'pandas',
        'datetime': 'datetime'  # datetime is part of standard library
    }
    
    missing_packages = []
    for package in str(e).split("'")[1::2]:
        if package in package_map and package != 'datetime':
            missing_packages.append(package_map[package])
    
    if missing_packages:
        print(f"Installing missing packages: {', '.join(missing_packages)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
    
    # Now try imports again
    import requests
    import pandas as pd
    from datetime import datetime

# Rest of your original script follows...
# Configuration
API_BASE_URL = "https://api.loyverse.com/v1.0"
API_TOKEN = "-"  # Token from Loyverse
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

def get_all_data(endpoint, params=None):
    """Generic function to fetch all paginated data from an endpoint"""
    all_data = []
    url = f"{API_BASE_URL}/{endpoint}"
    
    while url:
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()  # Will raise HTTPError for 4XX/5XX status
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                all_data.extend(data)
            elif endpoint in data:  # e.g., 'categories' in response
                all_data.extend(data.get(endpoint, []))
                url = f"{API_BASE_URL}/{endpoint}?cursor={data['cursor']}" if data.get('cursor') else None
            elif 'cursor' in data:  # Some endpoints return cursor directly
                all_data.extend(data.get('items', []))
                url = f"{API_BASE_URL}/{endpoint}?cursor={data['cursor']}" if data.get('cursor') else None
            else:
                all_data.append(data)
                url = None
                
            # Reset params after first request as cursor is in URL
            params = None
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error for {endpoint}: {e}")
            break
        except Exception as e:
            print(f"Error fetching {endpoint}: {e}")
            break
            
    return all_data

def flatten_receipt_payments(receipts):
    """Flatten payments field and add as separate columns"""
    flattened_data = []
    
    for receipt in receipts:
        # Create a base record with receipt informationth receipt information
        base_record = receipt.copy()
        
        # Flatten payments data
        if 'payments' in base_record:
            for i, payment in enumerate(base_record['payments']):
                # Payment detail as a separate record
                payment_record = base_record.copy()
                payment_record.update({
                    'payment_type_id': payment.get('payment_type_id'),
                    'payment_name': payment.get('name'),
                    'payment_type': payment.get('type'),
                    'money_amount': payment.get('money_amount'),
                    'paid_at': payment.get('paid_at'),
                    'payment_details': payment.get('payment_details')
                })
                flattened_data.append(payment_record)
        else:
            flattened_data.append(base_record)
    
    return flattened_data

def flatten_receipt_line_items(receipts):
    """Flatten line_items field and add as separate columns"""
    flattened_data = []
    
    for receipt in receipts:
        # Create a base record
        base_record = receipt.copy()
        
        # Flatten line_items data
        if 'line_items' in base_record:
            for i, line_item in enumerate(base_record['line_items']):
                # Add each line_item detail as a separate record
                line_item_record = base_record.copy()
                line_item_record.update({
                    'line_item_id': line_item.get('id'),
                    'item_id': line_item.get('item_id'),
                    'variant_id': line_item.get('variant_id'),
                    'item_name': line_item.get('item_name'),
                    'variant_name': line_item.get('variant_name'),
                    'sku': line_item.get('sku'),
                    'quantity': line_item.get('quantity'),
                    'price': line_item.get('price'),
                    'gross_total_money': line_item.get('gross_total_money'),
                    'total_money': line_item.get('total_money'),
                    'cost_total': line_item.get('cost_total'),
                    'line_note': line_item.get('line_note'),
                    'line_taxes': line_item.get('line_taxes'),
                    'total_discount': line_item.get('total_discount'),
                    'line_discounts': line_item.get('line_discounts'),
                    'line_modifiers': line_item.get('line_modifiers')
                })
                flattened_data.append(line_item_record)
        else:
            flattened_data.append(base_record)
    
    return flattened_data

def calculate_payment_totals(receipt_data):
    """Calculate total money spent for each payment type, accounting for SALE and REFUND receipt types"""
    payment_totals = {}
    
    # Declare payment_types names
    payment_types = ["QR Maybank", "Shopeefood", "Sedekah", "FoodPanda", "Grabfood", "Cash"]
    for payment_type in payment_types:
        payment_totals[payment_type] = 0
    
    # Calculate totals by payment_type
    for receipt in receipt_data:
        payment_name = receipt.get('payment_name')
        receipt_type = receipt.get('receipt_type', 'SALE')  # Default to SALE if not specified
        
        # Get monetary values
        total_money = float(receipt.get('total_money', 0))
        money_amount = float(receipt.get('money_amount', 0))
        
        # Use money_amount if available, otherwise fall back to total_money
        amount = money_amount if money_amount > 0 else total_money
        
        # Apply the sign based on receipt_type
        if receipt_type == 'REFUND':
            amount = -amount  # Subtract for refunds
        
        if payment_name in payment_totals:
            payment_totals[payment_name] += amount
    
    # Add payment totals back to each record
    for receipt in receipt_data:
        payment_name = receipt.get('payment_name')
        if payment_name in payment_totals:
            receipt['payment_total_by_type'] = payment_totals[payment_name]
    
    return receipt_data

def calculate_top_items(line_items_data, top_n=5):
    """Calculate top items by sales, accounting for receipt type"""
    # Create a DataFrame from the line items data
    df = pd.DataFrame(line_items_data)
    
    # Ensure necessary columns exist
    if not all(col in df.columns for col in ['item_name', 'receipt_type', 'total_money', 'quantity']):
        print("Warning: Required columns for top items calculation not found")
        return pd.DataFrame(columns=['Item Name', 'Total Sales', 'Total Quantity'])
    
    # Convert total_money and quantity to numeric
    df['total_money'] = pd.to_numeric(df['total_money'], errors='coerce').fillna(0)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    
    # Adjust values based on receipt type (negative for refunds)
    df['adjusted_total_money'] = df.apply(
        lambda row: -row['total_money'] if row.get('receipt_type') == 'REFUND' else row['total_money'],
        axis=1
    )
    
    df['adjusted_quantity'] = df.apply(
        lambda row: -row['quantity'] if row.get('receipt_type') == 'REFUND' else row['quantity'],
        axis=1
    )
    
    # Group by item_name
    item_summary = df.groupby('item_name').agg({
        'adjusted_total_money': 'sum',
        'adjusted_quantity': 'sum'
    }).reset_index()
    
    # Rename columns
    item_summary.columns = ['Item Name', 'Total Sales', 'Total Quantity']
    
    # Sort by Total Sales (descending) and get top N
    top_items = item_summary.sort_values('Total Sales', ascending=False).head(top_n)
    
    return top_items

def process_shifts_data(shifts_data):
    """Process shifts data to extract paid_out information by date"""
    # Create a DataFrame from the shifts data
    if not shifts_data:
        return pd.DataFrame(columns=['Date', 'Paid Out'])
    
    shifts_df = pd.DataFrame(shifts_data)
    
    # Extract date from opened_at field as specified in the API
    if 'opened_at' in shifts_df.columns:
        shifts_df['date_only'] = pd.to_datetime(shifts_df['opened_at']).dt.date
    else:
        # Fallback to other date fields if opened_at is not available
        date_fields = ['created_at', 'ended_at', 'updated_at']
        for field in date_fields:
            if field in shifts_df.columns:
                shifts_df['date_only'] = pd.to_datetime(shifts_df[field]).dt.date
                break
        else:
            # If no date fields available, create an empty date column
            shifts_df['date_only'] = None
    
    # Extract paid_out information directly from the specified field
    if 'paid_out' in shifts_df.columns:
        # Convert paid_out to float to ensure proper calculations
        shifts_df['paid_out'] = shifts_df['paid_out'].astype(float)
        paid_out_by_date = shifts_df.groupby('date_only')['paid_out'].sum().reset_index()
    else:
        # Fallback if paid_out field is not found or nested differently
        print("Warning: 'paid_out' field not found in shifts data")
        paid_out_by_date = pd.DataFrame({'date_only': shifts_df['date_only'].unique(), 'paid_out': 0})
    
    # Rename columns for consistency
    paid_out_by_date.columns = ['Date', 'Paid Out']
    return paid_out_by_date

def calculate_sales_metrics(df):
    """Calculate gross sales, net sales, and their difference from receipt data"""
    # Ensure all required fields are available
    if not df.empty and 'receipt_type' in df.columns and 'total_money' in df.columns:
        # Convert total_money to float for calc
        df['total_money'] = pd.to_numeric(df['total_money'], errors='coerce').fillna(0)
        
        # Calc gross sales (sum of sales)
        sales_only = df[df['receipt_type'] == 'SALE']
        gross_sales = sales_only['total_money'].sum()
        
        # Calc net sales (sales - refunds)
        refunds_only = df[df['receipt_type'] == 'REFUND']
        refunds_total = refunds_only['total_money'].sum()
        net_sales = gross_sales - refunds_total
        
        # Calc difference
        difference = gross_sales - net_sales
        
        return {
            'total_gross_sales': gross_sales,
            'total_net_sales': net_sales,
            'sales_difference': difference
        }
    else:
        return {
            'total_gross_sales': 0,
            'total_net_sales': 0, 
            'sales_difference': 0
        }

def export_to_excel(data_dict, filename="loyverse_data.xlsx"):
    """Export multiple datasets to different Excel sheets"""
    # Filter out empty datasets
    data_dict = {k: v for k, v in data_dict.items() if v}
    
    if not data_dict:
        print("No data to export - all API calls failed")
        return False
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Process shifts data if available
            shifts_df = pd.DataFrame()
            if 'shifts' in data_dict:
                shifts_data = data_dict['shifts']
                shifts_df = pd.DataFrame(shifts_data)
                shifts_df.to_excel(writer, sheet_name='shifts', index=False)
                
                # Process paid_out data
                paid_out_by_date = process_shifts_data(shifts_data)
            else:
                paid_out_by_date = pd.DataFrame(columns=['Date', 'Paid Out'])
            
            # Calculate top 5 items if line items data is available
            top_items_df = pd.DataFrame(columns=['Item Name', 'Total Sales', 'Total Quantity'])
            if 'receipt_items' in data_dict:
                top_items_df = calculate_top_items(data_dict['receipt_items'], top_n=5)
            
            # Process other datasets
            for sheet_name, data in data_dict.items():
                if sheet_name == 'shifts':
                    continue  # Already processed
                
                df = pd.DataFrame(data)
                
                # Add summary sheet for receipt data
                if sheet_name == "receipt_payments" and 'payment_name' in df.columns and 'receipt_date' in df.columns:
                    # Process receipt date to extract just the date part (without time)
                    df['date_only'] = pd.to_datetime(df['receipt_date']).dt.date
                    
                    # Create summary DataFrame of payment totals
                    # Calculate adjusted money_amount considering receipt_type
                    df['adjusted_money_amount'] = df.apply(
                        lambda row: -float(row.get('money_amount', 0))
                        if row.get('receipt_type') == 'REFUND'
                        else float(row.get('money_amount', 0)),
                        axis=1
                    )
                    # Now group by payment_name with adjusted amounts
                    payment_summary = df.groupby('payment_name')['adjusted_money_amount'].sum().reset_index()
                    payment_summary.columns = ['Payment Method', 'Net Total Amount']
                    
                    # Create summary DataFrame of daily sales totals, accounting for SALE and REFUND types
                    # Copy total_money to a new column that will have the correct sign based on receipt_type
                    df['adjusted_total_money'] = df.apply(
                        lambda row: -float(row.get('total_money', 0)) 
                        if row.get('receipt_type') == 'REFUND' 
                        else float(row.get('total_money', 0)), 
                        axis=1
                    )
                    daily_sales = df.groupby('date_only')['adjusted_total_money'].sum().reset_index()
                    daily_sales.columns = ['Date', 'Sales']
                    
                    # Calculate max rows for the first three sections
                    max_rows = max(len(payment_summary), len(daily_sales), len(paid_out_by_date))
                    payment_summary = payment_summary.reindex(range(max_rows))
                    daily_sales = daily_sales.reindex(range(max_rows))
                    paid_out_by_date = paid_out_by_date.reindex(range(max_rows))
                    
                    # Calculate sales metrics (gross sales, net sales, and difference)
                    sales_metrics = calculate_sales_metrics(df)
                    
                    # Create combined summary with empty columns
                    combined_summary = pd.DataFrame({
                        'Payment Method': payment_summary['Payment Method'],
                        'Net Total Amount': payment_summary['Net Total Amount'],
                        'Empty1': [None] * max_rows,  # First empty column
                        'Date': daily_sales['Date'],
                        'Sales': daily_sales['Sales'],
                        'Empty2': [None] * max_rows,  # Second empty column
                        'Date_PaidOut': paid_out_by_date['Date'],
                        'Paid Out': paid_out_by_date['Paid Out'],
                        'Empty3': [None] * max_rows,  # Third empty column
                        'Top Item': [None] * max_rows,
                        'Total Sales': [None] * max_rows,
                        'Total Quantity': [None] * max_rows,
                        'Empty4': [None] * max_rows,  # Fourth empty column for new metrics
                        'Sales Metrics': [None] * max_rows,
                        'Amount': [None] * max_rows
                    })
                    
                    # Add the top 5 items without stretching (using the first 5 rows only)
                    num_top_items = min(5, len(top_items_df))
                    for i in range(num_top_items):
                        if i < len(top_items_df):
                            combined_summary.loc[i, 'Top Item'] = top_items_df.iloc[i]['Item Name']
                            combined_summary.loc[i, 'Total Sales'] = top_items_df.iloc[i]['Total Sales']
                            combined_summary.loc[i, 'Total Quantity'] = top_items_df.iloc[i]['Total Quantity']
                    
                    # Add labels for metrics
                    metrics_labels = ['Total Gross Sales', 'Total Net Sales', 'Gross-Net Difference']
                    metrics_values = [
                        sales_metrics['total_gross_sales'],
                        sales_metrics['total_net_sales'],
                        sales_metrics['sales_difference']
                    ]
                    
                    for i, (label, value) in enumerate(zip(metrics_labels, metrics_values)):
                        combined_summary.loc[i, 'Sales Metrics'] = label
                        combined_summary.loc[i, 'Amount'] = value
                    
                    # Write main data and summaries to different sheets
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                    combined_summary.to_excel(writer, sheet_name="Summary", index=False)
                else:
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
        return True
    except Exception as e:
        print(f"Error exporting to Excel: {e}")
        return False

def main():
    print("Starting Loyverse data extraction...")
    
    # Define endpoint to extract
    endpoints = {
        "receipts": "receipts",
        "shifts": "shifts"
    }
    
    all_data = {}
    
    for name, endpoint in endpoints.items():
        print(f"Fetching {name} data...")
        data = get_all_data(endpoint)
        
        if name == "receipts":
            # Store original receipts
            receipts = data
            
            # Flatten payments
            payments = flatten_receipt_payments(receipts)
            payments = calculate_payment_totals(payments)
            
            # Flatten line items
            line_items = flatten_receipt_line_items(receipts)
            
            # Store separately
            all_data["receipt_payments"] = payments
            all_data["receipt_items"] = line_items
            continue  # skip adding to all_data[name]
        
        all_data[name] = data
        print(f"Retrieved {len(data)} {name} records")
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"loyverse_export_{timestamp}.xlsx"
    
    # Export to Excel
    if export_to_excel(all_data, filename):
        print(f"Data successfully exported to {filename}")
    else:
        print("Failed to export data - check error messages above")

if __name__ == "__main__":
    main()