from flask import Flask, render_template, request, jsonify
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

app = Flask(__name__)

def setup_google_sheets():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = Credentials.from_service_account_file(
        'credentials.json',
        scopes=scopes
    )
    
    return gspread.authorize(creds)

def get_sheet_data(spreadsheet_url, sheet_name=None, page=1, per_page=50):
    try:
        client = setup_google_sheets()
        spreadsheet = client.open_by_url(spreadsheet_url)
        
        if sheet_name:
            worksheet = spreadsheet.worksheet(sheet_name)
        else:
            worksheet = spreadsheet.get_worksheet(0)
        
        # Get all values
        data = worksheet.get_all_records()
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(data)
        
        # Calculate pagination
        total_records = len(df)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        # Get paginated data
        paginated_data = df.iloc[start_idx:end_idx]
        
        return {
            'data': paginated_data.to_dict('records'),
            'total': total_records,
            'page': page,
            'per_page': per_page,
            'total_pages': (total_records + per_page - 1) // per_page
        }
    
    except Exception as e:
        return {'error': str(e)}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    spreadsheet_url = request.args.get('spreadsheet_url')
    sheet_name = request.args.get('sheet_name')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    
    data = get_sheet_data(spreadsheet_url, sheet_name, page, per_page)
    return jsonify(data)

@app.route('/api/update', methods=['POST'])
def update_data():
    try:
        data = request.json
        spreadsheet_url = data.get('spreadsheet_url')
        sheet_name = data.get('sheet_name')
        row_data = data.get('row_data')
        row_index = data.get('row_index')
        
        client = setup_google_sheets()
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name) if sheet_name else spreadsheet.get_worksheet(0)
        
        # Update the row
        worksheet.update(f'A{row_index + 2}', [list(row_data.values())])
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True) 