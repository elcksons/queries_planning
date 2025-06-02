import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

def setup_google_sheets():
    # Define the scopes
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Create credentials
    creds = Credentials.from_service_account_file(
        'credentials.json',
        scopes=scopes
    )
    
    # Create client
    client = gspread.authorize(creds)
    return client

def download_sheet(spreadsheet_url, sheet_name=None):
    try:
        # Setup client
        client = setup_google_sheets()
        
        # Open the spreadsheet
        spreadsheet = client.open_by_url(spreadsheet_url)
        
        # If sheet_name is provided, get that specific sheet, otherwise get the first sheet
        if sheet_name:
            worksheet = spreadsheet.worksheet(sheet_name)
        else:
            worksheet = spreadsheet.get_worksheet(0)
        
        # Get all values
        data = worksheet.get_all_records()
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(data)
        
        # Save to CSV
        output_file = 'downloaded_sheet.csv'
        df.to_csv(output_file, index=False)
        print(f"Planilha baixada com sucesso e salva como {output_file}")
        
        return df
    
    except Exception as e:
        print(f"Erro ao baixar a planilha: {str(e)}")
        return None

if __name__ == "__main__":
    # URL da sua planilha do Google Sheets
    SPREADSHEET_URL = "COLOQUE_AQUI_A_URL_DA_SUA_PLANILHA"
    
    # Nome da aba (opcional)
    SHEET_NAME = None  # Deixe como None para usar a primeira aba
    
    # Baixar a planilha
    df = download_sheet(SPREADSHEET_URL, SHEET_NAME) 