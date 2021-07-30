import src.db.config as db
import src.utils.read_data as rd
import src.utils.email_sender as email
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def main_ingest():
    folder_path = os.getenv('FOLDER_PATH')
    files = [file for file in os.listdir(folder_path) if '.csv' in file]
    if len(files) != 0:
        print("The files to be ingest ", files)
        df_status = pd.DataFrame(columns=['file','ingest','rows','date', 'error'])
        row = 0
        for file_ in files:
            file_path = os.path.join(folder_path, file_)
            df, status, error, date = rd.read_one_file(file_path, file_)
            if not df.empty:
                db.df_to_sql(df, 'trips')
            df_status.loc[row] = [file_, status, df.shape[0], date, error]
            row += 1
        email.email_sender(df_status)
    else:
        print('Not files to be ingest')
        return None


if __name__ == "__main__":
    main_ingest()