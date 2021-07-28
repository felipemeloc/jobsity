import src.db.config as db
import src.utils.read_data as rd
import src.utils.email_sender as email
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def main_ingest():
    folder_path = os.getenv('FOLDER_PATH')
    files = os.listdir(folder_path)
    if files != 0:
        for file_ in files:
            file_path = os.path.join(folder_path, file_)
            df = rd.read_one_file(file_path) 
            if type(df) == type(pd.DataFrame()):
                db.df_to_sql(df, 'trips')
                email.email_sender(df.shape[0])

    else:
        print('Not files to be ingest')
        return None


if __name__ == "__main__":
    main_ingest()