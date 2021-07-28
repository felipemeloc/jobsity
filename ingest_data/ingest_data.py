import src.db.config as db
import src.utils.read_data as rd
import os

def main_ingest():
    folder_path = '../../../Downloads/test'
    files = os.listdir(folder_path)
    if files != 0:
        for file_ in files:
            file_path = os.path.join(folder_path, file_)
            df = rd.read_one_file(file_path) 
            db.df_to_sql(df, 'trips')
    else:
        print('Not files to be ingest')
        return None


if __name__ == "__main__":
    main_ingest()