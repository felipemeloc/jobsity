import pandas as pd
import shutil
import sys
import os


clean_coord = lambda x : x.split('(')[1].split(')')[0].replace(' ',',')


def read_one_file(path, file):
    """read_one_file: function for read raw data and clean it

    Args:
        path (str): Path to the folder with the data to be ingest

    Returns:
        pd.DataFrame: Data Frame of the read file
        str : status of ingest process
        str : description incase of error
        pd.TimeStamp : pandas._libs.tslibs.timestamps.Timestamp
    """
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None
        df['datetime'] = pd.to_datetime(df['datetime'])
        for col in ['origin_coord', 'destination_coord']:
            df[col] = df[col].apply(clean_coord)
        df['origin_lon'] = df['origin_coord'].apply(lambda x: x.split(',')[0]).astype(float)
        df['origin_lat'] = df['origin_coord'].apply(lambda x: x.split(',')[1]).astype(float)
        df['destination_lon'] = df['destination_coord'].apply(lambda x: x.split(',')[0]).astype(float)
        df['destination_lat'] = df['destination_coord'].apply(lambda x: x.split(',')[1]).astype(float)
        df = df[['region','origin_lon','origin_lat','destination_lon','destination_lat','datetime','datasource']]
        today = pd.Timestamp.today()
        backup_path = os.getenv('FOLDER_BACKUP_PATH')
        name = today.strftime('%Y%m%d_%H%M%s')
        file = f'{name}.csv'
        backup_file = os.path.join(backup_path, file)
        shutil.copyfile(path, backup_file)
        print(f"The input file moved to {backup_file}")
        os.remove(path)
        print(F"The {file} has been removed")
        return df, 'successfully','NA', today
    except:
        error = str(sys.exc_info()[1])
        return pd.DataFrame(), 'unsuccessfully', error, pd.Timestamp.today()