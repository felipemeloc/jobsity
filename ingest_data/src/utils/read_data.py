import pandas as pd
import os


clean_coord = lambda x : x.split('(')[1].split(')')[0].replace(' ',',')

def read_one_file(path):
    """read_one_file: function for read raw data and clean it

    Args:
        path (str): Path to the folder with the data to be ingest

    Returns:
        pd.DataFrame: Data Frame of the read file
    """
    df = pd.read_csv(path)
    if df.empty:
        return None
    df['datetime'] = pd.to_datetime(df['datetime'])
    for col in ['origin_coord', 'destination_coord']:
        df[col] = df[col].apply(clean_coord)
    df['origin_lon'] = df['origin_coord'].apply(lambda x: x.split(',')[0]).astype(float)
    df['origin_lat'] = df['origin_coord'].apply(lambda x: x.split(',')[1]).astype(float)
    df['destination_lon'] = df['origin_coord'].apply(lambda x: x.split(',')[0]).astype(float)
    df['destination_lat'] = df['origin_coord'].apply(lambda x: x.split(',')[1]).astype(float)
    df = df[['region','origin_lon','origin_lat','destination_lon','destination_lat','datetime','datasource']]
    return df