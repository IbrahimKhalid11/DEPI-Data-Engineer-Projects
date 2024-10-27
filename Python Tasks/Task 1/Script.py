import argparse
import pandas as pd
import json
import os
import time
from urllib.parse import urlparse

def clean(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

def main():
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inp', required=True)
    parser.add_argument('-o', '--out', required=True)
    parser.add_argument('-u', '--u', action='store_true')
    
    args = parser.parse_args()
    input_path = args.inp
    output_path = args.out
    u = args.u
    
    files = [file for file in os.listdir(input_path) if file.endswith('.json')]

    for file in files:
        file_path = os.path.join(input_path, file)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f.readlines()]
        
        print(f'{len(data)} records found in:\n\t {file_path}')
        
        df = pd.DataFrame(data)
        
        df['browser'] = df['a'].str.extract(r'([A-Za-z]+\/[0-9.]+)').fillna('')
        df['operating_sys'] = df['a'].str.extract(r'([A-Za-z]+ [A-Za-z]+ [0-9.]+)').fillna('')
        
        df['from_url'] = df['r'].apply(lambda x: clean(x) if pd.notna(x) else '')
        df['to_url'] = df['u'].apply(lambda x: clean(x) if pd.notna(x) else '')
        
        df['city'] = df['cy'].fillna('')
        df['latitude'] = df['ll'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        df['longitude'] = df['ll'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
        df['time_zone'] = df['tz'].fillna('')
        
        if u:
            df['time_in'] = df['t']
            df['time_out'] = df['hc']
        else:
            df['time_in'] = pd.to_datetime(df['t'], unit='s', errors='coerce')
            df['time_out'] = pd.to_datetime(df['hc'], unit='s', errors='coerce')
        
        columns_to_drop = ['a', 'c', 'nk', 'tz', 'gr', 'g', 'h', 'l', 'al', 'hh', 'r', 'u', 't', 'hc', 'cy', 'll']
        df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)
        
        df.fillna(value={'latitude': 0, 'longitude': 0}, inplace=True)
        
        df.drop_duplicates(inplace=True)
        
        output_file = os.path.join(output_path, f"DataByCsvWith u={u}.csv")
        df.to_csv(output_file, index=False)
        
        print(f'Size = {len(df)} rows transformed \n and saved to:\n\t {output_file}')
    
    end = time.time()
    print(f'Time: {end - start:.2f} seconds')

if __name__ == '__main__':
    main()
