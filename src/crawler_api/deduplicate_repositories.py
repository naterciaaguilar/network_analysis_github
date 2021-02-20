import argparse
from datetime import datetime

import pandas as pd

import os
import sys
sys.path.append('../utils')

import utils as utils

def read_all_files(main_path):
    folder_path = os.path.join(main_path, 'daily_crawler')

    repositories_df = pd.DataFrame()

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        print('Reading file', file_name)

        file_df = pd.read_csv(file_path, sep=',')

        repositories_df = repositories_df.append(file_df, ignore_index=True)

    return repositories_df

def deduplicate_repositories(repositories_df):
    return repositories_df.drop_duplicates(subset=['id', 'full_name'], keep='last')

def save_deduplicated_file(main_path, deduplicated_df, filtered=False):
    file_path = os.path.join(main_path, 'deduplicated_data', 'complete_repositories.csv')
    print_msg = '\nDeduplicated file saved on'

    if filtered:
        # if filtered, rename the previously saved file with the sufix _all_dates
        os.rename(file_path, file_path.replace('complete_repositories.csv', 'complete_repositories_all_dates.csv'))
        print_msg = '\nFiltered file saved on'

    deduplicated_df.to_csv(file_path, index=False)

    print(print_msg, file_path)

def main():    
    parser = argparse.ArgumentParser(description='Deduplicate crawled repositories for a language')
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('--start_date', default=None, 
                        help='The start date for filtering (if necessary)', required=False)
    parser.add_argument('--end_date', default=None, 
                        help='The end date for filtering (if necessary)', required=False)
    
    args = parser.parse_args()
    
    # Print start time processing
    start_time = datetime.now()
    print(f'Job stated at {start_time}\n')  

    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'repositories', args.language.lower())

    # Read all the files, concat repositories and deduplicate them
    repositories_df = read_all_files(main_path)
    deduplicated_repositories_df = deduplicate_repositories(repositories_df)

    # Print processed data
    print('\nShape of complete dataframe:', repositories_df.shape)
    print('Shape of deduplicated dataframe:', deduplicated_repositories_df.shape)

    # Save the deduplicated file
    save_deduplicated_file(main_path, deduplicated_repositories_df)

    # Filter the dataframe if necessary
    if args.start_date and args.end_date:
        # it is not necessary filter the end_date because the filter will be applyed on commits
        deduplicated_repositories_df = deduplicated_repositories_df[deduplicated_repositories_df['updated_at'] >= args.start_date]
        print('\nShape of filtered dataframe:', deduplicated_repositories_df.shape)

        # Save the deduplicated file after filtering
        save_deduplicated_file(main_path, deduplicated_repositories_df, filtered=True)

    # Print finish time processing
    end_time = datetime.now()
    print(f'\nJob finished at {end_time}\n')

    print('>> Job finished in', end_time - start_time, '<<')

if __name__ == '__main__':
    main()