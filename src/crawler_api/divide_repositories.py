import argparse
from datetime import datetime

import pandas as pd

import os
import sys
sys.path.append('../utils')

import utils as utils

def divide_repositories_file(main_path, language, rep_size, filter_list, separator=','):
    repositories_path = os.path.join(main_path, 'repositories', language.lower(), 'deduplicated_data')

    input_file_path = os.path.join(repositories_path, 'complete_repositories.csv')
    repositories_df = pd.read_csv(input_file_path)

    print('Total repositories for the language:', len(repositories_df))

    # if filter list is not null, the crawling list needs to be filtered
    if filter_list:
        repositories_df = repositories_df[~repositories_df['id'].isin(filter_list)]

    repositories_to_be_divided = len(repositories_df)
    print('Repositories to be divided:', repositories_to_be_divided, '\n')

    # divide into small files with 10.000 (default) repositories
    partitions = range(0, repositories_to_be_divided, rep_size)
    part_number = 1

    for part in partitions:
        start = part
        finish = part + rep_size
        if finish >= repositories_to_be_divided:
            finish = repositories_to_be_divided

        rep_partition_df = repositories_df.iloc[start:finish]

        print(f'Saving partition {part_number} from id {start} to id {finish} - size: {len(rep_partition_df)}')

        output_file_path = os.path.join(repositories_path, 'partitions', f'complete_repositories_part_{part_number}.csv')
        rep_partition_df.to_csv(output_file_path, sep=',', index=False)

        part_number += 1

def read_existing_log_metadata(main_path, language, separator=','):
    file_name = os.path.join(main_path, 'commits', language.lower(), f'crawling_commits_metadata.csv')
    
    filter_list = None

    # open the file and get the list of repositories
    log_file_df = pd.read_csv(file_name, sep=separator)
    filter_list = log_file_df['id'].unique().tolist()

    return filter_list

def main():    
    parser = argparse.ArgumentParser(description='Divide the main file of repositores into small ones')
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('--part-size', default=10000, 
                        help='Number of repositories in each new file', required=False)
    parser.add_argument('--ignore', default=True,
                        help='Ignore files already collected due the log metadata')
    
    args = parser.parse_args()
    
    # Print start time processing
    start_time = datetime.now()
    print(f'Job stated at {start_time}\n')  

    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler')

    repositories_list = []

    if args.ignore.lower() == 'true':
        repositories_list = read_existing_log_metadata(main_path, args.language)

    # Read the main file and divide it into partitions
    divide_repositories_file(main_path, args.language, int(args.part_size), repositories_list)

    # Print finish time processing
    end_time = datetime.now()
    print(f'\nJob finished at {end_time}\n')

    print('>> Job finished in', end_time - start_time, '<<')

if __name__ == '__main__':
    main()