import requests
import json
import pandas as pd
import csv
import argparse

import os
import sys
sys.path.append('../utils')

import utils as utils
import utils_api as api

from datetime import datetime

def get_topics_by_repo(token, metadata_path, language, end_date, partition, token_key, filter_list=None):
    # main path of files
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler')

    # path to save crawling files
    crawler_path = os.path.join(main_path, 'topics', language.lower(), 'crawler_files')

    # file to read all repositories for the language
    if not partition:
        repositories_path = os.path.join(main_path, 'repositories', language.lower(), 'deduplicated_data', 'complete_repositories.csv')
    else:
        repositories_path = os.path.join(main_path, 'repositories', language.lower(), 'deduplicated_data', 'partitions', f'complete_repositories_part_{partition}.csv')

    repositories_df = pd.read_csv(repositories_path)

    # select just the necessary columns
    repositories_df = repositories_df[['id', 'full_name', 'updated_at']]

    repositories_df['topics_url'] = repositories_df['full_name'].apply(lambda x: f"https://api.github.com/repos/{x}/topics")

    print('Total repositories for the language:', len(repositories_df))

    # if filter list is not null, the crawling list needs to be filtered
    if filter_list:
        repositories_df = repositories_df[~repositories_df['id'].isin(filter_list)]

    print('Repositories to be crawled:', len(repositories_df), '\n')

    # print('Ignoring one more repository: 301835369')
    # repositories_df = repositories_df[repositories_df['id'] != 301835369]
    # print('New size:', len(repositories_df), '\n')

    # sort the dataframe by the updated date of the repository
    repositories_df = repositories_df.sort_values(by=['updated_at'])

    # quantity of topics per page
    q_per_page = '&per_page=100'

    for index, row in repositories_df.iterrows():
        # original query for the repository
        base_query = f"{row['topics_url']}?until={end_date}"

        page = 1

        # create a non empty list to start the loop
        topics = ['']

        while topics:
            print(f"Requesting topics for repository {row['full_name']} - updated at {row['updated_at']} - page {page}")

            q_page = f'&page={page}'
            complete_query = base_query + q_per_page + q_page

            file_crawler_path = os.path.join(crawler_path, f"{row['id']}_{row['full_name'].replace('/', '_')}_{page}.csv")

            topics = save_result_query(token, complete_query, file_crawler_path, partition, token_key, language, row['id'], row['full_name'], 
                                   row['updated_at'])
            
            # log progress
            save_progress_metadata(metadata_path, language, row['id'], row['full_name'], 
                                   row['updated_at'], page, complete_query)

            page = page + 1

def save_result_query(token, query, file_name, partition, token_key, language, id, full_name, updated_at):
    result = requests.get(query, headers={'Authorization': 'token %s' % token, 'Accept': 'application/vnd.github.mercy-preview+json'}) 
    topics = json.loads(result.content)
    
    if topics:
        if 'names' in topics:
            if topics['names']:
                topics_df = pd.DataFrame.from_dict(topics)
                topics_df['id'] = id
                topics_df['full_name'] = full_name
                topics_df['updated_at'] = updated_at
                topics_df.to_csv(file_name, sep=',', index=False)
            else:
                topics = []

        else:
            if 'message' in topics  and topics['message'] in ['Not Found', 'Repository access blocked']:
                print('INFO: Repository not found or access blocked!')
                save_repositories_not_found(query, language, partition, token_key)
                topics = []
            else:
                print('Return query:', topics)
                print('!!! CHECK ERROR !!!')
                sys.exit()

    # verify the request time from API
    api.verify_request_time(token, 'core')

    return topics 

def save_progress_metadata(file_name, language, repo_id, repo_full_name, updated_at, page, complete_query, separator=','):
    with open(file_name, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)
        
        csv_file.writerow([str(datetime.now()),
                           language, repo_id, repo_full_name, updated_at, page, complete_query])

    a.close()

def save_repositories_not_found(query, language, partition, token_key, separator=','):
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'topics', language.lower())
    if not partition:
        file_name = os.path.join(main_path, 'crawling_topics_repositories_not_found.csv')
    else:
        file_name = os.path.join(main_path, f'crawling_topics_repositories_not_found_part_{partition}_{token_key}.csv')

    with open(file_name, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)
        csv_file.writerow([str(datetime.now()), query])
    a.close()

def create_progress_file(in_progress, language, partition, token_key, separator=','):
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'topics', language.lower())

    if not partition:
        file_name = os.path.join(main_path, 'crawling_topics_metadata.csv')
    else:
        file_name = os.path.join(main_path, f'crawling_topics_metadata_part_{partition}_{token_key}.csv')
    
    filter_list = None

    if not in_progress:
        # create the metadata file with the header
        with open(file_name, mode='w', newline='') as w:
            csv_file = csv.writer(w, delimiter=separator)
            csv_file.writerow(['log_date', 'language', 'id', 'full_name', 'updated_at', 'page', 'complete_query'])
        w.close()
    else:
        # open the file and get the list of repositories
        # the last repository will be outside the list because it could not be finished
        # the logs for that repository have to be excluded
        log_file_df = pd.read_csv(file_name, sep=separator)
        last_repository = log_file_df.iloc[-1]['id']

        log_file_df = log_file_df[log_file_df['id'] != last_repository]
        log_file_df.sort_values(by=['log_date']).to_csv(file_name, sep=',', index=False)

        filter_list = log_file_df['id'].unique().tolist()

    return file_name, filter_list

def main():    
    parser = argparse.ArgumentParser(description='Repositories topics collector from Github')
    parser.add_argument('-t', '--token', 
                        help='The Github token identifier to crawling data', required=True)
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('--end_date', default='2020-11-30', 
                        help='The end date for crawling (format: YYYY-MM-DD)', required=True)
    parser.add_argument('--cont', default='false',
                        help='Use this param with True value to continue a started crawling in a specific language',
                        required=False)
    parser.add_argument('-p', '--partition', default=None,
                        help='If the input file is partitionated, this param gives the partition')

    args = parser.parse_args()
    
    # Print start time processing
    start_time = datetime.now()
    print('Crawling stated at', start_time)

    # Get token by key
    token = utils.get_token_key(args.token)
    print(f'Token successfully obtained using token key {args.token}\n')

    # Recover the metadata file
    in_progress = args.cont.lower() == 'true'
    metadata_file_name, filter_list = create_progress_file(in_progress, args.language, args.partition, args.token)

    # Create the search query using the given params and save the results
    get_topics_by_repo(token, metadata_file_name, args.language, args.end_date, args.partition, args.token, filter_list)
    
    # Print finish time processing
    end_time = datetime.now()
    print('Crawling finished at', end_time)

    print('\nCrawling finished in', end_time - start_time)

if __name__ == '__main__':
    main()