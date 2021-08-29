import requests
import pandas as pd
import csv
import argparse
import urllib.request

import os
import sys
sys.path.append('../utils')

import utils as utils
import utils_api as api

from datetime import datetime


def get_readme_by_repo(token, metadata_path, language, token_key, filter_list=None):
    # main path of files
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler')

    # path to save crawling files
    crawler_path = os.path.join(main_path, 'readme', language.lower(), 'crawler_files')
    
    #csv file with all the repositories
    year = '2019'
    filename = f"cleaned_{language}_{year}.csv"

    # file to read all repositories for the language
    repositories_path = os.path.join(main_path, 'projects', filename)
    repositories_df = pd.read_csv(repositories_path)

    # select just the necessary columns
    repositories_df = repositories_df[['id', 'full_name', 'updated_at']]

    repositories_df['readme_location'] = repositories_df['full_name'].apply(lambda x: f"https://github.com/{x}")

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

    for index, row in repositories_df.iterrows():
        # original query for the repository
        base_query = f"{row['readme_location']}"

        print(f"Requesting readme file location for repository {row['full_name']} - updated at {row['updated_at']}")

        file_crawler_path = os.path.join(crawler_path, f"{row['full_name'].replace('/', '_')}")

        html_page = retrieve_html_page(token, base_query, token_key, language)

        if html_page:
            save_result_query(html_page, file_crawler_path)
        # log progress
        save_progress_metadata(metadata_path, language, row['id'], row['full_name'], 
                                row['updated_at'], base_query)

def save_result_query(data, file_crawler_path):
    with open(str(file_crawler_path+'.txt'), 'a', encoding='utf-8') as fo:
        fo.write(data)
        fo.close()

def retrieve_html_page(token, query, token_key, language):
    html = ''
    api.verify_request_time(token, 'core')
    try:
        html = requests.get(query, headers={'Authorization': 'token %s' % token}).text
    except:
        try:
            api.verify_request_time(token, 'core')
            request = urllib.request.Request(query)
            request.add_header("Authorization", "token %s" % token)
            result = urllib.request.urlopen(request)
            html = result.read()
        except:
            print('INFO: Repository not found or access blocked!')
            save_repositories_not_found(query, language, token_key)

    return html


def save_progress_metadata(file_name, language, repo_id, repo_full_name, updated_at, complete_query, separator=','):
    with open(file_name, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)
        
        csv_file.writerow([str(datetime.now()),
                           language, repo_id, repo_full_name, updated_at, complete_query])

    a.close()

def save_repositories_not_found(query, language, token_key, separator=','):
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'readme', language.lower())
    file_name = os.path.join(main_path, 'crawling_readme_repositories_not_found.csv')

    with open(file_name, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)
        csv_file.writerow([str(datetime.now()), query])
    a.close()

def create_progress_file(in_progress, language, token_key, separator=','):
    main_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'readme', language.lower())

    file_name = os.path.join(main_path, 'crawling_readme_metadata.csv')
    
    filter_list = None

    if not in_progress:
        # create the metadata file with the header
        with open(file_name, mode='w', newline='') as w:
            csv_file = csv.writer(w, delimiter=separator)
            csv_file.writerow(['log_date', 'language', 'id', 'full_name', 'updated_at', 'complete_query'])
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
    parser = argparse.ArgumentParser(description='Readme files collector from Github')
    parser.add_argument('-t', '--token', 
                        help='The Github token identifier to crawling data', required=True)
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('--cont', default='false',
                        help='Use this param with True value to continue a started crawling in a specific language',
                        required=False)

    args = parser.parse_args()
    
    # Print start time processing
    start_time = datetime.now()
    print('Crawling stated at', start_time)

    # Get token by key
    token = utils.get_token_key(args.token)
    print(f'Token successfully obtained using token key {args.token}\n')

    # Recover the metadata file
    in_progress = args.cont.lower() == 'true'
    metadata_file_name, filter_list = create_progress_file(in_progress, args.language, args.token)

    # Create the search query using the given params and save the results
    get_readme_by_repo(token, metadata_file_name, args.language, args.token, filter_list)
    
    # Print finish time processing
    end_time = datetime.now()
    print('Crawling finished at', end_time)

    print('\nCrawling finished in', end_time - start_time)

if __name__ == '__main__':
    main()