import requests
import json
import pandas as pd
import math
import csv
import argparse

import os
import sys
sys.path.append('../utils')

import utils as utils
import utils_api as api

from datetime import datetime

def get_repositories_by_time(token, metadata_path, language, start_date, year, end_date=None):
    q_language = f'%3A\"{language}\"'
    q_per_page = '&per_page=100'

    # path to save crawling files
    crawler_path = os.path.join(utils.get_main_path(), 'data', 'crawler', 'repositories', language.lower(), 'daily_crawler')

    #search for single year
    q_creation_date = f'%3A{year}-01-01..{year}-12-31'

    # original query based on language and stars
    # base_query = f'https://api.github.com/search/repositories?q=stars%3A>1+created{q_creation_date}+language{q_language}'
    base_query = f'https://api.github.com/search/repositories?q=stars%3A>5+created{q_creation_date}+language{q_language}'

    if not end_date:
        # if end date is not given, use the current date
        end_date = datetime.now().strftime('%Y-%m-%d')

    # get all the dates for crawling
    days = list(pd.date_range(start_date, end_date, freq='d'))
    str_days = [d.strftime('%Y-%m-%d') for d in days]

    for date in str_days:
        q_date = f'%3A{date}'

        date_query = base_query + f'+pushed{q_date}'

        r_date = requests.get(date_query, headers={'Authorization': 'token %s' % token}) 
        # r_date = requests.get(base_query, headers={'Authorization': 'token %s' % token}) 
        data = json.loads(r_date.content)
        
        total_count = data['total_count']

        print(f'Requesting repositories for {date} - {total_count} results')
        # quit()
        # verify the request time from API
        api.verify_request_time(token, 'search')

        if total_count <= 1000:
            page = 1
            while data['items'] and page <= 10:
                print(f'Requesting repositories for {date} - page {page}')

                q_page = f'&page={page}'
                complete_query = date_query + q_per_page + q_page

                file_crawler_path = os.path.join(crawler_path, f'{language.lower()}_{date}_{year}_{page}.csv')

                data = save_result_query(token, complete_query, file_crawler_path)
                
                # log progress
                save_progress_metadata(metadata_path, language, 0, f'{year}-01-01', date, page,
                                       data['total_count'], data['incomplete_results'], complete_query)

                page = page + 1

        else:
            month_groups = {1: ('01-01', '01-31'),
                            2: ('02-01', '03-31'),
                            3: ('04-01', '04-30'),
                            4: ('05-01', '06-30'),
                            5: ('07-01', '08-31'),
                            6: ('09-01', '10-31'),
                            7: ('11-01', '11-30'),
                            8: ('12-01', '12-31')}
            for month in month_groups:
                q_creation_date = f'%3A{year}-{month_groups[month][0]}..{year}-{month_groups[month][1]}'

                # new partitions by creation date
                monthly_date_query = date_query.replace(f'+created%3A{year}-01-01..{year}-12-31', f'+created{q_creation_date}')

                r_date = requests.get(monthly_date_query, headers={'Authorization': 'token %s' % token}) 
                data = json.loads(r_date.content)
                total_count = data['total_count']

                print(f'Requesting repositories for {date} and creation year {year} - monthly division {month} - {total_count} results')

                # verify the request time from API
                api.verify_request_time(token, 'search')

                page = 1
                while data['items'] and page <= 10:
                    print(f'Requesting repositories for {date} and creation year {year} - monthly division {month} - page {page}')

                    q_page = f'&page={page}'
                    monthly_complete_query = monthly_date_query + q_per_page + q_page
                    
                    monthly_file_crawler_path = os.path.join(crawler_path, f'{language.lower()}_{date}_{year}_{month}_{page}.csv')

                    data = save_result_query(token, monthly_complete_query, monthly_file_crawler_path)
                    
                    # log progress
                    save_progress_metadata(metadata_path, language, 0, 
                                            f'{year}-{month_groups[month][0]}..{year}-{month_groups[month][1]}', 
                                            date, page, data['total_count'], data['incomplete_results'], monthly_complete_query)

                    page = page + 1

def save_result_query(token, query, file_name):
    result = requests.get(query, headers={'Authorization': 'token %s' % token}) 
    data = json.loads(result.content)

    try:
        total_count = data['total_count']
        
        if total_count > 1000:
            print('!!! ERROR !!!')
            print('Crawler was stopped because the total count is greater then 1000')
            print('Please, look for another way of partition')
            sys.exit()

        if data['items']:
            items = pd.DataFrame(data['items'])
            items.to_csv(file_name, sep=',', index=False)
    except:
        print('Error to fetch data from request API')
        print(data)
        sys.exit()

    # verify the request time from API
    api.verify_request_time(token, 'search')

    return data

def save_progress_metadata(file_name, language, stars, created_at, updated_at, page, total_count,
                           incomplete_results, complete_query, separator=','):
    with open(file_name, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)
        
        csv_file.writerow([str(datetime.now()),
                           language, stars, created_at, updated_at,
                           page, total_count, incomplete_results, complete_query])

    a.close()

def create_progress_file(in_progress, language, separator=','):
    file_name = os.path.join(utils.get_main_path(), 'data', 'crawler', 'repositories', language.lower(), f'crawling_repositories_metadata.csv')
    
    last_updated_date = None

    if not in_progress:
        # create the metadata file with the header
        with open(file_name, mode='w', newline='') as w:
            csv_file = csv.writer(w, delimiter=separator)
            csv_file.writerow(['log_date', 'language', 'stars', 'created_at', 'updated_at', 'page', 
                               'total_count', 'incomplete_results', 'complete_query'])
        w.close()
    else:
        # open the file and get the last date crawled
        # the logs for that date have to be excluded
        log_file_df = pd.read_csv(file_name, sep=separator)
        last_updated_date = max(log_file_df['updated_at'])

        log_file_df = log_file_df[log_file_df['updated_at'] != last_updated_date]
        log_file_df.to_csv(file_name, sep=',', index=False)

    return file_name, last_updated_date

def main():    
    parser = argparse.ArgumentParser(description='Repositories collector from Github')
    parser.add_argument('-t', '--token', 
                        help='The Github token identifier to crawling data', required=True)
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('-d', '--date', default='2019-01-01', 
                        help='The start date for crawling (format: YYYY-MM-DD)', required=True)
    parser.add_argument('-y', '--year', default='2019', 
                        help='The repository creation year (format: YYYY)', required=True)
    parser.add_argument('--cont', default=False,
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
    metadata_file_name, last_updated_date = create_progress_file(in_progress, args.language)

    # Replace the date when last_updated_date is not null
    crawling_date = last_updated_date if last_updated_date else args.date

    # Create the search query using the given params and save the results
    get_repositories_by_time(token, metadata_file_name, args.language, crawling_date, args.year)
    
    # Print finish time processing
    end_time = datetime.now()
    print('Crawling finished at', end_time)

    print('\nCrawling finished in', end_time - start_time)

if __name__ == '__main__':
    main()