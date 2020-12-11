import requests
import json
import argparse
import datetime
import time
import csv

import sys
sys.path.append('../utils')

import pandas as pd

import utils as utils

def get_max_stars(token, language, start_date):
    q_date = f'%3e{start_date}'
    q_language = f'\"{language}\"'
    
    complete_query = f'https://api.github.com/search/repositories?q=language%3A{q_language}+pushed%3A{q_date}&s=stars&o=desc'
    r = requests.get(complete_query,  headers={'Authorization': 'token %s' % token})

    data = json.loads(r.content)

    total_repositories = int(data['total_count'])
    incomplete_results = bool(data['incomplete_results'])

    print(f'There are {total_repositories} repositories for {language} considering {"INCOMPLETE" if incomplete_results else "COMPLETE"} results')

    first_position = data['items'][0]
    stars = first_position['stargazers_count']

    print(f'Max stars number: {stars}\n')

    return stars

def save_stars_histogram(token, language, start_date, init_star, max_stars, stars_file_path, separator=','):
    q_date = f'%3e{start_date}'
    q_language = f'\"{language}\"'

    stars_query = f'https://api.github.com/search/repositories?q=language%3A{q_language}+pushed%3A{q_date}'

    stars_count = {}
    stars_incomplete = {}

    with open(stars_file_path, mode='a', newline='') as a:
        csv_file = csv.writer(a, delimiter=separator)

        # Add 10 to the max_stars number due to possible gain of more stars during the process
        for i in range(init_star, max_stars+10):
            print(f'Getting {language} repositories with {i} stars')

            stars_statement = f'+stars%3A{i}'
            complete_query = stars_query + stars_statement
            r = requests.get(complete_query,  headers={'Authorization': 'token %s' % token})

            data = json.loads(r.content)

            try:
                number_of_repositories = int(data['total_count'])
                incomplete_results = bool(data['incomplete_results'])
            except:
                print(data)
                sys.exit(-1)

            # save the retrieved row in file
            csv_file.writerow([i, number_of_repositories, incomplete_results])

            # verify the request time from API
            rate_limit_request = 'https://api.github.com/rate_limit'
            rate_limit = requests.get(rate_limit_request, headers={'Authorization': 'token %s' % token})
            rate_limit = json.loads(rate_limit.content)

            if rate_limit['resources']['search']['remaining'] == 0:
                now = datetime.datetime.utcnow()
                reset_time = datetime.datetime.utcfromtimestamp(rate_limit['resources']['search']['reset'])

                wait_seconds = (reset_time - now).total_seconds()

                # wait the reset time to continue
                if wait_seconds > 0:
                    print(f'\nSleeping {wait_seconds} until continue...\n')
                    time.sleep(wait_seconds + 1)
    a.close()

def create_replace_stars_file(file_path, separator=','):
    with open(file_path, mode='w', newline='') as w:
        # write the header of the file
        csv_file = csv.writer(w, delimiter=separator)
        csv_file.writerow(['stars', 'repositories', 'incomplete_results'])
    w.close()

def get_crawling_progress(file_path, separator=','):
    with open(file_path, mode='r', newline='') as r:
        csv_file = csv.reader(r, delimiter=separator)
        next(csv_file, None)  # skip the headers

        last_line = None
        for last_line in csv_file:
            pass 

        last_star = int(last_line[0]) if last_line else 0
    r.close()

    return last_star

def main():    
    parser = argparse.ArgumentParser(description='Stars collector from Github')
    parser.add_argument('-t', '--token', 
                        help='The Github token identifier to crawling data', required=True)
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('-d', '--date', default='2019-12-01', 
                        help='The start date for crawling (format: YYYY-MM-DD)', required=False)
    parser.add_argument('--cont', default=False,
                        help='Use this param with True value to continue a started crawling in a specific language',
                        required=False)

    args = parser.parse_args()
    print(args) 
    
    # Print start time processing
    start_time = datetime.datetime.now()
    print(f'Crawling stated at {start_time}\n')   
    
    # Get token by key
    token = utils.get_token_key(args.token)
    print(f'Token successfully obtained using token key {args.token}\n')

    # Recover the last number of stars or inicialize a new file
    stars_file_path = f'{utils.get_main_path()}data\\crawler\\stars\\{args.language}_stars_histogram.csv'
    if args.cont:
        init_star = get_crawling_progress(stars_file_path)
    else:
        init_star = 0
        create_replace_stars_file(stars_file_path)

    # Get max stars for language
    max_stars = get_max_stars(token, args.language, args.date)

    # Recover the histogram of repositories by stars
    save_stars_histogram(token, args.language, args.date, init_star, max_stars, stars_file_path)

    # Save the stars file
    stars_path = f'{utils.get_main_path()}data\\crawler\\stars\\{args.language}_stars_histogram.csv'
    stars_df.to_csv(stars_path, index=False, sep=';')
    print(f'\nStars file successfully saved on {stars_path}\n')
    
    # Print finish time processing
    end_time = datetime.datetime.now()
    print(f'Crawling finished at {end_time}\n')

    print('>> Crawling finished in', end_time - start_time, '<<')

if __name__ == '__main__':
    main()