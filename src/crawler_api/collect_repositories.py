import requests
import json
import pandas as pd
import math
import os
import sys
import csv
import argparse


from datetime import datetime

def background():
    date = '%3e2019-12-01'
    language = '\"Jupyter+Notebook\"'
    per_page = 100

    addr = f'https://api.github.com/search/repositories?q=language%3A{language}+pushed%3A{date}&per_page={per_page}'
    r = requests.get(addr)

    # requests.get('https://api.github.com/search/code?l=Jupyter+Notebook&q=ipynb+in:path+extension:ipynb',
    #           headers={'Authorization': 'token %s' % os.environ['GITHUB_TOKEN']})

    data = json.loads(r.content)

    print(data['total_count'])
    print(len(data['items']))


    n_pages = math.ceil(int(data['total_count']) / per_page)


    for i in range(1, n_pages):
        print(f"Recovering page {i}")
        addr = f'https://api.github.com/search/repositories?q=language%3A{language}+pushed%3A{date}&per_page={per_page}&page={i}'
        r = requests.get(addr)
        data = json.loads(r.content)
        ponei = data['items']

        df = pd.DataFrame(ponei)
        print(f"Saving Results to CSV_{i}")
        df.to_csv(f'results_{i}.csv')

def get_main_path():
    current_path = os.getcwd()
    limit_position = current_path.find('src')
    
    return current_path[:limit_position]

def get_token_key(token_key):
    # get the path before the src folder and complete to the data path
    tokens_path = get_main_path() + 'data\\tokens.csv'
    
    df_tokens = pd.read_csv(tokens_path, header=None, sep=',', names=['token_key', 'token'], engine='python')
    
    try:
        return df_tokens[[df_tokens['token_key'] == token_key]]['token'][0]
    except:
        print(f'The token key ({token_key}) does not exist in the tokens file ({tokens_path})')
        sys.exit(1)

def get_search_query(language, date):
    q_date = f'%3e{date}'
    q_language = f'\"{language}\"'
    q_per_page = 100

    complete_query = f'https://api.github.com/search/repositories?q=language%3A{q_language}+pushed%3A{q_date}&per_page={q_per_page}'
    
    return complete_query

def main():    
    parser = argparse.ArgumentParser(description='Repositories collector from Github')
    parser.add_argument('-t', '--token', 
                        help='The Github token identifier to crawling data', required=True)
    parser.add_argument('-l', '--language', 
                        help='The programming language to be collected (hint: replace spaces by +)', required=True)
    parser.add_argument('-d', '--date', default='2019-12-01', 
                        help='The start date for crawling (format: YYYY-MM-DD)', required=True)

    args = parser.parse_args()
    
    # Print start time processing
    start_time = datetime.now()
    print('Crawling stated at', start_time)

    # Get token by key
    token = get_token_key(args.token)
    print(f'Token successfully obtained using token key {args.token}\n')

    # Create the search query using the given params
    query = get_search_query(args.language, args.date)
    print('Resulted search query:\n', query, '\n')
    
    # Print finish time processing
    end_time = datetime.now()
    print('Crawling finished at', end_time)

    print('\nCrawling finished in', end_time - start_time)

if __name__ == '__main__':
    main()