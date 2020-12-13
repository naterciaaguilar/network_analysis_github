import sys
import os

import pandas as pd

def get_main_path():
    current_path = os.getcwd()
    limit_position = current_path.find('src')
    
    return current_path[:limit_position]

def get_token_key(token_key, filename='tokens.csv'):
    # get the path before the src folder and complete to the data path
    tokens_path = os.path.join(get_main_path(), 'data', filename)
    
    df_tokens = pd.read_csv(tokens_path, header=None, sep=',', names=['token_key', 'token'], engine='python')
    
    try:
        return df_tokens[df_tokens['token_key'] == token_key].iloc[0]['token']
    except:
        print(f'The token key ({token_key}) does not exist in the tokens file ({tokens_path})')
        sys.exit(-1)