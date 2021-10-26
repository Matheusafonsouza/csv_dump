import pandas as pd
from tqdm import tqdm
import math
import re

def convert_to_int(value):
    """
    convert string integer value to int type on python
    """
    return int(float(value))

def remove_special_characters(value):
    """
    remove any special character inside string
    """
    return re.sub(r'[^A-Za-z0-9]+', '', value)

def create_sql_insert(table, keys, values):
    """
    create insert message for sql file, using keys to table definition and
    values array for define inserted values
    """
    line = ""
    line += f'INSERT INTO {table} {keys} VALUES ('

    values_list = list()
    for value in values:
        if isinstance(value, int):
            values_list.append(f'{value}')
        else:
            values_list.append(f'"{value}"')
        
    line += ', '.join(values_list)
    line += ');\n'
    return line

def verify_already_exists(value, arr):
    """
    verify if a value already exists in array, removing the probability of
    re-insert it on the list
    """
    if value in arr:
        return True
    return False

# read csv file and create file to sql creation
df = pd.read_csv('./hashtag_joebiden.csv')
file = open('./insert.sql', 'w')

# USER TABLE
user_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating user table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        user_id = convert_to_int(row['user_id'])
        if verify_already_exists(user_id, user_ids_list):
            continue

        keys = '(user_id, name, screen_name, description, followers_count, location)'
        values = [
            user_id,
            remove_special_characters(row['user_name']),
            remove_special_characters(row['user_screen_name']),
            remove_special_characters(row['user_description']),
            convert_to_int(row['user_followers_count']),
            remove_special_characters(row['user_location'])
        ]
        line = create_sql_insert('USER', keys, values)
        file.write(line)
        user_ids_list.append(user_id)

# STATE TABLE
state_code_dump_list = list()
for state_code in tqdm(df['state_code'].unique(), desc='Creating state list'):
    try:
        parsed_state_code = float(state_code)
    except:
        parsed_state_code = float('1.0')

    if not math.isnan(parsed_state_code):
        filtered_df = df[df['state'].notnull()]
        rows = filtered_df[filtered_df['state_code'] == state_code]
        state_name_list = rows['state'].unique()
        state = state_name_list[0]
        state_code_dump_list.append(dict(state_code=state_code, state=state))

for state_dict in tqdm(state_code_dump_list, desc='Creating state table inserts'):
    keys = '(state_code, state_name)'
    values = [
        remove_special_characters(state_dict['state_code']),
        remove_special_characters(state_dict['state'])
    ]
    line = create_sql_insert('STATE', keys, values)
    file.write(line)

# LOCATION TABLE
location_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating location table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        lat = row['lat']
        long = row['long']
        if verify_already_exists(f'{lat}{long}', location_ids_list):
            continue

        keys = '(latitude, longitude, city, country, continent, state_code)'
        values = [
            lat,
            long,
            remove_special_characters(row['city']),
            remove_special_characters(row['country']),
            remove_special_characters(row['continent']),
            remove_special_characters(row['state_code']),
            remove_special_characters(row['state'])
        ]
        line = create_sql_insert('LOCATION', keys, values)
        file.write(line)
        location_ids_list.append(f'{lat}{long}')

# TWEET TABLE
tweet_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating tweet table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        tweet_id = convert_to_int(row['tweet_id'])
        if verify_already_exists(tweet_id, tweet_ids_list):
            continue

        keys = '(tweet_id, created_at, message, likes, retweet_count, collected_at, user_id, latitude, longitude, source)'
        values = [
            tweet_id,
            row['created_at'],
            remove_special_characters(row['tweet']),
            convert_to_int(row['likes']),
            convert_to_int(row['retweet_count']),
            row['collected_at'],
            convert_to_int(row['user_id']),
            row['lat'],
            row['long'],
            remove_special_characters(row['source'])
        ]
        line = create_sql_insert('TWEET', keys, values)
        file.write(line)
        tweet_ids_list.append(tweet_id)

file.close()
