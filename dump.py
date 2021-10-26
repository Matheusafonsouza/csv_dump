import pandas as pd
from tqdm import tqdm
import math
import re

def convert_to_int(value):
    return int(float(value))

def remove_special_characters(value):
    return re.sub(r'[^A-Za-z0-9]+', '', value)

df = pd.read_csv('./hashtag_joebiden.csv')
file = open('./insert.sql', 'w')

# USER TABLE
user_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating user table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        user_id = convert_to_int(row['user_id'])
        if user_id in user_ids_list:
            continue

        user_name = remove_special_characters(row['user_name'])
        user_screen_name = remove_special_characters(row['user_screen_name'])
        user_description = remove_special_characters(row['user_description'])
        user_followers_count = convert_to_int(row['user_followers_count'])
        user_location = remove_special_characters(row['user_location'])

        line += ("INSERT INTO USER (user_id, name, screen_name, description, followers_count, location) " \
                "VALUES ("
                f'{user_id},'
                f'"{user_name}",'
                f'"{user_screen_name}",'
                f'"{user_description}",'
                f'{user_followers_count},'
                f'"{user_location}"'
                ");\n")
        file.write(line)
        user_ids_list.append(user_id)

# STATE TABLE
state_code_list = df['state_code'].unique()
state_code_dump_list = list()
for state_code in tqdm(state_code_list, desc='Creating state list'):
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
    line = ""

    state_code = remove_special_characters(state_dict['state_code'])
    state = remove_special_characters(state_dict['state'])

    line += ("INSERT INTO STATE (state_code, state_name) " \
            "VALUES ("
            f'"{state_code}",'
            f'"{state}"'
            ");\n")
    file.write(line)

# LOCATION TABLE
location_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating location table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""

        lat = row['lat']
        long = row['long']

        if f'{lat}{long}' in location_ids_list:
            continue

        city = remove_special_characters(row['city'])
        country = remove_special_characters(row['country'])
        continent = remove_special_characters(row['continent'])
        state_code = remove_special_characters(row['state_code'])
        state = remove_special_characters(row['state'])

        line += ("INSERT INTO LOCATION (latitude, longitude, city, country, continent, state_code) " \
                "VALUES ("
                f'"{lat}",'
                f'"{long}",'
                f'"{city}",'
                f'"{country}",'
                f'"{continent}",'
                f'"{state_code}"'
                ");\n")
        file.write(line)
        location_ids_list.append(f'{lat}{long}')

# TWEET TABLE
tweet_ids_list = list()
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating tweet table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        tweet_id = convert_to_int(row['tweet_id'])
        if tweet_id in tweet_ids_list:
            continue

        created_at = row['created_at']
        tweet = remove_special_characters(row['tweet'])
        likes = convert_to_int(row['likes'])
        retweet_count = convert_to_int(row['retweet_count'])
        collected_at = row['collected_at']
        user_id = convert_to_int(row['user_id'])
        lat = row['lat']
        long = row['long']
        source = remove_special_characters(row['source'])

        line += ("INSERT INTO TWEET (tweet_id, created_at, message, likes, retweet_count, collected_at, user_id, latitude, longitude, source) " \
                "VALUES ("
                f'{tweet_id},'
                f'"{created_at}",'
                f'"{tweet}",'
                f'{likes},'
                f'{retweet_count},'
                f'"{collected_at}",'
                f'{user_id},'
                f'"{lat}",'
                f'"{long}",'
                f'"{source}"'
                ");\n")
        file.write(line)
        tweet_ids_list.append(tweet_id)

file.close()
