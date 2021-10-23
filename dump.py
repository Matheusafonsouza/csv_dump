import pandas as pd
from tqdm import tqdm
import math

df = pd.read_csv('./hashtag_joebiden.csv')
file = open('./insert.sql', 'w')

# USER TABLE
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating user table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        line += ("INSERT INTO USER (user_id, name, screen_name, description, followers_count, location) " \
                "VALUES ("
                f"'{row['user_id']}'," 
                f"'{row['user_name']}'," 
                f"'{row['user_screen_name']}'," 
                f"'{row['user_description']}'," 
                f"'{row['user_followers_count']}'," 
                f"'{row['user_location']}'" 
                ")\n")
        file.write(line)

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
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        line += ("INSERT INTO STATE (state_code, state_name) " \
                "VALUES ("
                f"'{state_dict['state_code']}'," 
                f"'{state_dict['state']}'" 
                ")\n")
        file.write(line)

# SOURCE TABLE
sources = df['source'].unique()
for source in tqdm(sources, desc='Creating source table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        line += ("INSERT INTO SOURCE (name) " \
                "VALUES ("
                f"'{source}'" 
                ")\n")
        file.write(line)

# LOCATION TABLE
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating location table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        line += ("INSERT INTO LOCATION (latitude, longitude, city, country, continent, state_code, state_name) " \
                "VALUES ("
                f"'{row['lat']}'," 
                f"'{row['long']}'," 
                f"'{row['city']}'," 
                f"'{row['country']}'," 
                f"'{row['continent']}'," 
                f"'{row['state_code']}'," 
                f"'{row['state']}'" 
                ")\n")
        file.write(line)

# TWEET TABLE
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='Creating tweet table inserts'):
    have_null_value = row.isnull().values.any()
    if not have_null_value:
        line = ""
        line += ("INSERT INTO TWEET (tweet_id, created_at, message, likes, retweet_count, collected_at, user_id, latitude, longitude, source_id) " \
                "VALUES ("
                f"'{row['tweet_id']}'," 
                f"'{row['created_at']}'," 
                f"'{row['tweet']}'," 
                f"'{row['likes']}'," 
                f"'{row['retweet_count']}'," 
                f"'{row['collected_at']}'," 
                f"'{row['user_id']}'," 
                f"'{row['lat']}'," 
                f"'{row['long']}'," 
                f"'{row['source']}'" 
                ")\n")
        file.write(line)

file.close()
