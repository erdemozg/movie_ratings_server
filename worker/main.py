import os
import time
import uuid
import sqlite3
from contextlib import closing
from datetime import date, timedelta
import json
import sinematv

db_file_path =  os.path.join(os.environ.get("DB_FOLDER", default="/files"), "movie_ratings.db") 

json_dumps_folder = os.environ.get("JSON_DUMPS_FOLDER", default="/files/json_dumps")

query_interval_in_seconds = os.environ.get("QUERY_INTERVAL_IN_SECONDS", default=14400)


def setup_db():
    if not os.path.isfile(db_file_path):
        with closing(sqlite3.connect(db_file_path)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute('''
                    CREATE TABLE movies 
                    (
                        id TEXT, 
                        streaming_provider TEXT,
                        channel TEXT, 
                        air_date TEXT, 
                        air_time_hour INTEGER, 
                        air_time_minute INTEGER, 
                        movie_title TEXT, 
                        release_year INTEGER, 
                        summary TEXT, 
                        image_url TEXT, 
                        provider_url TEXT,
                        imdb_movie_id TEXT,
                        imdb_rating REAL
                    )
                ''')


def insert(streaming_provider, date_string, channels, flows):
    if len(channels) > 0:
        with closing(sqlite3.connect(db_file_path)) as connection:
            with closing(connection.cursor()) as cursor:
                for i in range(len(flows)):
                    channel = channels[i]
                    flow = flows[i]
                    for item in flow:
                        air_time_hour = item['start_time'].split(':')[0]
                        air_time_minute = item['start_time'].split(':')[1]
                        title = item.get('title', '')
                        release_year = item.get('release_year', 0)
                        summary = item.get('summary', '')
                        image_url = item.get('image_url', '')
                        movie_url = item.get('url', '')
                        imdb_movie_id = item.get('imdb_movie_id', '')
                        imdb_rating = item.get('imdb_rating', '')
                        existing_records = cursor.execute('''
                            select 
                                *
                            from 
                                movies 
                            where 
                                streaming_provider = ?
                                and channel = ?
                                and air_date = ?
                                and air_time_hour = ?
                                and air_time_minute = ?
                                and movie_title = ?
                            ''',
                            (streaming_provider, channel, date_string, air_time_hour, air_time_minute, item['title'])).fetchall()
                        if len(existing_records) == 0:
                            query = 'insert into movies values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                            cursor.execute(
                                query, (
                                    str(uuid.uuid4()), 
                                    streaming_provider, 
                                    channel, 
                                    date_string, 
                                    air_time_hour, 
                                    air_time_minute, 
                                    title, 
                                    release_year, 
                                    summary, 
                                    image_url, 
                                    movie_url,
                                    imdb_movie_id,
                                    imdb_rating)
                                )
            connection.commit()


def build_date_string(input_date):
    day = input_date.day
    month = input_date.month
    year = input_date.year
    day_str = str(day) if len(str(day)) > 1 else f'0{day}'
    month_str = str(month) if len(str(month)) > 1 else f'0{month}'
    year_str = str(year)
    return f'{day_str}.{month_str}.{year_str}'


def get_dates_to_request(streaming_provider):
    dates_to_request = []
    now = date.today()
    with closing(sqlite3.connect(db_file_path)) as connection:
        with closing(connection.cursor()) as cursor:
            for i in range(7):
                date_to_request = build_date_string(now + timedelta(days=i))
                existing_records = cursor.execute(
                    'select * from movies where streaming_provider = ? and air_date = ?',
                    (streaming_provider, date_to_request)).fetchall()
                if len(existing_records) == 0:
                    dates_to_request.append(date_to_request)
    return dates_to_request


def process_sinematv():
    dates_to_request = get_dates_to_request(sinematv.streaming_provider_name)
    print(dates_to_request)
    for date_string in dates_to_request:
        channels, flows = sinematv.get_channels_and_movies(date_string)
        insert(sinematv.streaming_provider_name, date_string , channels, flows)


def dump_daily_json_files():
    
    now = date.today()
    with closing(sqlite3.connect(db_file_path)) as connection:
        connection.row_factory = sqlite3.Row
        with closing(connection.cursor()) as cursor:
            for i in range(7):
                list_to_dump = []
                date_string = build_date_string(now + timedelta(days=i))
                json_file =  os.path.join(json_dumps_folder, f'{date_string}.json')
                if not os.path.isfile(json_file):
                    rows = cursor.execute('''
                        select 
                            id,                
                            streaming_provider,
                            channel,           
                            air_date,          
                            air_time_hour,     
                            air_time_minute,   
                            movie_title,       
                            release_year,      
                            summary,           
                            image_url,         
                            provider_url,      
                            imdb_movie_id,     
                            imdb_rating       
                        from 
                            movies 
                        where 
                            air_date = ?''', [date_string]).fetchall()
                    if len(rows) > 0:
                        list_to_dump = [dict(zip(row.keys(), row)) for row in rows]
                        with open(json_file, 'w', encoding="utf-8") as f:
                            json.dump(list_to_dump, f, ensure_ascii=False, indent=4, sort_keys=True)
                        print(f'written: {json_file}')                       


def main():
    setup_db()
    while True:
        try:
            print("processing sinema tv")
            process_sinematv()
            print("dumping json file")
            dump_daily_json_files()
        except Exception as e:
            print(f'{type(e)}:{e}')
        print("sleeping")
        time.sleep(int(query_interval_in_seconds))


if __name__ == '__main__':
    main()
