import requests
import re
import time
from bs4 import BeautifulSoup as Soup
from imdb import IMDb


streaming_provider_name = 'sinematv'
base_url = 'https://sinematv.com.tr'
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
imdb_client = IMDb()
movie_cache = {}


def get_channels_and_movies(date_string):
    with requests.Session() as session:
        session.headers = {'User-Agent': user_agent}
        response = session.get(f'{base_url}/Yayin-Akisi')
        request_verification_token = ''

        for line in response.text.splitlines():
            if 'var requestVerificationToken' in line:
                matches = re.search('value="([^"]*)"', line)
                request_verification_token = matches.group(1)

        response = session.post(f'{base_url}/Asset/GetTvGuide/', data={'date': date_string, '__RequestVerificationToken': request_verification_token})
        soup = Soup(response.text, features="html.parser")
        channels = [ch.get_text().strip() for ch in soup.select('div.flow-channel')]
        flow_parts = soup.select('div.row.flow > div.col-xs-12')
        channel_flows = flow_parts[1].select('div.row')
        channel_movie_lists = [[{'title': event.select('a')[0].get_text().strip(), 'url': event.select('a')[0]["href"], 'start_time': event.select('a')[2].get_text().strip()} for event in channel_flows[i].select('div.event')] for i in range(len(channel_flows))]

        for channel_movie_list in channel_movie_lists:
            for channel_movie in channel_movie_list:
                movie_details_url = f'{base_url}{channel_movie["url"]}'
                print(movie_details_url)
                cache_hit = False
                
                if movie_details_url in movie_cache:
                    cache_item = movie_cache[movie_details_url]
                    if cache_item["queried_at"] > get_current_time_in_millisecods() - (1000 * 60 * 60 * 24 * 30):
                        print("cache hit!")
                        channel_movie["release_year"] = cache_item.get('release_year')
                        channel_movie['image_url'] = cache_item.get('image_url')
                        channel_movie['summary'] = cache_item.get('summary', None)
                        channel_movie['imdb_movie_id'] = cache_item.get('imdb_movie_id')
                        channel_movie['imdb_rating'] = cache_item.get('imdb_rating')
                        cache_hit = True
                    else:
                        print("cache expired")
                
                if not cache_hit:
                    response = session.get(movie_details_url)
                    soup = Soup(response.text, features="html.parser")
                    title_with_year = soup.select('span.fl-title-medium')
                    if len(title_with_year) > 0:
                        matches = re.findall('\((\d\d\d\d)\)', title_with_year[0].get_text().strip())
                        if len(matches) > 0:
                            release_year = matches[len(matches)-1]
                            if release_year.isdigit():
                                channel_movie["release_year"] = release_year
                    img_elems = soup.select('div.img-fl-detail > img')
                    if len(img_elems) > 0:
                        elem = img_elems[0]
                        if elem.has_attr('data-slazy-src'):
                            channel_movie['image_url'] = elem['data-slazy-src']
                    summary = soup.select('div.container > div.row > div.col-xs-12 > p')
                    if len(summary) > 0:
                        channel_movie['summary'] = summary[0].get_text().strip()
                    imdb_results = imdb_client.search_movie(f'{channel_movie["title"]} ({channel_movie["release_year"]})')
                    if len(imdb_results) > 0:
                        imdb_result = imdb_results[0]
                        channel_movie['imdb_movie_id'] = imdb_result.movieID
                        imdb_client.update(imdb_result, info=['vote details'])
                        if 'arithmetic mean' in imdb_result:
                            channel_movie['imdb_rating'] = imdb_result['arithmetic mean']
                            print(f'{imdb_result.movieID}: {channel_movie["imdb_rating"]}')
                    channel_movie["queried_at"] = get_current_time_in_millisecods()
                    movie_cache[movie_details_url] = channel_movie
        
        return channels, channel_movie_lists


def get_current_time_in_millisecods():
    return round(time.time() * 1000)
