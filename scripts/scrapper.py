import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import os
import random
from tqdm import tqdm


def get_director_from_movie_page(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        director = None

        try:
            director_section = soup.select_one(
                'section[data-testid="title-cast"] .ipc-metadata-list__item:nth-child(1)')
            if director_section and "Director" in director_section.text:
                director_links = director_section.select('a[class*="ipc-metadata-list-item__list-content-item"]')
                if director_links:
                    director = director_links[0].text.strip()
        except Exception:
            pass

        return director
    except Exception as e:
        print(f"Error fetching director info from {url}: {e}")
        return None


def extract_movie_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Пользуемся JSON-LD script tag'ом, потому что imdb почему то
    # в html ответе возвращает только информацию о 25 фильмах((
    json_ld = soup.find('script', {'type': 'application/ld+json'})

    if not json_ld:
        print("JSON-LD data not found")
        return []

    try:
        data = json.loads(json_ld.string)
        movies = []

        for item in data.get('itemListElement', []):
            movie_data = item.get('item', {})

            title = movie_data.get('name')

            rating = None
            if 'aggregateRating' in movie_data:
                rating = movie_data['aggregateRating'].get('ratingValue')

            genres = []
            genre_data = movie_data.get('genre')
            if isinstance(genre_data, list):
                genres = genre_data
            elif isinstance(genre_data, str):
                genres = [genre.strip() for genre in genre_data.split(',')]

            description = movie_data.get('description')

            url = movie_data.get('url')

            movie = {
                'title': title,
                'rating': rating,
                'genres': ', '.join(genres) if genres else None,
                'description': description,
                'director': None,
                'url': url
            }

            movies.append(movie)

        return movies

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []


def scrape_imdb_top_movies(num_movies=250):
    url = "https://www.imdb.com/chart/top/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }

    try:
        print(f"Fetching main page: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        movies = extract_movie_data(response.text)
        movies = movies[:num_movies]

        print(f"Found {len(movies)} movies. Now fetching director information...")

        for i, movie in enumerate(tqdm(movies, desc="Fetching directors")):
            if movie['url']:
                director = get_director_from_movie_page(movie['url'])
                movies[i]['director'] = director

                time.sleep(random.uniform(1, 3))

        df = pd.DataFrame(movies)

        os.makedirs('data', exist_ok=True)

        csv_path = 'data/imdb_top_movies.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"Successfully scraped {len(df)} movies and saved to {csv_path}")

        return df

    except Exception as e:
        print(f"Error during scraping: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    num_movies = 250
    scrape_imdb_top_movies(num_movies)
