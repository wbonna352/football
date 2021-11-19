import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = 'https://fbref.com/en/comps/'
page = requests.get(URL).content
soup = BeautifulSoup(page, 'html.parser')

tbodies = (
    soup
    .find('div', {'id': 'content'})
    .find_all('tbody')
)

extract_dict = dict()
id = 1

for t in tbodies:
    for row in t.find_all('tr'):
        extract_dict[id] = dict()
        for col_value in row:
            extract_dict[id][col_value.get('data-stat')] = col_value.get_text()
            if col_value.get('data-stat') == 'league_name' and col_value.find('a') is not None:
                extract_dict[id]['competition_url'] = 'http://fbref.com' +  col_value.find('a')['href']
        id+=1

leagues_df = pd.DataFrame.from_dict(extract_dict).T
leagues_df = leagues_df[leagues_df['league_name'] != 'Big 5 European Leagues Combined']

extract_seasons_dict = dict()
season_id = 1
for _, league in leagues_df.iterrows():
    league_url = league['competition_url']

    league_page = requests.get(league_url).content
    league_soup = BeautifulSoup(league_page, 'html.parser')
    league_tbodies = (
        league_soup
            .find('div', {'id': 'content'})
            .find_all('tbody')
    )
    for t in league_tbodies:
        for row in t.find_all('tr'):
            extract_seasons_dict[season_id] = dict()
            for col_value in row:
                extract_seasons_dict[season_id][col_value.get('data-stat')] = col_value.get_text()
                if col_value.get('data-stat') in ['league_name', 'comp_name'] and col_value.find('a') is not None:
                    extract_seasons_dict[season_id]['season_url'] = 'http://fbref.com' +  col_value.find('a')['href']
            final_url = extract_seasons_dict[season_id]['season_url']
            final_page = requests.get(final_url).content
            final_soup = BeautifulSoup(final_page, 'html.parser')
            try:
                for i in (
                        final_soup
                        .find('div', {'id': 'inner_nav'})
                        .find_all('a')
                ):
                    if i.get_text() == 'Scores & Fixtures' and i is not None:
                        extract_seasons_dict[season_id]['fixtures_url'] = 'http://fbref.com' + i['href']
                        break
            except:
                extract_seasons_dict[season_id]['fixtures_url'] = None

            for col in ['gender', 'governing_body', 'tier']:
                extract_seasons_dict[season_id][col] = league[col]
            try:
                extract_seasons_dict[season_id]['country'] = league['country'].split(' ',)[1]
            except:
                extract_seasons_dict[season_id]['country'] = None

            season_id+=1

seasons_df = pd.DataFrame.from_dict(extract_seasons_dict).T

seasons_df['league'] = seasons_df['league_name'].combine_first(seasons_df['comp_name'])
seasons_df['season'] = seasons_df['season'].combine_first((seasons_df['year']))
seasons_df['url'] = seasons_df['fixtures_url']

seasons_df_final = (
    seasons_df
    [seasons_df['fixtures_url'].notna()]
    [['season', 'league', 'url', 'host_country', 'gender', 'governing_body', 'tier', 'country']]
    .drop_duplicates()
)


seasons_df_final.to_csv('competitions_df.csv', index=False)