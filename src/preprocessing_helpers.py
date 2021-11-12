from bs4 import BeautifulSoup
import requests
import pandas as pd


def read_site(url: str):
    page = requests.get(url).content
    return BeautifulSoup(page, 'html.parser')


def read_tbody(tbody):
    helper = dict()
    final_df = pd.DataFrame()

    for row in tbody.find_all('tr'):
        for col_value in row:
            helper[col_value.get('data-stat')] = col_value.get_text()
            if col_value.get('data-stat') == 'player' and col_value.find('a') is not None:
                helper['player_id'] = col_value.find('a')['href'].split('/')[3]
            if col_value.get('data-stat') == 'squad_a' and col_value.find('a') is not None:
                helper['home_team_id'] = col_value.find('a')['href'].split('/')[3]
            if col_value.get('data-stat') == 'squad_b' and col_value.find('a') is not None:
                helper['away_team_id'] = col_value.find('a')['href'].split('/')[3]
            if col_value.get('data-stat') == 'squad' and col_value.find('a') is not None:
                helper['team_id'] = col_value.find('a')['href'].split('/')[3]
        df = pd.DataFrame(helper, index=[0])
        final_df = pd.concat([final_df, df])

    return final_df


def read_tbodies(list_of_tbodies):
    final_df = pd.DataFrame()
    helper = dict()

    for tbody in list_of_tbodies:
        df = read_tbody(tbody)
        final_df = pd.concat([final_df, df])

    return final_df


def scrap_matches(soup, competition_id) -> pd.DataFrame:
    html = (
        soup
            .find('div', {'id': 'all_sched'})
            .find('div', {'class': 'table_container current'})
            .find('table', {'class': 'stats_table sortable min_width'})
            .find('tbody')
    )

    final_df = pd.DataFrame()

    for row in html.find_all('tr'):
        if row.get('class'):
            continue
        if row.find('td', {'data-stat': 'match_report'}).find('a', href=True) is None:
            continue
        helper = dict()
        for i in row:
            if i.get('data-stat') == 'match_report':
                helper[i.get('data-stat')] = 'http://fbref.com' + i.find('a')['href']
            else:
                helper[i.get('data-stat')] = i.get_text()
            if i.get('data-stat') == 'squad_a' and i.find('a') is not None:
                helper['home_team_id'] = i.find('a')['href'].split('/')[3]
            if i.get('data-stat') == 'squad_b' and i.find('a') is not None:
                helper['away_team_id'] = i.find('a')['href'].split('/')[3]
            if i.get('data-stat') == 'squad' and i.find('a') is not None:
                helper['team_id'] = i.find('a')['href'].split('/')[3]
        df = pd.DataFrame(helper, index=[0])
        final_df = pd.concat([final_df, df])
    final_df['competition_id'] = competition_id

    return final_df


def scrap_goalkeeper_stats(match_soup, match_id):
    tbodies = [div.find('tbody') for div in (
        match_soup
            .find('div', {'id': 'content'})
            .find_all('div', {'class': 'table_wrapper'})
    ) if len(div.get('class')) == 1]

    final_df = read_tbodies(tbodies)
    final_df['match_id'] = match_id
    final_df = final_df.rename(columns={'id': 'goalkeeper_stat_id'})

    return final_df


def scrap_shots(match_soup, match_id):
    tbody = (
        match_soup
            .find('div', {'id': 'content'})
            .find('div', {'id': 'all_shots'})
            .find('div', {'id': 'div_shots_all'})
            .find('tbody')
    )

    final_df = read_tbody(tbody)
    final_df['match_id'] = match_id

    return final_df


def scrap_players_stats(match_soup, home_team, away_team, table_number, match_id, home_team_id, away_team_id):

    wrappers = (
        match_soup
            .find('div', {'id': 'content'})
            .find_all('div', {'class': 'table_wrapper tabbed'})
        [:2]
    )

    wrap1_tbodies = wrappers[0].find_all('tbody')
    wrap2_tbodies = wrappers[1].find_all('tbody')

    tbodies = [wrap1_tbodies[table_number], wrap2_tbodies[table_number]]

    final_df = pd.DataFrame()

    for tbody, team, team_id in zip(tbodies, [home_team, away_team], [home_team_id, away_team_id]):
        single_df = read_tbody(tbody)
        single_df['team'] = team
        single_df['team_id'] = team_id
        final_df = pd.concat([final_df, single_df])

    final_df['match_id'] = match_id

    return final_df
