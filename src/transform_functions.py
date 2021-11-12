import numpy as np
import pandas as pd


def transform_matches(df, is_international):

    list_of_columns = [
        'gameweek', 'dayofweek', 'date', 'time', 'home_team',
        'xg_home', 'home_score', 'away_score', 'xg_away', 'away_team',
        'attendance', 'venue', 'referee', 'match_report', 'notes',
        'competition_id', 'home_team_id', 'away_team_id', 'home_team_country',
        'away_team_country', 'home_score_penalty_shootout', 'away_score_penalty_shootout'
    ]

    df = df.rename(columns={
        'squad_a': 'home_team',
        'squad_b': 'away_team',
        'xg_a': 'xg_home',
        'xg_b': 'xg_away'
    })
    df['attendance'] = df['attendance'].apply(lambda x: x.replace(',', ''))
    df[['score', 'away_score']] = df['score'].str.split('–', expand=True)
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.rename(columns={'score': 'home_score'})
    if is_international:
        df['home_team_country'] = df['home_team'].apply(lambda x: x.split(' ')[-1])
        df['away_team_country'] = df['away_team'].apply(lambda x: x.split(' ')[0])
        df['home_team'] = df['home_team'].apply(lambda x: ' '.join(x.split(' ')[:-1]))
        df['away_team'] = df['away_team'].apply(lambda x: ' '.join(x.split(' ')[1:]))
    try:
        df['home_score'].astype(int)
    except ValueError:
        df['home_score_penalty_shootout'] = df['home_score'].apply(lambda x: str(x).split(' ')[0][1:-1])
        df['away_score_penalty_shootout'] = df['away_score'].apply(lambda x: str(x).split(' ')[-1][1:-1])
        df['home_score'] = df['home_score'].apply(lambda x: str(x).split(' ')[-1])
        df['away_score'] = df['away_score'].apply(lambda x: str(x).split(' ')[0])

    for col in list_of_columns:
        if col not in df.columns:
            df[col] = ''

    return df


def transform_summary(df):
    df['first_squad'] = (df['player'].str[:1] != u'\xa0')
    df['player'] = df['player'].str.strip()
    df['nationality'] = df['nationality'].str.split(' ', expand=True)[1]
    df['age'] = np.where(df['age'] == '', np.NaN, df['age'])
    df['age'] = df['age'].str.split('-', expand=True)[0].astype(float) + \
                df['age'].str.split('-', expand=True)[1].astype(float) / 365.0
    return df


def transform_shots(df):
    try:
        df[['minute', 'additional_time']] = df['minute'].str.split('+', expand=True, n=2)
    except:
        df['additional_time'] = None
    df = df.rename(columns={'squad': 'team'})
    df = df[df['player'] != '']
    df['penalty'] = np.where(df['player'].str[-1] == ')', True, False)
    df['player'] = df['player'].apply(lambda x: x.replace(' (pen)', ''))

    return df


def transform_goalkeeper_stats(df):
    df['nationality'] = df['nationality'].str.split(' ', expand=True)[1]
    df['age'] = np.where(df['age'] == '', np.NaN, df['age'])
    df['age'] = df['age'].str.split('-', expand=True)[0].astype(float) + \
                df['age'].str.split('-', expand=True)[1].astype(float) / 365.0
    return df


def transform_stat_player(df):
    df = df.drop(columns=[
        'player',
        'minutes',
        'nationality',
        'shirtnumber',
        'position',
        'age',
        'team'
    ])

    return df
