from ETL_module import ETLCompetitions
import pandas as pd

inputs = pd.read_csv('links.csv')

for _, row in inputs.iterrows():
    x = ETLCompetitions(
        season=row['season'],
        league=row['league'],
        url=row['url']
    )

    print('------------ START ------------')
    print(f"""
        season: {row['season']}
        league: {row['league']}
    """)
    x.create_database_connection()
    print('Connection created')
    x.find_competition_id()
    x.insert_into_competitions()
    x.extract_matches()
    x.transform_matches()
    x.insert_record_into_matches_and_player_stats()
    x.close_database_connection()
    print('Connection closed')
    print('------------ FINISH ------------')
