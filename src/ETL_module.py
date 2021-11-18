import pandas as pd
import pymssql
from datetime import date, timedelta

import preprocessing_helpers as ph
import transform_functions as tf


class ETLCompetitions:

    def __init__(self, url, season, league):
        self.url = url
        self.season = season
        self.league = league

    @property
    def is_international(self):
        return self.league in ['Champions League', 'Europa League']

    def create_database_connection(self):
        server = 'localhost'
        username = 'sa'
        password = '<Bz4bH7z'
        database = 'football'

        self.conn = pymssql.connect(server, username, password, database)

    def close_database_connection(self):
        self.conn.close()

    def commit_database(self):
        self.conn.commit()

    def find_competition_id(self):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT id
            FROM fbref.Competitions
            WHERE url = '{self.url}'
        """)
        result = cursor.fetchone()
        if result:
            self.is_new = False
            self.competition_id = result[0]
        else:
            self.is_new = True
            cursor.execute(f"""
                SELECT MAX(id)+1
                FROM fbref.Competitions
            """)
            result = cursor.fetchone()
            self.competition_id = result[0] if result[0] is not None else 1
        cursor.close()

    def insert_into_competitions(self):
        if self.is_new:
            cursor = self.conn.cursor()
            cursor.execute(f"""
                INSERT INTO fbref.Competitions (id, season, league, url)
                VALUES ('{self.competition_id}', '{self.season}', '{self.league}', '{self.url}')
            """)
            self.commit_database()
            cursor.close()

    def extract_matches(self):
        self.matches_after_extract = ph.scrap_matches(
            soup=ph.read_site(self.url),
            competition_id=self.competition_id
        )

    def transform_matches(self):
        self.matches_df = tf.transform_matches(self.matches_after_extract, self.is_international)

    def find_match_id(self, match_report: str):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT id
            FROM fbref.Matches
            WHERE match_report = '{match_report}'
        """)
        result = cursor.fetchone()
        if result:
            is_new = False
            match_id = result[0]
        else:
            is_new = True
            cursor.execute(f"""
                SELECT MAX(id)+1
                FROM fbref.Matches
            """)
            result = cursor.fetchone()
            match_id = result[0] if result[0] is not None else 1
        cursor.close()
        return match_id, is_new

    def insert_record_into_matches_and_player_stats(self):

        count = 0

        PlayerStats_table_names_dict = {
            0: 'fbref.PlayerStatsSummary',
            1: 'fbref.PlayerStatsPassing',
            2: 'fbref.PlayerStatsPassTypes',
            3: 'fbref.PlayerStatsDefActions',
            4: 'fbref.PlayerStatsPossession',
            5: 'fbref.PlayerStatsMisc'
        }

        def load_player_stats(table_name, df, columns):

            for col in columns:
                if col not in df.columns:
                    df[col] = None

            cols_str = ", ".join([str(i) for i in columns])
            for _, r in df.where((pd.notnull(df)), None).iterrows():
                sql = f"INSERT INTO {table_name} ({cols_str}) VALUES (" + "%s,"*(len(columns)-1) + "%s)"
                cursor.execute(sql, tuple([r[col] for col in columns]))


        cursor = self.conn.cursor()
        for _, row in self.matches_df.replace({"'": "''"}, regex=True).iterrows():
            match_id, is_new = self.find_match_id(row['match_report'])

            if (
                    is_new and
                    (row['date'] < (date.today() - timedelta(days=6))) and
                    row['home_score'] #and
                    #row['xg_home'] # TODO
            ):

                count += 1
                print(f"""{count}{chr(9)}{row['home_team']} - {row['away_team']}""")

                cursor.execute(f"""
                    INSERT INTO fbref.Matches (
                    id, gameweek, dayofweek, date, time, home_team,
                    xg_home, home_score, away_score, xg_away, away_team,
                    attendance, venue, referee, match_report, notes,
                    competition_id, home_team_id, away_team_id, home_team_country,
                    away_team_country, home_score_penalty_shootout, away_score_penalty_shootout)
                    VALUES (
                    '{match_id}', '{row['gameweek']}', '{row['dayofweek']}',
                    '{row['date']}', '{row['time']}', '{row['home_team']}', '{row['xg_home']}',
                    '{row['home_score']}', '{row['away_score']}', '{row['xg_away']}', '{row['away_team']}',
                    '{row['attendance']}', '{row['venue']}', '{row['referee']}', '{row['match_report']}',
                    '{row['notes']}', '{row['competition_id']}', '{row['home_team_id']}', '{row['away_team_id']}',
                    '{row['home_team_country']}', '{row['away_team_country']}', '{row['home_score_penalty_shootout']}',
                    '{row['away_score_penalty_shootout']}')
                """)




                # EXTRACT
                match_soup = ph.read_site(row['match_report'])
                try:
                    shots_extracted = ph.scrap_shots(match_soup, match_id)
                    goalkeeper_stats_extracted = ph.scrap_goalkeeper_stats(match_soup, match_id)
                except:
                    pass
                player_stats_extracted_dict = dict()
                for table_number, table_name in PlayerStats_table_names_dict.items():
                    try:
                        player_stats_extracted_dict[table_name] = ph.scrap_players_stats(
                            match_soup=match_soup,
                            home_team=row['home_team'],
                            away_team=row['away_team'],
                            table_number=table_number,
                            match_id=match_id,
                            home_team_id=row['home_team_id'],
                            away_team_id=row['away_team_id']
                        )
                    except:
                        pass



                # TRANSFORM
                try:
                    shots_transformed = tf.transform_shots(shots_extracted)
                    goalkeeper_stats_transformed = tf.transform_goalkeeper_stats(goalkeeper_stats_extracted)
                except:
                    shots_transformed = pd.DataFrame()
                    goalkeeper_stats_transformed = pd.DataFrame()
                player_stats_transformed_dict = dict()
                for table_number, table_name in PlayerStats_table_names_dict.items():
                    try:
                        if table_name == 'fbref.PlayerStatsSummary':
                            player_stats_transformed_dict[table_name] = \
                                tf.transform_summary(player_stats_extracted_dict[table_name])
                        else:
                            try:
                                player_stats_transformed_dict[table_name] = \
                                tf.transform_stat_player(player_stats_extracted_dict[table_name])
                            except:
                                player_stats_transformed_dict[table_name] = pd.DataFrame()
                    except:
                        pass

                # LOAD

                loads = [
                    {'table_name': 'fbref.Shots',
                     'df': shots_transformed,
                     'columns': [
                         'player_id', 'minute', 'additional_time', 'player','team_id', 'team',
                         'outcome', 'distance', 'body_part', 'notes', 'sca_1_player',
                         'sca_1_type', 'sca_2_player', 'sca_2_type', 'match_id', 'penalty'
                     ]},
                    {'table_name': 'fbref.GoalkeeperStats',
                     'df': goalkeeper_stats_transformed,
                     'columns': [
                         'player_id', 'age', 'minutes', 'match_id', 'shots_on_target_against',
                         'goals_against_gk', 'saves', 'save_pct', 'psxg_gk', 'passes_completed_launched_gk',
                         'passes_launched_gk', 'passes_pct_launched_gk', 'passes_gk', 'passes_throws_gk',
                         'pct_passes_launched_gk', 'passes_length_avg_gk', 'goal_kicks',
                         'pct_goal_kicks_launched', 'goal_kick_length_avg', 'crosses_gk',
                         'crosses_stopped_gk', 'crosses_stopped_pct_gk', 'def_actions_outside_pen_area_gk',
                         'avg_distance_def_actions_gk'
                     ]},
                    {'table_name': 'fbref.PlayerStatsDefActions',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsDefActions'],
                     'columns': [
                         'player_id', 'match_id', 'tackles', 'tackles_won', 'tackles_def_3rd',
                         'tackles_mid_3rd', 'tackles_att_3rd', 'dribble_tackles', 'dribbles_vs',
                         'dribble_tackles_pct', 'dribbled_past', 'pressures', 'pressure_regains',
                         'pressure_regain_pct', 'pressures_def_3rd', 'pressures_mid_3rd', 'pressures_att_3rd',
                         'blocks', 'blocked_shots', 'blocked_shots_saves', 'blocked_passes', 'interceptions',
                         'tackles_interceptions', 'clearances', 'errors'
                     ]},
                    {'table_name': 'fbref.PlayerStatsMisc',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsMisc'],
                     'columns': [
                         'player_id', 'match_id', 'cards_yellow', 'cards_red', 'cards_yellow_red',
                         'fouls', 'fouled', 'offsides', 'crosses', 'interceptions', 'tackles_won',
                         'pens_won', 'pens_conceded', 'own_goals', 'ball_recoveries', 'aerials_won',
                         'aerials_lost', 'aerials_won_pct'
                     ]},
                    {'table_name': 'fbref.PlayerStatsPassing',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsPassing'],
                     'columns': [
                         'player_id', 'match_id', 'passes_completed', 'passes', 'passes_pct',
                         'passes_total_distance', 'passes_progressive_distance', 'passes_completed_short',
                         'passes_short', 'passes_pct_short', 'passes_completed_medium', 'passes_medium',
                         'passes_pct_medium', 'passes_completed_long', 'passes_long', 'passes_pct_long',
                         'assists', 'xa', 'assisted_shots', 'passes_into_final_third', 'passes_into_penalty_area',
                         'crosses_into_penalty_area', 'progressive_passes'
                     ]},
                    {'table_name': 'fbref.PlayerStatsPassTypes',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsPassTypes'],
                     'columns': [
                         'player_id', 'match_id', 'passes', 'passes_live', 'passes_dead', 'passes_free_kicks',
                         'through_balls', 'passes_pressure', 'passes_switches', 'crosses', 'corner_kicks',
                         'corner_kicks_in', 'corner_kicks_out', 'corner_kicks_straight', 'passes_ground',
                         'passes_low', 'passes_high', 'passes_left_foot', 'passes_right_foot', 'passes_head',
                         'throw_ins', 'passes_other_body', 'passes_completed', 'passes_offsides', 'passes_oob',
                         'passes_intercepted', 'passes_blocked'
                     ]},
                    {'table_name': 'fbref.PlayerStatsPossession',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsPossession'],
                     'columns': [
                         'player_id', 'match_id', 'touches', 'touches_def_pen_area', 'touches_def_3rd',
                         'touches_mid_3rd', 'touches_att_3rd', 'touches_att_pen_area',
                         'touches_live_ball', 'dribbles_completed', 'dribbles', 'players_dribbled_past', 'nutmegs',
                         'carries', 'carry_distance', 'carry_progressive_distance', 'progressive_carries',
                         'carries_into_final_third', 'carries_into_penalty_area', 'miscontrols', 'dispossessed',
                         'pass_targets', 'passes_received', 'passes_received_pct', 'progressive_passes_received'
                     ]},
                    {'table_name': 'fbref.PlayerStatsSummary',
                     'df': player_stats_transformed_dict['fbref.PlayerStatsSummary'],
                     'columns': [
                         'player_id', 'match_id', 'player', 'nationality', 'shirtnumber', 'position', 'age', 'minutes',
                         'first_squad', 'goals', 'assists', 'pens_made', 'pens_att', 'shots_total', 'shots_on_target',
                         'cards_yellow', 'cards_red', 'touches', 'pressures', 'tackles', 'interceptions', 'blocks', 'xg',
                         'npxg', 'xa', 'sca', 'gca', 'passes_completed', 'passes', 'passes_pct', 'progressive_passes',
                         'carries', 'dribbles_completed', 'dribbles', 'team', 'team_id'
                     ]}
                ]

                for table in loads:
                    try:
                        load_player_stats(**table)
                    except:
                        pass

                self.commit_database()


