CREATE DATABASE football;
GO

USE football;
GO

CREATE SCHEMA fbref;
GO

CREATE TABLE fbref.Competitions
(
    id              INT          NOT NULL,
    season          VARCHAR(30)  NOT NULL,
    league          VARCHAR(30)  NOT NULL,
    url             VARCHAR(200) NOT NULL,
    CONSTRAINT PK_Competitions_id
        PRIMARY KEY (id)                 ,
    CONSTRAINT AK_url
        UNIQUE (url)
);

CREATE TABLE fbref.Matches
(
    id             INT          NOT NULL,
    gameweek       INT                  ,
    dayofweek      VARCHAR(30)          ,
    date           DATETIME2            ,
    time           VARCHAR(30)          ,
    home_team_id   VARCHAR(50)          ,
    home_team      VARCHAR(30)          ,
    home_team_country VARCHAR(10)       ,
    xg_home        FLOAT(2)             ,
    home_score     INT                  ,
    away_score     INT                  ,
    xg_away        FLOAT(2)             ,
    away_team_id   VARCHAR(50)          ,
    away_team      VARCHAR(30)          ,
    away_team_country VARCHAR(10)       ,
    attendance     INT                  ,
    venue          VARCHAR(200)         ,
    referee        VARCHAR(50)          ,
    match_report   VARCHAR(200)         ,
    notes          VARCHAR(300)         ,
    competition_id INT                  ,
    home_score_penalty_shootout INT     ,
    away_score_penalty_shootout INT     ,
    CONSTRAINT PK_Matches_id
        PRIMARY KEY (id)                ,
    CONSTRAINT FK_Matches_Competitions
        FOREIGN KEY (competition_id)
        REFERENCES fbref.Competitions (id),
    CONSTRAINT AK_match_report
        UNIQUE (match_report)
);

CREATE TABLE fbref.PlayerStatsSummary
(
    player_id               VARCHAR(50)  NOT NULL,
    player                  VARCHAR(50)          ,
    nationality             VARCHAR(50)          ,
    match_id                INT          NOT NULL,
    shirtnumber             INT                  ,
    position                VARCHAR(20)          ,
    age                     FLOAT(4)             ,
    minutes                 INT                  ,
    first_squad             BIT          NOT NULL,
    goals                   INT                  ,
    assists                 INT                  ,
    pens_made               INT                  ,
    pens_att                INT                  ,
    shots_total             INT                  ,
    shots_on_target         INT                  ,
    cards_yellow            INT                  ,
    cards_red               INT                  ,
    touches                 INT                  ,
    pressures               INT                  ,
    tackles                 INT                  ,
    interceptions           INT                  ,
    blocks                  INT                  ,
    xg                      FLOAT(2)             ,
    npxg                    FLOAT(2)             ,
    xa                      FLOAT(2)             ,
    sca                     INT                  ,
    gca                     INT                  ,
    passes_completed        INT                  ,
    passes                  INT                  ,
    passes_pct              FLOAT(3)             ,
    progressive_passes      INT                  ,
    carries                 INT                  ,
    progressive_carries     INT                  ,
    dribbles_completed      INT                  ,
    dribbles                INT                  ,
    team_id                 VARCHAR(50)          ,
    team                    VARCHAR(30)          ,
    CONSTRAINT PK_PlayerStatsSummary
        PRIMARY KEY (player_id, match_id)        ,
    CONSTRAINT FK_PlayerStatsSummary_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.PlayerStatsPassing
(
    player_id                       VARCHAR(50)  NOT NULL,
    match_id                        INT          NOT NULL,
    passes_completed                INT                 ,
    passes                          INT                 ,
    passes_pct                      FLOAT(3)            ,
    passes_total_distance           INT                 ,
    passes_progressive_distance     INT                 ,
    passes_completed_short          INT                 ,
    passes_short                    INT                 ,
    passes_pct_short                FLOAT(3)            ,
    passes_completed_medium         INT                 ,
    passes_medium                   INT                 ,
    passes_pct_medium               FLOAT(3)            ,
    passes_completed_long           INT                 ,
    passes_long                     INT                 ,
    passes_pct_long                 FLOAT(3)            ,
    assists                         INT                 ,
    xa                              FLOAT(2)            ,
    assisted_shots                  INT                 ,
    passes_into_final_third         INT                 ,
    passes_into_penalty_area        INT                 ,
    crosses_into_penalty_area       INT                 ,
    progressive_passes              INT                 ,
    CONSTRAINT PK_PlayerStatsPassing
        PRIMARY KEY (player_id, match_id)               ,
    CONSTRAINT FK_PlayerStatsPassing_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.PlayerStatsPossession
(
    player_id                       VARCHAR(50) NOT NULL,
    match_id                        INT         NOT NULL,
    touches                         INT                 ,
    touches_def_pen_area            INT                 ,
    touches_def_3rd                 INT                 ,
    touches_mid_3rd                 INT                 ,
    touches_att_3rd                 INT                 ,
    touches_att_pen_area            INT                 ,
    touches_live_ball               INT                 ,
    dribbles_completed              INT                 ,
    dribbles                        INT                 ,
    dribbles_completed_pct          FLOAT(3)            ,
    players_dribbled_past           INT                 ,
    nutmegs                         INT                 ,
    carries                         INT                 ,
    carry_distance                  INT                 ,
    carry_progressive_distance      INT                 ,
    progressive_carries             INT                 ,
    carries_into_final_third        INT                 ,
    carries_into_penalty_area       INT                 ,
    miscontrols                     INT                 ,
    dispossessed                    INT                 ,
    pass_targets                    INT                 ,
    passes_received                 INT                 ,
    passes_received_pct             FLOAT(3)            ,
    progressive_passes_received     INT                 ,
    CONSTRAINT PK_PlayerStatsPossession
        PRIMARY KEY (player_id, match_id)               ,
    CONSTRAINT FK_PlayerStatsPossession_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.Shots
(
    id                  INT    IDENTITY(1,1),
    player_id           VARCHAR(50) NOT NULL,
    minute              INT                 ,
    additional_time     INT                 ,
    player              VARCHAR(50)         ,
    team_id             VARCHAR(50)         ,
    team                VARCHAR(50)         ,
    outcome             VARCHAR(50)         ,
    distance            INT                 ,
    body_part           VARCHAR(50)         ,
    notes               VARCHAR(100)        ,
    sca_1_player        VARCHAR(50)         ,
    sca_1_type          VARCHAR(50)         ,
    sca_2_player        VARCHAR(50)         ,
    sca_2_type          VARCHAR(50)         ,
    match_id            INT                 ,
    penalty             BIT                 ,
    CONSTRAINT PK_Shots_id
        PRIMARY KEY (id)                    ,
    CONSTRAINT FK_Shots_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.PlayerStatsPassTypes
(
    player_id               VARCHAR(50)  NOT NULL,
    match_id                INT          NOT NULL,
    passes                  INT                  ,
    passes_live             INT                  ,
    passes_dead             INT                  ,
    passes_free_kicks       INT                  ,
    through_balls           INT                  ,
    passes_pressure         INT                  ,
    passes_switches         INT                  ,
    crosses                 INT                  ,
    corner_kicks            INT                  ,
    corner_kicks_in         INT                  ,
    corner_kicks_out        INT                  ,
    corner_kicks_straight   INT                  ,
    passes_ground           INT                  ,
    passes_low              INT                  ,
    passes_high             INT                  ,
    passes_left_foot        INT                  ,
    passes_right_foot       INT                  ,
    passes_head             INT                  ,
    throw_ins               INT                  ,
    passes_other_body       INT                  ,
    passes_completed        INT                  ,
    passes_offsides         INT                  ,
    passes_oob              INT                  ,
    passes_intercepted      INT                  ,
    passes_blocked          INT                  ,
    CONSTRAINT PK_PlayerStatsPassTypes
        PRIMARY KEY (player_id, match_id)        ,
    CONSTRAINT FK_PlayerStatsPassTypes_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.GoalkeeperStats
(
    id                                  INT        IDENTITY(1,1),
    player_id                           VARCHAR(50)     NOT NULL,
    age                                 FLOAT(4)                ,
    minutes                             INT                     ,
    match_id                            INT             NOT NULL,
    shots_on_target_against             INT                     ,
    goals_against_gk                    INT                     ,
    saves                               INT                     ,
    save_pct                            FLOAT(3)                ,
    psxg_gk                             FLOAT(3)                ,
    passes_completed_launched_gk        INT                     ,
    passes_launched_gk                  INT                     ,
    passes_pct_launched_gk              FLOAT(3)                ,
    passes_gk                           INT                     ,
    passes_throws_gk                    INT                     ,
    pct_passes_launched_gk              FLOAT(3)                ,
    passes_length_avg_gk                FLOAT(3)                ,
    goal_kicks                          INT                     ,
    pct_goal_kicks_launched             FLOAT(3)                ,
    goal_kick_length_avg                FLOAT(3)                ,
    crosses_gk                          INT                     ,
    crosses_stopped_gk                  INT                     ,
    crosses_stopped_pct_gk              FLOAT(3)                ,
    def_actions_outside_pen_area_gk     INT                     ,
    avg_distance_def_actions_gk         FLOAT(3)                ,
    CONSTRAINT PK_GoalkeeperStats_id
        PRIMARY KEY (id)                                        ,
    CONSTRAINT FK_GoalkeeperStats_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.PlayerStatsMisc
(
    player_id               VARCHAR(50)  NOT NULL,
    match_id                INT          NOT NULL,
    cards_yellow            INT                 ,
    cards_red               INT                 ,
    cards_yellow_red        INT                 ,
    fouls                   INT                 ,
    fouled                  INT                 ,
    offsides                INT                 ,
    crosses                 INT                 ,
    interceptions           INT                 ,
    tackles_won             INT                 ,
    pens_won                INT                 ,
    pens_conceded           INT                 ,
    own_goals               INT                 ,
    ball_recoveries         INT                 ,
    aerials_won             INT                 ,
    aerials_lost            INT                 ,
    aerials_won_pct         FLOAT(3)            ,
    CONSTRAINT PK_PlayerStatsMisc
        PRIMARY KEY (player_id, match_id)       ,
    CONSTRAINT FK_PlayerStatsMisc_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);

CREATE TABLE fbref.PlayerStatsDefActions
(
    player_id                   VARCHAR(50)     NOT NULL,
    match_id                    INT             NOT NULL,
    tackles                     INT                     ,
    tackles_won                 INT                     ,
    tackles_def_3rd             INT                     ,
    tackles_mid_3rd             INT                     ,
    tackles_att_3rd             INT                     ,
    dribble_tackles             INT                     ,
    dribbles_vs                 INT                     ,
    dribble_tackles_pct         FLOAT(3)                ,
    dribbled_past               INT                     ,
    pressures                   INT                     ,
    pressure_regains            INT                     ,
    pressure_regain_pct         FLOAT(3)                ,
    pressures_def_3rd           INT                     ,
    pressures_mid_3rd           INT                     ,
    pressures_att_3rd           INT                     ,
    blocks                      INT                     ,
    blocked_shots               INT                     ,
    blocked_shots_saves         INT                     ,
    blocked_passes              INT                     ,
    interceptions               INT                     ,
    tackles_interceptions       INT                     ,
    clearances                  INT                     ,
    errors                      INT                     ,
    CONSTRAINT PK_PlayerStatsDefActions
        PRIMARY KEY (player_id, match_id)               ,
    CONSTRAINT FK_PlayerStatsDefActions_Matches
        FOREIGN KEY (match_id)
        REFERENCES fbref.Matches (id)
);