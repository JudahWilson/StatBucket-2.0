from django.db import models


class Actionmap(models.Model):
    description = models.CharField(max_length=64)
    play_map = models.ForeignKey('Playmap', models.DO_NOTHING, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'actionmap'


class Coaches(models.Model):
    br_id = models.CharField(unique=True, max_length=10, blank=True, null=True)
    first_name = models.CharField(max_length=35, blank=True, null=True)
    last_name = models.CharField(max_length=35, blank=True, null=True)
    birthdate = models.DateField(blank=True, null=True)
    origin_city = models.CharField(max_length=35, blank=True, null=True)
    origin_territory = models.CharField(max_length=35, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'coaches'


class Coachstates(models.Model):
    coach_br = models.ForeignKey(Coaches, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'coachstates'


class Games(models.Model):
    br_id = models.CharField(unique=True, max_length=12, blank=True, null=True)
    season_br_id = models.CharField(max_length=8, blank=True, null=True)
    home_team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    away_team_br_id = models.CharField(max_length=3, blank=True, null=True)
    home_team_points = models.IntegerField(blank=True, null=True)
    away_team_points = models.IntegerField(blank=True, null=True)
    date_time = models.DateTimeField(blank=True, null=True)
    attendance = models.IntegerField(blank=True, null=True)
    duration_minutes = models.IntegerField(blank=True, null=True)
    arena = models.CharField(max_length=65, blank=True, null=True)
    ot = models.CharField(max_length=3, blank=True, null=True)
    url = models.CharField(max_length=200, blank=True, null=True)
    inactive_players = models.TextField(blank=True, null=True)
    officials = models.TextField(blank=True, null=True)
    game_duration = models.CharField(max_length=5, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'games'

    def __str__(self):
        return self.date_time.strftime("%Y-%m-%d %H:%M:%S") + ' - ' + self.home_team_br_id + ' vs ' + self.away_team_br_id


class Playactions(models.Model):
    play_id = models.IntegerField(blank=True, null=True)
    player_br = models.ForeignKey('Players', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    action_code = models.CharField(max_length=60, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playactions'


class Playergamehalfstats(models.Model):
    player_br = models.ForeignKey('Players', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    half = models.IntegerField(blank=True, null=True)
    opponent_br_id = models.CharField(max_length=3, blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    seconds_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    plus_minus = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playergamehalfstats'


class Playergameovertimestats(models.Model):
    player_br = models.ForeignKey('Players', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    opponent_br_id = models.CharField(max_length=3, blank=True, null=True)
    overtime = models.IntegerField(blank=True, null=True)
    seconds_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    plus_minus = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playergameovertimestats'


class Playergamequarterstats(models.Model):
    player_br = models.ForeignKey('Players', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    opponent_br_id = models.CharField(max_length=3, blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    seconds_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    plus_minus = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playergamequarterstats'


class Playergamestats(models.Model):
    player_br = models.ForeignKey('Players', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    opponent_team_br_id = models.CharField(max_length=3, blank=True, null=True)
    played = models.IntegerField(blank=True, null=True)
    reason_for_absence = models.CharField(max_length=20, blank=True, null=True)
    started = models.IntegerField(blank=True, null=True)
    seconds_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    plus_minus = models.IntegerField(blank=True, null=True)
    true_shooting_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    effective_field_goal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    three_point_attempt_rate = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throw_attempt_rate = models.DecimalField(max_digits=6, decimal_places=3, blank=True, null=True)
    offensive_rebound_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    defensive_rebound_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    total_rebound_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    assist_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    steal_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    block_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    turnover_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    usage_percentage = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    offensive_rating = models.IntegerField(blank=True, null=True)
    defensive_rating = models.IntegerField(blank=True, null=True)
    box_plus_minus = models.DecimalField(max_digits=4, decimal_places=1, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playergamestats'


class Players(models.Model):
    br_id = models.CharField(unique=True, max_length=9, blank=True, null=True, db_comment='TEST')
    first_name = models.CharField(max_length=35, blank=True, null=True)
    last_name = models.CharField(max_length=35, blank=True, null=True)
    city = models.CharField(max_length=30, blank=True, null=True)
    territory = models.CharField(max_length=30, blank=True, null=True)
    country = models.CharField(max_length=30, blank=True, null=True)
    full_name = models.CharField(max_length=50, blank=True, null=True)
    suffix = models.CharField(max_length=5, blank=True, null=True)
    year_start = models.CharField(max_length=4, blank=True, null=True)
    year_end = models.CharField(max_length=4, blank=True, null=True)
    position = models.CharField(max_length=5, blank=True, null=True)
    height_str = models.CharField(max_length=4, blank=True, null=True)
    height_in = models.IntegerField(blank=True, null=True)
    weight = models.IntegerField(blank=True, null=True)
    birth_date = models.DateField(blank=True, null=True)
    colleges = models.JSONField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'players'


class Playerstates(models.Model):
    player_br_id = models.CharField(max_length=9, blank=True, null=True)
    team_id = models.IntegerField(blank=True, null=True)
    jersey_no = models.IntegerField(blank=True, null=True)
    position = models.CharField(max_length=3, blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playerstates'


class Playloaderrors(models.Model):
    url = models.CharField(max_length=200, blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    time = models.CharField(max_length=7, blank=True, null=True)
    html = models.CharField(max_length=300, blank=True, null=True)
    error = models.TextField(blank=True, null=True)
    traceback = models.TextField(blank=True, null=True)
    is_play_not_yet_supported_error = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playloaderrors'


class Playmap(models.Model):
    content_format = models.CharField(max_length=200, db_comment='the structure of the pbp row. \r\n- {FT} resembles feet variable.\r\n- {PL} resembles player variable.\r\n- {TM} resembles team variable.')
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'playmap'


class Plays(models.Model):
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    clock_time = models.CharField(max_length=7, blank=True, null=True)
    distance_feet = models.IntegerField(blank=True, null=True)
    home_score = models.IntegerField()
    away_score = models.IntegerField()
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'plays'


class Seasons(models.Model):
    season_start_year = models.CharField(max_length=4, blank=True, null=True)
    season_end_year = models.CharField(max_length=4, blank=True, null=True)
    league = models.CharField(max_length=3, blank=True, null=True)
    champion = models.IntegerField(blank=True, null=True)
    mvp = models.IntegerField(blank=True, null=True)
    roy = models.IntegerField(blank=True, null=True)
    scoring_leader = models.IntegerField(blank=True, null=True)
    rebounds_leader = models.IntegerField(blank=True, null=True)
    assists_leader = models.IntegerField(blank=True, null=True)
    win_shares_leader = models.IntegerField(blank=True, null=True)
    br_id = models.CharField(max_length=8, blank=True, null=True)
    active = models.CharField(max_length=8, blank=True, null=True)
    champion_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    mvp_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    roy_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', related_name='seasons_roy_br_set', blank=True, null=True)
    scoring_leader_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', related_name='seasons_scoring_leader_br_set', blank=True, null=True)
    rebounding_leader_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', related_name='seasons_rebounding_leader_br_set', blank=True, null=True)
    assists_leader_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', related_name='seasons_assists_leader_br_set', blank=True, null=True)
    winshares_leader_br = models.ForeignKey(Players, models.DO_NOTHING, to_field='br_id', related_name='seasons_winshares_leader_br_set', blank=True, null=True)
    scoring_leader_points = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'seasons'


class Teamgamehalfstats(models.Model):
    game_id = models.IntegerField(blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    half = models.IntegerField(blank=True, null=True)
    minutes_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'teamgamehalfstats'


class Teamgameovertimestats(models.Model):
    game_id = models.IntegerField(blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    overtime = models.IntegerField(blank=True, null=True)
    minutes_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'teamgameovertimestats'


class Teamgamequarterstats(models.Model):
    game_id = models.IntegerField(blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    quarter = models.IntegerField(blank=True, null=True)
    minutes_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'teamgamequarterstats'


class Teamgamestats(models.Model):
    game_id = models.IntegerField(blank=True, null=True)
    game_br = models.ForeignKey(Games, models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    team_br = models.ForeignKey('Teams', models.DO_NOTHING, to_field='br_id', blank=True, null=True)
    minutes_played = models.IntegerField(blank=True, null=True)
    field_goals = models.IntegerField(blank=True, null=True)
    field_goal_attempts = models.IntegerField(blank=True, null=True)
    field_goal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    three_pointers = models.IntegerField(blank=True, null=True)
    three_pointer_attempts = models.IntegerField(blank=True, null=True)
    three_pointer_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    free_throws = models.IntegerField(blank=True, null=True)
    free_throw_attempts = models.IntegerField(blank=True, null=True)
    free_throw_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    rebounds = models.IntegerField(blank=True, null=True)
    offensive_rebounds = models.IntegerField(blank=True, null=True)
    defensive_rebounds = models.IntegerField(blank=True, null=True)
    assists = models.IntegerField(blank=True, null=True)
    steals = models.IntegerField(blank=True, null=True)
    blocks = models.IntegerField(blank=True, null=True)
    turnovers = models.IntegerField(blank=True, null=True)
    personal_fouls = models.IntegerField(blank=True, null=True)
    points = models.IntegerField(blank=True, null=True)
    true_shooting_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    effective_field_goal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    three_point_attempt_rate = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    free_throw_attempt_rate = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    offensive_rebound_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    defensive_rebound_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    total_rebound_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    assist_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    steal_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    block_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    turnover_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    usage_percentage = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    offensive_rating = models.IntegerField(blank=True, null=True)
    defensive_rating = models.IntegerField(blank=True, null=True)
    pace_factor = models.DecimalField(max_digits=4, decimal_places=1, blank=True, null=True)
    ft_per_fga = models.DecimalField(max_digits=4, decimal_places=3, blank=True, null=True)
    inactive_players = models.TextField(blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'teamgamestats'


class Teams(models.Model):
    name = models.CharField(max_length=50, blank=True, null=True)
    br_id = models.CharField(unique=True, max_length=3, blank=True, null=True)
    league = models.CharField(max_length=3, blank=True, null=True)
    season_start_year = models.CharField(max_length=4, blank=True, null=True)
    season_end_year = models.CharField(max_length=4, blank=True, null=True)
    location = models.CharField(max_length=30, blank=True, null=True)
    dynamic = models.JSONField(blank=True, null=True, db_comment='Columns not yet added to table store into a json object in this column.')

    class Meta:
        db_table = 'teams'
