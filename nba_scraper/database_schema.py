"""
NBA Data Scraping System - Database Schema Design

Based on analysis of basketball-reference.com, this defines the core schema
for comprehensive NBA data storage with relationships between datasets.

Key Data Types Identified:
- Season summaries (team standings, playoffs, awards)
- Player statistics (per game, totals, advanced, shooting)
- Game logs and box scores
- Team statistics and schedules  
- Player profiles and career stats
- Awards and honors
- Draft data
- Coaching records
- Transactions

Schema supports:
- Dynamic column addition
- Schema change tracking
- Relationships between datasets
- Temporal data (seasons, dates)
- Comprehensive stats coverage
"""

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, DateTime, 
    Boolean, Text, ForeignKey, Index, UniqueConstraint, JSON
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
import uuid

Base = declarative_base()

class Season(Base):
    """NBA seasons - core temporal entity"""
    __tablename__ = 'seasons'
    
    id = Column(Integer, primary_key=True)
    year = Column(Integer, unique=True, nullable=False)  # e.g., 2024 for 2023-24 season
    start_date = Column(Date)
    end_date = Column(Date)
    playoff_start_date = Column(Date)
    playoff_end_date = Column(Date)
    champion_team_id = Column(Integer, ForeignKey('teams.id'))
    mvp_player_id = Column(Integer, ForeignKey('players.id'))
    roy_player_id = Column(Integer, ForeignKey('players.id'))
    
    # Relationships
    champion = relationship("Team", foreign_keys=[champion_team_id])
    mvp = relationship("Player", foreign_keys=[mvp_player_id])
    roy = relationship("Player", foreign_keys=[roy_player_id])
    team_stats = relationship("TeamSeasonStats", back_populates="season")
    player_stats = relationship("PlayerSeasonStats", back_populates="season")
    games = relationship("Game", back_populates="season")

class Team(Base):
    """NBA teams - franchise information"""
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    abbreviation = Column(String(3), unique=True, nullable=False)  # BOS, LAL, etc.
    name = Column(String(100), nullable=False)  # Boston Celtics
    city = Column(String(100), nullable=False)  # Boston
    conference = Column(String(10))  # Eastern, Western
    division = Column(String(20))  # Atlantic, Central, etc.
    founded_year = Column(Integer)
    arena_name = Column(String(200))
    
    # Relationships
    home_games = relationship("Game", foreign_keys="Game.home_team_id", back_populates="home_team")
    away_games = relationship("Game", foreign_keys="Game.away_team_id", back_populates="away_team")
    season_stats = relationship("TeamSeasonStats", back_populates="team")
    players = relationship("PlayerTeamAssociation", back_populates="team")

class Player(Base):
    """NBA players - individual player information"""
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    br_id = Column(String(50), unique=True)  # basketball-reference.com player ID
    name = Column(String(200), nullable=False)
    birth_date = Column(Date)
    birth_place = Column(String(200))
    height_inches = Column(Integer)
    weight_lbs = Column(Integer)
    college = Column(String(200))
    high_school = Column(String(200))
    draft_year = Column(Integer)
    draft_round = Column(Integer)
    draft_pick = Column(Integer)
    draft_team_id = Column(Integer, ForeignKey('teams.id'))
    
    # Relationships
    draft_team = relationship("Team", foreign_keys=[draft_team_id])
    season_stats = relationship("PlayerSeasonStats", back_populates="player")
    game_stats = relationship("PlayerGameStats", back_populates="player")
    team_associations = relationship("PlayerTeamAssociation", back_populates="player")

class Game(Base):
    """Individual NBA games"""
    __tablename__ = 'games'
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    home_team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    away_team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    home_score = Column(Integer)
    away_score = Column(Integer)
    overtime_periods = Column(Integer, default=0)
    game_type = Column(String(20))  # Regular Season, Playoffs, etc.
    playoff_series_id = Column(Integer, ForeignKey('playoff_series.id'))
    attendance = Column(Integer)
    
    # Relationships
    season = relationship("Season", back_populates="games")
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    playoff_series = relationship("PlayoffSeries")
    player_stats = relationship("PlayerGameStats", back_populates="game")
    
    __table_args__ = (
        UniqueConstraint('date', 'home_team_id', 'away_team_id'),
        Index('idx_game_date', 'date'),
        Index('idx_game_teams', 'home_team_id', 'away_team_id'),
    )

class PlayerTeamAssociation(Base):
    """Track which players played for which teams in which seasons"""
    __tablename__ = 'player_team_associations'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    jersey_number = Column(Integer)
    position = Column(String(10))  # PG, SG, SF, PF, C
    
    # Relationships
    player = relationship("Player", back_populates="team_associations")
    team = relationship("Team", back_populates="players")
    season = relationship("Season")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'team_id', 'season_id'),
    )

class TeamSeasonStats(Base):
    """Team statistics for a complete season"""
    __tablename__ = 'team_season_stats'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    
    # Basic season record
    games_played = Column(Integer)
    wins = Column(Integer)
    losses = Column(Integer)
    win_percentage = Column(Float)
    
    # Conference/Division standings
    conference_rank = Column(Integer)
    division_rank = Column(Integer)
    games_behind = Column(Float)
    
    # Basic team stats (per game averages)
    points_per_game = Column(Float)
    points_allowed_per_game = Column(Float)
    field_goals_made = Column(Float)
    field_goals_attempted = Column(Float)
    field_goal_percentage = Column(Float)
    three_pointers_made = Column(Float)
    three_pointers_attempted = Column(Float)
    three_point_percentage = Column(Float)
    free_throws_made = Column(Float)
    free_throws_attempted = Column(Float)
    free_throw_percentage = Column(Float)
    offensive_rebounds = Column(Float)
    defensive_rebounds = Column(Float)
    total_rebounds = Column(Float)
    assists = Column(Float)
    steals = Column(Float)
    blocks = Column(Float)
    turnovers = Column(Float)
    personal_fouls = Column(Float)
    
    # Advanced stats
    offensive_rating = Column(Float)
    defensive_rating = Column(Float)
    net_rating = Column(Float)
    pace = Column(Float)
    effective_field_goal_percentage = Column(Float)
    true_shooting_percentage = Column(Float)
    
    # Playoff info
    made_playoffs = Column(Boolean, default=False)
    playoff_seed = Column(Integer)
    playoff_wins = Column(Integer, default=0)
    playoff_losses = Column(Integer, default=0)
    
    # Relationships
    team = relationship("Team", back_populates="season_stats")
    season = relationship("Season", back_populates="team_stats")
    
    __table_args__ = (
        UniqueConstraint('team_id', 'season_id'),
        Index('idx_team_season', 'team_id', 'season_id'),
    )

class PlayerSeasonStats(Base):
    """Player statistics for a complete season"""
    __tablename__ = 'player_season_stats'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'))  # Primary team if traded mid-season
    
    # Basic counting stats
    games_played = Column(Integer)
    games_started = Column(Integer)
    minutes_per_game = Column(Float)
    field_goals_made = Column(Float)
    field_goals_attempted = Column(Float)
    field_goal_percentage = Column(Float)
    three_pointers_made = Column(Float)
    three_pointers_attempted = Column(Float)
    three_point_percentage = Column(Float)
    two_pointers_made = Column(Float)
    two_pointers_attempted = Column(Float)
    two_point_percentage = Column(Float)
    effective_field_goal_percentage = Column(Float)
    free_throws_made = Column(Float)
    free_throws_attempted = Column(Float)
    free_throw_percentage = Column(Float)
    offensive_rebounds = Column(Float)
    defensive_rebounds = Column(Float)
    total_rebounds = Column(Float)
    assists = Column(Float)
    steals = Column(Float)
    blocks = Column(Float)
    turnovers = Column(Float)
    personal_fouls = Column(Float)
    points = Column(Float)
    
    # Advanced stats
    player_efficiency_rating = Column(Float)
    true_shooting_percentage = Column(Float)
    usage_percentage = Column(Float)
    win_shares = Column(Float)
    win_shares_per_48 = Column(Float)
    box_plus_minus = Column(Float)
    value_over_replacement_player = Column(Float)
    
    # Relationships
    player = relationship("Player", back_populates="season_stats")
    season = relationship("Season", back_populates="player_stats")
    team = relationship("Team")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'season_id', 'team_id'),
        Index('idx_player_season', 'player_id', 'season_id'),
    )

class PlayerGameStats(Base):
    """Individual player statistics for a single game"""
    __tablename__ = 'player_game_stats'
    
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'), nullable=False)
    game_id = Column(Integer, ForeignKey('games.id'), nullable=False)
    team_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    
    # Game participation
    started = Column(Boolean, default=False)
    minutes_played = Column(Integer)  # Total seconds
    
    # Shooting stats
    field_goals_made = Column(Integer, default=0)
    field_goals_attempted = Column(Integer, default=0)
    three_pointers_made = Column(Integer, default=0)
    three_pointers_attempted = Column(Integer, default=0)
    free_throws_made = Column(Integer, default=0)
    free_throws_attempted = Column(Integer, default=0)
    
    # Other stats
    offensive_rebounds = Column(Integer, default=0)
    defensive_rebounds = Column(Integer, default=0)
    assists = Column(Integer, default=0)
    steals = Column(Integer, default=0)
    blocks = Column(Integer, default=0)
    turnovers = Column(Integer, default=0)
    personal_fouls = Column(Integer, default=0)
    points = Column(Integer, default=0)
    plus_minus = Column(Integer)
    
    # Relationships
    player = relationship("Player", back_populates="game_stats")
    game = relationship("Game", back_populates="player_stats")
    team = relationship("Team")
    
    __table_args__ = (
        UniqueConstraint('player_id', 'game_id'),
        Index('idx_player_game', 'player_id', 'game_id'),
    )

class PlayoffSeries(Base):
    """NBA playoff series information"""
    __tablename__ = 'playoff_series'
    
    id = Column(Integer, primary_key=True)
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    round_name = Column(String(100))  # First Round, Conference Semifinals, etc.
    team1_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    team2_id = Column(Integer, ForeignKey('teams.id'), nullable=False)
    team1_wins = Column(Integer, default=0)
    team2_wins = Column(Integer, default=0)
    series_winner_id = Column(Integer, ForeignKey('teams.id'))
    
    # Relationships
    season = relationship("Season")
    team1 = relationship("Team", foreign_keys=[team1_id])
    team2 = relationship("Team", foreign_keys=[team2_id])
    winner = relationship("Team", foreign_keys=[series_winner_id])

class Award(Base):
    """NBA awards and honors"""
    __tablename__ = 'awards'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)  # MVP, DPOY, ROY, etc.
    season_id = Column(Integer, ForeignKey('seasons.id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.id'))
    team_id = Column(Integer, ForeignKey('teams.id'))  # For team awards
    
    # Relationships
    season = relationship("Season")
    player = relationship("Player")
    team = relationship("Team")

class ScrapingJob(Base):
    """Track scraping job metadata"""
    __tablename__ = 'scraping_jobs'
    
    id = Column(Integer, primary_key=True)
    job_id = Column(String(50), unique=True, default=lambda: str(uuid.uuid4()))
    dataset_name = Column(String(100), nullable=False)  # team_stats, player_stats, etc.
    url = Column(Text, nullable=False)
    status = Column(String(20), default='pending')  # pending, running, completed, failed
    started_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime)
    error_message = Column(Text)
    records_scraped = Column(Integer, default=0)
    
    __table_args__ = (
        Index('idx_scraping_job_status', 'status'),
        Index('idx_scraping_job_dataset', 'dataset_name'),
    )

class SchemaChange(Base):
    """Track dynamic schema changes and migrations"""
    __tablename__ = 'schema_changes'
    
    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), nullable=False)
    column_name = Column(String(100), nullable=False)
    operation = Column(String(20), nullable=False)  # add, remove, modify
    old_definition = Column(JSON)  # Column definition before change
    new_definition = Column(JSON)  # Column definition after change
    migration_applied = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    applied_at = Column(DateTime)
    
    __table_args__ = (
        Index('idx_schema_change_table', 'table_name'),
        Index('idx_schema_change_status', 'migration_applied'),
    )

# Dynamic table for storing intermediate/test data
class IntermediateData(Base):
    """Flexible table for storing scraped data before final processing"""
    __tablename__ = 'intermediate_data'
    
    id = Column(Integer, primary_key=True)
    job_id = Column(String(50), ForeignKey('scraping_jobs.job_id'), nullable=False)
    table_name = Column(String(100), nullable=False)  # Target table name
    source_url = Column(Text, nullable=False)
    raw_data = Column(JSON, nullable=False)  # Scraped data as JSON
    processed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    processed_at = Column(DateTime)
    
    # Relationships
    scraping_job = relationship("ScrapingJob")
    
    __table_args__ = (
        Index('idx_intermediate_job', 'job_id'),
        Index('idx_intermediate_processed', 'processed'),
        Index('idx_intermediate_table', 'table_name'),
    )

def create_database_engine(database_url="postgresql://user:pass@localhost/nba_data"):
    """
    Create database engine and all tables
    
    Args:
        database_url: SQLAlchemy database URL
        
    Returns:
        SQLAlchemy engine and session maker
    """
    engine = create_engine(database_url, echo=False)
    Base.metadata.create_all(engine)
    SessionMaker = sessionmaker(bind=engine)
    return engine, SessionMaker

def get_or_create_season(session, year):
    """Get or create a season record"""
    season = session.query(Season).filter_by(year=year).first()
    if not season:
        season = Season(year=year)
        session.add(season)
        session.commit()
    return season

def get_or_create_team(session, abbreviation, name, city):
    """Get or create a team record"""
    team = session.query(Team).filter_by(abbreviation=abbreviation).first()
    if not team:
        team = Team(abbreviation=abbreviation, name=name, city=city)
        session.add(team)
        session.commit()
    return team

def get_or_create_player(session, name, br_id=None):
    """Get or create a player record"""
    if br_id:
        player = session.query(Player).filter_by(br_id=br_id).first()
    else:
        player = session.query(Player).filter_by(name=name).first()
    
    if not player:
        player = Player(name=name, br_id=br_id)
        session.add(player)
        session.commit()
    return player

if __name__ == "__main__":
    # Create SQLite database for development
    engine, SessionMaker = create_database_engine("sqlite:///nba_data.db")
    print("Database schema created successfully!")
    print(f"Tables created: {list(Base.metadata.tables.keys())}")