"""Example module showing doctests with Sphinx integration."""

def calculate_player_efficiency(points, minutes, games):
    """Calculate a simple player efficiency metric.
    
    This function demonstrates how doctests work with Sphinx documentation.
    The examples below will be automatically tested when doctests run.
    
    Args:
        points (int): Total points scored
        minutes (int): Total minutes played  
        games (int): Number of games played
        
    Returns:
        float: Efficiency rating (points per minute per game)
        
    Examples:
        Basic efficiency calculation:
        
        >>> calculate_player_efficiency(100, 200, 5)
        0.1
        
        Higher efficiency player:
        
        >>> calculate_player_efficiency(250, 300, 10) 
        0.08333333333333333
        
        Edge case with zero games (should raise an error):
        
        >>> calculate_player_efficiency(100, 200, 0)
        Traceback (most recent call last):
            ...
        ValueError: Cannot calculate efficiency with zero games
        
        Example with ellipsis for long output:
        
        >>> result = {'player': 'LeBron James', 'stats': [25, 30, 28, 35]}
        >>> result  # doctest: +ELLIPSIS
        {'player': 'LeBron James', ...}
        
        Multi-line statement with continuation:
        
        >>> efficiency = calculate_player_efficiency(
        ...     points=150,
        ...     minutes=240, 
        ...     games=6
        ... )
        >>> round(efficiency, 4)
        0.1042
    """
    if games == 0:
        raise ValueError("Cannot calculate efficiency with zero games")
    
    return points / (minutes * games)


class PlayerStats:
    """A simple class to demonstrate doctests with class methods."""
    
    def __init__(self, name):
        """Initialize player stats.
        
        Args:
            name (str): Player name
            
        Examples:
            >>> player = PlayerStats("LeBron James")
            >>> player.name
            'LeBron James'
            >>> player.total_points
            0
        """
        self.name = name
        self.total_points = 0
        self.games_played = 0
    
    def add_game_stats(self, points):
        """Add stats from a single game.
        
        Args:
            points (int): Points scored in the game
            
        Examples:
            >>> player = PlayerStats("Kobe Bryant")
            >>> player.add_game_stats(24)
            >>> player.total_points
            24
            >>> player.games_played
            1
            >>> player.add_game_stats(35)
            >>> player.total_points
            59
            >>> player.games_played 
            2
        """
        self.total_points += points
        self.games_played += 1
    
    def average_points(self):
        """Calculate average points per game.
        
        Returns:
            float: Average points per game, or 0 if no games played
            
        Examples:
            >>> player = PlayerStats("Michael Jordan")
            >>> player.average_points()  # No games yet
            0.0
            >>> player.add_game_stats(30)
            >>> player.add_game_stats(25)
            >>> player.average_points()  # TEST: Should be 27.5
            27.5
        """
        if self.games_played == 0:
            return 0.0
        return self.total_points / self.games_played


def complex_calculation(player_data):
    """Demonstrate ellipsis usage in doctests.
    
    Args:
        player_data (dict): Dictionary with player information
        
    Returns:
        dict: Processed player statistics
        
    Examples:
        Simple ellipsis for long function calls:
        
        >>> result = complex_calculation({
        ...     'name': 'Kobe Bryant',
        ...     'points': [24, 35, 42],
        ...     'minutes': [38, 42, 45]
        ... })
        >>> result['name']
        'Kobe Bryant'
        
        Using ellipsis to show partial dictionary output:
        
        >>> player_stats = complex_calculation({'name': 'LeBron', 'points': [25, 30], 'minutes': [40, 38]})
        >>> player_stats  # doctest: +ELLIPSIS
        {...'efficiency': ...}
        
        Multi-line statement continuation:
        
        >>> efficiency = calculate_player_efficiency(
        ...     points=100,
        ...     minutes=200, 
        ...     games=5
        ... )
        >>> efficiency
        0.1
    """
    total_points = sum(player_data.get('points', []))
    total_minutes = sum(player_data.get('minutes', []))
    games = len(player_data.get('points', []))
    
    return {
        'name': player_data['name'],
        'total_points': total_points,
        'total_minutes': total_minutes,
        'games': games,
        'efficiency': total_points / total_minutes if total_minutes > 0 else 0
    }