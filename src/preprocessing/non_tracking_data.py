import polars as pl

class NonTrackingDataProcessor:
    def __init__(self, 
                 games_file_path: str,
                 plays_file_path: str,
                 player_file_path: str,
                 player_plays_file_path: str) -> None:
        self.games = self.load_data(games_file_path)
        self.plays = self.load_data(plays_file_path)
        self.players = self.load_data(player_file_path)
        self.player_plays = self.load_data(player_plays_file_path)

    def load_data(self) -> pl.DataFrame:
        data = pl.read_csv(self.file_path)
        self.data = data
        return data
    
    
    def _join_games_plays_df(self) -> pl.DataFrame:
        return self.plays.join(self.games, on="gameId", how="left")
    

    def _aggregate_player_plays_df(self) -> pl.DataFrame:
        pass

    def _feature_engineer(self) -> pl.DataFrame:
        games_plays_joined = self._join_games_plays_df()

        # Create possession team features
        games_plays_joined = games_plays_joined.with_column(
            # Get pre snap scores
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapHomeScore")).otherwise(pl.col("preSnapVisitorScore")).alias("preSnapPossessionTeamScore"),
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapVisitorScore")).otherwise(pl.col("preSnapHomeScore")).alias("preSnapDefensiveTeamScore"),
            # Get pre snap win probability
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapHomeTeamWinProbability")).otherwise(pl.col("preSnapVisitorTeamWinProbability")).alias("preSnapPossessionTeamWP"),
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapVisitorTeamWinProbability")).otherwise(pl.col("preSnapHomeTeamWinProbability")).alias("preSnapDefensiveTeamWP"),
            # Get win probability added
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("homeTeamWinProbabilityAdded")).otherwise(pl.col("visitorTeamWinProbilityAdded")).alias("possesionTeamWPAdded"),
            pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("visitorTeamWinProbabilityAdded")).otherwise(pl.col("homeTeamWinProbilityAdded")).alias("defensiveTeamWPAdded"),
            # Add defensive team expected points added
            -1 * pl.col("expectedPointsAdded").alias("defensiveTeamExpectedPointsAdded"),
            # Add score difference
            pl.col("preSnapPossessionTeamScore") - pl.col("preSnapDefensiveTeamScore").alias("scoreDifference"),
        )

        return games_plays_joined

    def process(self):
        pass