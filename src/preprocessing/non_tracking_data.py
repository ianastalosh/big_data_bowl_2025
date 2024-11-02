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

    def load_data(self, file_path) -> pl.DataFrame:
        data = pl.scan_csv(file_path, ignore_errors=True)
        return data
    
    
    def _join_games_plays_df(self) -> pl.DataFrame:
        return self.plays.join(self.games, on="gameId", how="left")
    

    def _aggregate_player_plays_df(self) -> pl.DataFrame:
        """
        Aggregate the player_plays df to get information on the number 
        of players in motion/shifts at snap and lineset per play

        """
        team_level_player_play_summary = self.player_plays.\
            group_by(["gameId", "playId", "teamAbbr"]).\
            agg((pl.col("inMotionAtBallSnap") == "TRUE").sum().alias("playNumPlayersInMotionAtSnap"),
                (pl.col("shiftSinceLineset") == "TRUE").sum().alias("playNumPlayersShiftSinceLineset"),
                (pl.col("motionSinceLineset") == "TRUE").sum().alias("playNumPlayersMotionSinceLineset"),
                pl.col("yardageGainedAfterTheCatch").sum().alias("playYardageGainedAfterTheCatch"),
                (pl.col("wasRunningRoute") == "1").sum().alias("playNumRoutesRun"),
                ((pl.col("motionSinceLineset") == "TRUE") & (pl.col("wasTargettedReceiver") == 1)).sum().alias("playerMotionWasTargetted"),
                ((pl.col("inMotionAtBallSnap") == "TRUE") & (pl.col("wasTargettedReceiver") == 1)).sum().alias("playerInMotionAtSnapWasTargetted")).\
            with_columns(
                (pl.col("playNumPlayersInMotionAtSnap") > 0).alias("playHadPlayersInMotionAtSnap"),
                (pl.col("playNumPlayersMotionSinceLineset") > 0).alias("playHadPlayersMotionSinceLineset"),
                (pl.col("playNumPlayersShiftSinceLineset") > 0).alias("playHadPlayersShiftSinceLineset")
        )

        return team_level_player_play_summary
    
    def _create_possession_team_player_play_features(self, aggregated_player_play_df) -> pl.DataFrame:
        possession_team_df = aggregated_player_play_df.select(
            gameId=pl.col("gameId"), 
            playId=pl.col("playId"), 
            possessionTeam = pl.col("teamAbbr"),
            possessionTeamNumPlayersInMotionAtSnap = pl.col("playNumPlayersInMotionAtSnap"),
            possessionTeamNumPlayersShiftSinceLineset = pl.col("playNumPlayersShiftSinceLineset"),
            possessionTeamNumPlayersMotionSinceLineset = pl.col("playNumPlayersMotionSinceLineset"),
            playerMotionWasTargetted = pl.col("playerMotionWasTargetted"),
            playerInMotionAtSnapWasTargetted = pl.col("playerInMotionAtSnapWasTargetted"),
        )

        defensive_team_df = aggregated_player_play_df.select(
            gameId=pl.col("gameId"), 
            playId=pl.col("playId"), 
            defensiveTeam = pl.col("teamAbbr"),
            defensiveTeamNumPlayersInMotionAtSnap = pl.col("playNumPlayersInMotionAtSnap"),
            defensiveTeamNumPlayersShiftSinceLineset = pl.col("playNumPlayersShiftSinceLineset"),
            defensiveTeamNumPlayersMotionSinceLineset = pl.col("playNumPlayersMotionSinceLineset")
        )

        return possession_team_df, defensive_team_df
    

    def feature_engineer_play_data(self) -> pl.DataFrame:
        games_plays_joined = self._join_games_plays_df()
        aggregated_player_plays = self._aggregate_player_plays_df()

        # Create possession team features
        output_data = games_plays_joined.with_columns(
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

        # Join the aggregated player plays data
        possession_player_plays, defensive_player_plays = self._create_possession_team_player_play_features(aggregated_player_plays)

        output_data = output_data.\
            join(possession_player_plays, on=["gameId", "playId", "possessionTeam"], how="left").\
            join(defensive_player_plays, on=["gameId", "playId", "defensiveTeam"], how="left")

        return output_data

    def process(self):
        pass