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
            with_columns(
                motionOrShiftSinceLineset=((pl.col("motionSinceLineset") == "TRUE") | (pl.col("shiftSinceLineset") == "TRUE"))).\
            group_by(["gameId", "playId", "teamAbbr"]).\
            agg(playNumPlayersInMotionAtSnap=(pl.col("inMotionAtBallSnap") == "TRUE").sum(),
                playNumPlayersShiftSinceLineset=(pl.col("shiftSinceLineset") == "TRUE").sum(),
                playNumPlayersMotionSinceLineset=(pl.col("motionSinceLineset") == "TRUE").sum(),
                playNumPlayersMotionOrShiftSinceLineset=(pl.col("motionOrShiftSinceLineset") == True).sum(),
                playYardageGainedAfterTheCatch=pl.col("yardageGainedAfterTheCatch").sum(),
                playNumRoutesRun=(pl.col("wasRunningRoute") == "1").sum(),
                playerMotionWasTargetted=((pl.col("motionSinceLineset") == "TRUE") & (pl.col("wasTargettedReceiver") == 1)).sum(),
                playerInMotionAtSnapWasTargetted=((pl.col("inMotionAtBallSnap") == "TRUE") & (pl.col("wasTargettedReceiver") == 1)).sum()).\
            with_columns(
                playHadPlayersInMotionAtSnap=(pl.col("playNumPlayersInMotionAtSnap") > 0),
                playHadPlayersMotionSinceLineset=(pl.col("playNumPlayersMotionSinceLineset") > 0),
                playHadPlayersShiftSinceLineset=(pl.col("playNumPlayersShiftSinceLineset") > 0),
                playhadPlayersMotionOrShiftSinceLineset=(pl.col("playNumPlayersMotionOrShiftSinceLineset") > 0),
                playHadMotion=(pl.when(pl.col("playNumPlayersInMotionAtSnap") + pl.col("playNumPlayersMotionOrShiftSinceLineset") > 0).then(1).otherwise(0)),
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
            playHadPlayersInMotionAtSnap=pl.col("playHadPlayersInMotionAtSnap"),
            playHadPlayersMotionSinceLineset=pl.col("playHadPlayersMotionSinceLineset"),
            playHadPlayersShiftSinceLineset=pl.col("playHadPlayersShiftSinceLineset"),
            playhadPlayersMotionOrShiftSinceLineset=pl.col("playhadPlayersMotionOrShiftSinceLineset"),
            playHadMotion=pl.col("playHadMotion"),
            
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
            # Get distance to the endzone
            distanceToEndzone=pl.when(pl.col("possessionTeam") == pl.col("yardlineSide")).then(100 - pl.col("yardlineNumber")).otherwise(pl.col("yardlineNumber")),
            # Extract play type
            playType=pl.when(pl.col("passResult") == "").then(pl.lit("run")).otherwise(pl.lit("pass")),
            # Get pre snap scores
            preSnapPossessionTeamScore=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapHomeScore")).otherwise(pl.col("preSnapVisitorScore")),
            preSnapDefensiveTeamScore=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapVisitorScore")).otherwise(pl.col("preSnapHomeScore")),
            # Get pre snap win probability
            preSnapPossessionTeamWP=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapHomeTeamWinProbability")).otherwise(pl.col("preSnapVisitorTeamWinProbability")),
            preSnapDefensiveTeamWP=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("preSnapVisitorTeamWinProbability")).otherwise(pl.col("preSnapHomeTeamWinProbability")),
            # Get win probability added
            possessionTeamWPAdded=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("homeTeamWinProbabilityAdded")).otherwise(pl.col("visitorTeamWinProbilityAdded")),
            defensiveTeamWPAdded=pl.when(pl.col("possessionTeam") == pl.col("homeTeamAbbr")).then(pl.col("visitorTeamWinProbilityAdded")).otherwise(pl.col("homeTeamWinProbabilityAdded")),
            # Add defensive team expected points added
            defensiveTeamExpectedPointsAdded=(-1 * pl.col("expectedPointsAdded")),
            # Add score difference
        ).with_columns(
            scoreDifference=(pl.col("preSnapPossessionTeamScore") - pl.col("preSnapDefensiveTeamScore")),
        )

        # Join the aggregated player plays data
        possession_player_plays, defensive_player_plays = self._create_possession_team_player_play_features(aggregated_player_plays)

        output_data = output_data.\
            join(possession_player_plays, on=["gameId", "playId", "possessionTeam"], how="left").\
            join(defensive_player_plays, on=["gameId", "playId", "defensiveTeam"], how="left")

        return output_data

    def process(self):
        pass