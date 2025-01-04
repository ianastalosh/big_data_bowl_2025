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
                playerHadMotionAndCameSet=((pl.col("motionSinceLineset") == "TRUE") | (pl.col("shiftSinceLineset") == "TRUE")),
                playerHadPreSnapMotion=((pl.col("motionSinceLineset") == "TRUE") | (pl.col("shiftSinceLineset") == "TRUE") | (pl.col("inMotionAtBallSnap") == "TRUE"))).\
            group_by(["gameId", "playId", "teamAbbr"]).\
            agg(playNumPlayersInMotionAtSnap=(pl.col("inMotionAtBallSnap") == "TRUE").sum(),
                playNumPlayersShiftSinceLineset=(pl.col("shiftSinceLineset") == "TRUE").sum(),
                playNumPlayersMotionSinceLineset=(pl.col("motionSinceLineset") == "TRUE").sum(),
                playNumPlayersHadMotionAndCameSet=(pl.col("playerHadMotionAndCameSet") == True).sum(),
                playNumPlayersPreSnapMotion=(pl.col("playerHadPreSnapMotion") == True).sum(),
                playerMotionCameSetWasTargetted=((pl.col("playerHadMotionAndCameSet") == True) & (pl.col("wasTargettedReceiver") == 1)).sum(),
                playerInMotionAtSnapWasTargetted=((pl.col("inMotionAtBallSnap") == "TRUE") & (pl.col("wasTargettedReceiver") == 1)).sum(),
                playerInMotionAtSnapRanRoute=((pl.col("inMotionAtBallSnap") == "TRUE") & (pl.col("routeRan") == "TRUE")).sum(),
                playerPreSnapMotionWasTargetted=((pl.col("playerHadPreSnapMotion") == True) & (pl.col("wasTargettedReceiver") == 1)).sum(),
                yardsAfterCatch=pl.sum("yardageGainedAfterTheCatch"),
                playNumTargetedReceivers=pl.sum("wasTargettedReceiver"),
                ).\
            with_columns(
                playHadPlayersInMotionAtSnap=(pl.col("playNumPlayersInMotionAtSnap") > 0),
                playHadPlayersMotionSinceLineset=(pl.col("playNumPlayersMotionSinceLineset") > 0),
                playHadPlayersShiftSinceLineset=(pl.col("playNumPlayersShiftSinceLineset") > 0),
                playHadMotionAndCameSet=(pl.col("playNumPlayersHadMotionAndCameSet") > 0),
                playHadPreSnapMotion=(pl.col("playNumPlayersPreSnapMotion") > 0)        
            ).\
            with_columns(
                playMotionType=pl.when((pl.col("playHadMotionAndCameSet") == True) & (pl.col("playHadPlayersInMotionAtSnap") == True)).then(pl.lit("Pre-snap and at-snap motion")).\
                                    when((pl.col("playHadMotionAndCameSet") == True) & (pl.col("playHadPlayersInMotionAtSnap") == False)).then(pl.lit("Pre-snap only")).\
                                    when((pl.col("playHadMotionAndCameSet") == False) & (pl.col("playHadPlayersInMotionAtSnap") == True)).then(pl.lit("At-snap only")).\
                                    otherwise(pl.lit("No motion"))
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
            playNumTargetedReceivers=pl.col("playNumTargetedReceivers"),
            playHadPlayersInMotionAtSnap=pl.col("playHadPlayersInMotionAtSnap"),
            playHadPlayersMotionSinceLineset=pl.col("playHadPlayersMotionSinceLineset"),
            playHadPlayersShiftSinceLineset=pl.col("playHadPlayersShiftSinceLineset"),
            playHadMotionAndCameSet=pl.col("playHadMotionAndCameSet"),
            playHadPreSnapMotion=pl.col("playHadPreSnapMotion"),
            playerMotionCameSetWasTargetted = pl.col("playerMotionCameSetWasTargetted"),
            playerInMotionAtSnapWasTargetted = pl.col("playerInMotionAtSnapWasTargetted"),
            playerPreSnapMotionWasTargetted = pl.col("playerPreSnapMotionWasTargetted"),
            playMotionType=pl.col("playMotionType"),
            yardsAfterCatch=pl.col("yardsAfterCatch")
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
            # Extract time remaining
            quarterSecondsRemaining=pl.col('gameClock').str.split(':').map_elements(lambda x: int(x[0]) * 60 + int(x[1]), return_dtype=pl.Int32),
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
            halfSecondsRemaining=pl.when(pl.col("quarter").is_in([2,4,5,6])).then(pl.col("quarterSecondsRemaining")).otherwise(900 + pl.col("quarterSecondsRemaining")),
            gameSecondsRemaining=pl.when(pl.col("quarter").is_in([5])).then(pl.col("quarterSecondsRemaining")).otherwise(900 * (4 - pl.col("quarter")) + pl.col("quarterSecondsRemaining")),
        )

        # Join the aggregated player plays data
        possession_player_plays, defensive_player_plays = self._create_possession_team_player_play_features(aggregated_player_plays)

        output_data = output_data.\
            join(possession_player_plays, on=["gameId", "playId", "possessionTeam"], how="left").\
            join(defensive_player_plays, on=["gameId", "playId", "defensiveTeam"], how="left")

        return output_data

    def process(self):
        pass