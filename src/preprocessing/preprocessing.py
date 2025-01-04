import polars as pl
from .tracking_data import TrackingDataProcessor
from .non_tracking_data import NonTrackingDataProcessor

class BigDataBowlData:
    def __init__(self, 
                 games_file_path: str,
                 plays_file_path: str,
                 player_file_path: str,
                 player_plays_file_path: str,
                 tracking_data_file_paths: list[str]) -> None:
        
        self.tracking_data_processor = TrackingDataProcessor(tracking_data_file_paths)
        self.non_tracking_data_processor = NonTrackingDataProcessor(games_file_path,
                                                                    plays_file_path,
                                                                    player_file_path,
                                                                    player_plays_file_path)
        
        self.raw_games = self.non_tracking_data_processor.games
        self.raw_plays = self.non_tracking_data_processor.plays
        self.raw_players = self.non_tracking_data_processor.players
        self.raw_player_plays = self.non_tracking_data_processor.player_plays
        
        self.plays_df = self.non_tracking_data_processor.feature_engineer_play_data()

        tracking_data = self.tracking_data_processor.process()
        self.tracking_data = self._add_offense_indicator_to_tracking_data(tracking_data)
        self.line_set_tracking = self.tracking_data.filter(pl.col("event") == "line_set").collect()
        self.ball_snap_tracking = self.tracking_data.filter(pl.col("event") == "ball_snap").collect()

    def _filter_play_id(self, lazy_frame: pl.LazyFrame, game_id: int, play_id: int) -> pl.LazyFrame:
        return lazy_frame.filter(pl.col("gameId") == game_id).filter(pl.col("playId") == play_id)
    
    def get_play_data(self, game_id: int, play_id: int) -> dict:
        play_tracking_data = self._filter_play_id(self.tracking_data, game_id, play_id)
        play_plays_df = self._filter_play_id(self.plays_df, game_id, play_id).collect()
        play_player_plays = self._filter_play_id(self.raw_player_plays, game_id, play_id).collect()
        
        line_set_frame = self.line_set_tracking.filter(pl.col("gameId") == game_id).filter(pl.col("playId") == play_id)
        ball_snap_frame = self.ball_snap_tracking.filter(pl.col("gameId") == game_id).filter(pl.col("playId") == play_id)

        return {
            "tracking_df": play_tracking_data,
            "line_set_tracking": line_set_frame,
            "ball_snap_tracking": ball_snap_frame,
            "play_df": play_plays_df,
            "player_play_df": play_player_plays
        }
    
    def _add_offense_indicator_to_tracking_data(self, tracking_df):
        tracking_df_with_offense_indicator = tracking_df.join(self.plays_df.select(["gameId", "playId", "possessionTeam"]), on=["gameId", "playId"], how="left").\
            with_columns(
                team=pl.when(pl.col("club") == pl.col("possessionTeam")).then(pl.lit("offense"))
                .when(pl.col("club") != "football").then(pl.lit("defense"))
                .otherwise(pl.lit("football"))).\
        drop(["possessionTeam"])
        return tracking_df_with_offense_indicator
