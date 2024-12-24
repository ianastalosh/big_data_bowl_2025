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
        self.tracking_data = self.tracking_data_processor.process()
        self.plays_df = self.non_tracking_data_processor.feature_engineer_play_data()

    def _filter_play_id(self, lazy_frame: pl.LazyFrame, game_id: int, play_id: int) -> pl.LazyFrame:
        return lazy_frame.filter(pl.col("gameId") == game_id).filter(pl.col("playId") == play_id)
    
    def get_tracking_data_for_play(self, game_id: int, play_id: int) -> dict:
        play_tracking_data = self._filter_play_id(self.tracking_data, game_id, play_id).collect()
        play_plays_df = self._filter_play_id(self.plays_df, game_id, play_id).collect()
        play_player_plays = self._filter_play_id(self.raw_player_plays, game_id, play_id).collect()
        
        return {
            "tracking_data": play_tracking_data,
            "plays_df": play_plays_df,
            "player_plays": play_player_plays
        }
