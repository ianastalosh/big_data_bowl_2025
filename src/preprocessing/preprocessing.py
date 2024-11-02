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
