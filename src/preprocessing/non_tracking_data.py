import polars as pl

class NonTrackingDataProcessor:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def load_data(self) -> pl.DataFrame:
        data = pl.read_csv(self.file_path)
        self.data = data
        return data
    

class GamesProcessor(NonTrackingDataProcessor):
    def preprocess(self) -> pl.DataFrame:
        pass

class PlayerPlayProcessor(NonTrackingDataProcessor):
    def preprocess(self) -> pl.DataFrame:
        pass

class PlayersProcessor(NonTrackingDataProcessor):
    def preprocess(self) -> pl.DataFrame:
        pass

class PlaysProcessor(NonTrackingDataProcessor):
    def preprocess(self) -> pl.DataFrame:
        pass