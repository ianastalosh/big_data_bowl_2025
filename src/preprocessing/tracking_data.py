# Import and process tracking data
import polars as pl
import logging
from config import Constants


class TrackingDataProcessor:
    def __init__(self, tracking_data_file_paths: list[str]) -> None:
        if not isinstance(tracking_data_file_paths, list):
            raise ValueError("tracking_data_file_paths must be a list of strings.")
        self.tracking_data_file_paths = tracking_data_file_paths
        self.tracking_data = None  # Initialize to None

    def _load_tracking_data(self) -> None:
        tracking_data_list = []
        for file_path in self.tracking_data_file_paths:
            tracking_data_list.append(pl.scan_csv(file_path, ignore_errors=True))
            logging.info(f"Loaded file {file_path}")
        tracking_data = pl.concat(tracking_data_list)
        self.tracking_data = tracking_data
        logging.info(f"Total dataframe has shape: {tracking_data.select(pl.len()).collect().item()}")
    
    def _normalise_features(self) -> pl.DataFrame:
        tracking_data = self.tracking_data

        # Normalise features
        tracking_data = tracking_data.with_columns(
            x=pl.when(pl.col("playDirection") == "left").then(Constants.X_MAX - pl.col("x")).otherwise(pl.col("x")),
            y=pl.when(pl.col("playDirection") == "left").then(Constants.Y_MAX - pl.col("y")).otherwise(pl.col("y")),
            # TODO: Check the logic of the dir normalisation, is different in R code
            dir=pl.when(pl.col("playDirection") == "left").then((pl.col("dir") + 180) % 360).otherwise(pl.col("dir")),
            o=pl.when(pl.col("playDirection") == "left").then((pl.col("o") + 180) % 360).otherwise(pl.col("o")),
            
        )
        self.tracking_data = tracking_data

    def process(self):
        self._load_tracking_data()
        self._normalise_features()
        return self.tracking_data
