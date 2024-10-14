# Import and process tracking data
import polars as pl
import logging



class TrackingDataProcessor:
    def __init__(self, tracking_data_file_paths: list[str]) -> None:
        if not isinstance(tracking_data_file_paths, list):
            raise ValueError("tracking_data_file_paths must be a list of strings.")
        self.tracking_data_file_paths = tracking_data_file_paths
        self.tracking_data = None  # Initialize to None

    def load_tracking_data(self) -> None:
        tracking_data = pl.DataFrame()
        for file_path in self.tracking_data_file_paths:
            tracking_data = tracking_data.vstack(pl.scan_csv(file_path, ignore_errors=True))
            logging.info(f"Loaded file {file_path}")
        self.tracking_data = tracking_data
        logging.info(f"Total dataframe has shape: {tracking_data.shape}")
    
    def process_tracking_data(self) -> pl.DataFrame:
        pass

    def process(self):
        pass
