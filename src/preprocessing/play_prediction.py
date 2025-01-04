import polars as pl
from .tracking_data import TrackingDataProcessor
from .non_tracking_data import NonTrackingDataProcessor
from .preprocessing import BigDataBowlData
import numpy as np
from scipy.spatial import ConvexHull
from sklearn.cluster import KMeans


class PlayPredictionModel:
    def __init__(self, 
                 data: BigDataBowlData) -> None:
        
        self.data = data

    def _create_play_info_dict(self, play_df):
        assert len(play_df) == 1, "DataFrame must have exactly one row"
        return play_df.to_dicts()[0]
    
    def _get_original_play_direction(self):
        return self.tracking_df["playDirection"].item(0)

    def _get_first_down_line(self): 
        return self.x_los + self.play_info["yardsToGo"]
    
    def get_specific_play_data(self, gameId, playId):
        play_data = self.data.get_play_data(gameId, playId)

        play_info = self._create_play_info_dict(play_data["play_df"])
        line_set_tracking = play_data["line_set_tracking"]
        ball_snap_tracking = play_data["ball_snap_tracking"]

        x_los = line_set_tracking.filter(pl.col("club") == "football").select("x").item()
        y_los = line_set_tracking.filter(pl.col("club") == "football").select("y").item()

        x_first_down_marker = x_los + play_info["yardsToGo"]

        return {
            "play_info": play_info,
            "line_set_tracking": line_set_tracking,
            "ball_snap_tracking": ball_snap_tracking,
            "x_los": x_los,
            "y_los": y_los,
            "x_first_down_marker": x_first_down_marker
        }
    
    def get_game_state_features(self, play_info):

        features = {
            "gameId": play_info["gameId"],
            "playId": play_info["playId"],
            "quarter": play_info["quarter"],
            "down": play_info["down"],
            "logYardsToGo": np.log(play_info["yardsToGo"]),
            "distanceToEndzone": play_info["distanceToEndzone"],
            "scoreDifference": play_info["scoreDifference"],
            "gameSecondsRemaining": play_info["gameSecondsRemaining"],
        }
        return features
    
    def compute_offense_spatial_features(self, play_data, offense_tracking):
        pass
    
    
    def _cluster_into_3_smallest_to_largest(location_array):
        k_means = KMeans(n_clusters=3, random_state=441).fit(location_array)

        cluster_centroids = k_means.cluster_centers_
        cluster_labels = k_means.labels_

        # Re-order so that the clusters are ordered from smallest to largest
        centroid_sizes = np.mean(cluster_centroids, axis=1)
        # Get the ordering that would sort centroids from smallest to largest
        centroid_order = np.argsort(centroid_sizes)

        # Create a mapping from old to new labels
        label_map = {old_label: new_label for new_label, old_label in enumerate(centroid_order)}

        # Apply the mapping to get new labels
        new_labels = np.array([label_map[label] for label in cluster_labels])
        label_counts = np.bincount(new_labels)

        ordered_centroids = [centroid[0] for centroid in cluster_centroids[centroid_order]]

        return {
            "cluster_centroids": ordered_centroids,
            "cluster_labels": new_labels,
            "cluster_0_centroid": ordered_centroids[0],
            "cluster_1_centroid": ordered_centroids[1],
            "cluster_2_centroid": ordered_centroids[2],
            "cluster_0_count": label_counts[0],
            "cluster_1_count": label_counts[1],
            "cluster_2_count": label_counts[2]
        }

    
    def compute_defense_spatial_features(self, play_data, defense_tracking):

        defense_tracking = defense_tracking.\
            with_columns(x_rel_los = pl.col("x") - play_data["x_los"],
                        in_tackle_box = pl.when((pl.col("x") <= (play_data["x_los"] + 5)) & 
                                                (pl.col("y") <= (play_data["y_los"] + 4)) & 
                                                (pl.col("y") >= (play_data["y_los"] - 4))).then(1).otherwise(0),
                        left_side = pl.when(pl.col("y") <= 160/6).then(1).otherwise(0),
                        right_side = pl.when(pl.col("y") >= 160/6).then(1).otherwise(0))

        x_centroid = defense_tracking["x"].mean()
        x_rel_centroid = defense_tracking["x_rel_los"].mean()
        y_centroid = defense_tracking["y"].mean()

        defense_depth = defense_tracking["x"].max() - defense_tracking["x"].min()
        defense_width = defense_tracking["y"].max() - defense_tracking["y"].min()

        left_sided_defenders = defense_tracking["left_side"].sum()
        right_sided_defenders = defense_tracking["right_side"].sum()
        defenders_in_box = defense_tracking["in_tackle_box"].sum()

        hull = ConvexHull(defense_tracking[["x", "y"]].to_numpy())
        hull_perimeter = hull.area
        hull_volume = hull.volume

        x_clusters = self._cluster_into_3_smallest_to_largest(defense_tracking[["x"]].to_numpy())
        y_clusters = self._cluster_into_3_smallest_to_largest(defense_tracking[["y"]].to_numpy())

        # Something about the speed of the defenders

        return {
            "defense_x_centroid": x_centroid,
            "defense_x_rel_centroid": x_rel_centroid,
            "defense_y_centroid": y_centroid,
            "defense_depth": defense_depth,
            "defense_width": defense_width,
            "defenders_in_box": defenders_in_box,
            "defenders_left_side": left_sided_defenders,
            "defenders_right_side": right_sided_defenders,
            "defense_hull_perimeter": hull_perimeter,
            "defense_hull_volume": hull_volume,
            "depth_left_cluster_centroid": x_clusters["cluster_0_centroid"],
            "depth_middle_cluster_centroid": x_clusters["cluster_1_centroid"],
            "depth_right_cluster_centroid": x_clusters["cluster_2_centroid"],
            "width_front_cluster_centroid": y_clusters["cluster_0_centroid"],
            "width_middle_cluster_centroid": y_clusters["cluster_1_centroid"],
            "width_back_cluster_centroid": y_clusters["cluster_2_centroid"]
        }

        # Count defenders in box
    def get_model_features(self, gameId, playId):

        play_data = self.get_specific_play_data(gameId, playId)

        game_state_features = self.get_game_state_features(play_data["play_info"])

        defense_ball_snap_spatial_features = self.compute_defense_spatial_features(play_data, play_data["ball_snap_tracking"].filter(pl.col("team") == "defense"))
        # spatial_features = self.get_spatial_features(gameId, playId)

        return {"game_state_features": game_state_features,
                "defense_spatial_features": defense_ball_snap_spatial_features}
