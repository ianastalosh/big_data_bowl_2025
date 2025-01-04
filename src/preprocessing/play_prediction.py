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

        if len(line_set_tracking) == 0:
            x_los = None
            y_los = None
            x_first_down_marker = None
        else:
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
    
    
    def _cluster_into_3_smallest_to_largest(self, location_array):
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

    def compute_offense_spatial_features(self, play_data, offense_tracking):
        offense_tracking = offense_tracking.\
            with_columns(x_rel_los = pl.col("x") - play_data["x_los"],
                        in_offensive_tackle_box = pl.when((pl.col("x") >= (play_data["x_los"] - 8)) & 
                                                (pl.col("y") <= (play_data["y_los"] + 6)) & 
                                                (pl.col("y") >= (play_data["y_los"] - 6))).then(1).otherwise(0),
                        left_side = pl.when(pl.col("y") <= play_data["y_los"]).then(1).otherwise(0),
                        right_side = pl.when(pl.col("y") >= play_data["y_los"]).then(1).otherwise(0),
                        in_motion = pl.when(pl.col("s") > 0.6).then(1).otherwise(0))

        x_centroid = offense_tracking["x"].mean()
        x_rel_centroid = offense_tracking["x_rel_los"].mean()
        y_centroid = offense_tracking["y"].mean()

        offense_depth = offense_tracking["x"].max() - offense_tracking["x"].min()
        offense_width = offense_tracking["y"].max() - offense_tracking["y"].min()

        left_sided_offense = offense_tracking["left_side"].sum()
        right_sided_offense = offense_tracking["right_side"].sum()
        offense_in_box = offense_tracking["in_offensive_tackle_box"].sum()


        players_in_motion = offense_tracking["in_motion"].sum()
        average_speed = offense_tracking["s"].mean()

        hull = ConvexHull(offense_tracking[["x", "y"]].to_numpy())
        hull_perimeter = hull.area
        hull_volume = hull.volume

        x_clusters = self._cluster_into_3_smallest_to_largest(offense_tracking[["x_rel_los"]].to_numpy())
        y_clusters = self._cluster_into_3_smallest_to_largest(offense_tracking[["y"]].to_numpy())

        return {
            "offense_x_centroid": x_centroid,
            "offense_x_rel_centroid": x_rel_centroid,
            "offense_y_centroid": y_centroid,
            "offense_depth": offense_depth,
            "offense_width": offense_width,
            "offense_in_box": offense_in_box,
            "offense_left_side": left_sided_offense,
            "offense_right_side": right_sided_offense,
            "offense_in_motion": players_in_motion,
            "offense_average_speed": average_speed,
            "offense_hull_perimeter": hull_perimeter,
            "offense_hull_volume": hull_volume,
            "offense_depth_back_cluster_centroid": x_clusters["cluster_0_centroid"],
            "offense_depth_middle_cluster_centroid": x_clusters["cluster_1_centroid"],
            "offense_depth_front_cluster_centroid": x_clusters["cluster_2_centroid"],
            "offense_width_left_cluster_centroid": y_clusters["cluster_0_centroid"],
            "offense_width_middle_cluster_centroid": y_clusters["cluster_1_centroid"],
            "offense_width_right_cluster_centroid": y_clusters["cluster_2_centroid"],
            "offense_depth_back_cluster_count": x_clusters["cluster_0_count"],
            "offense_depth_middle_cluster_count": x_clusters["cluster_1_count"],
            "offense_depth_front_cluster_count": x_clusters["cluster_2_count"],
            "offense_width_left_cluster_count": y_clusters["cluster_0_count"],
            "offense_width_middle_cluster_count": y_clusters["cluster_1_count"],
            "offense_width_right_cluster_count": y_clusters["cluster_2_count"]
        }

    def compute_defense_spatial_features(self, play_data, defense_tracking):

        defense_tracking = defense_tracking.\
            with_columns(x_rel_los = pl.col("x") - play_data["x_los"],
                        in_tackle_box = pl.when((pl.col("x") <= (play_data["x_los"] + 5)) & 
                                                (pl.col("y") <= (play_data["y_los"] + 4)) & 
                                                (pl.col("y") >= (play_data["y_los"] - 4))).then(1).otherwise(0),
                        left_side = pl.when(pl.col("y") <= play_data["y_los"]).then(1).otherwise(0),
                        right_side = pl.when(pl.col("y") >= play_data["y_los"]).then(1).otherwise(0),
                        in_motion = pl.when(pl.col("s") > 0.6).then(1).otherwise(0))

        x_centroid = defense_tracking["x"].mean()
        x_rel_centroid = defense_tracking["x_rel_los"].mean()
        y_centroid = defense_tracking["y"].mean()

        defense_depth = defense_tracking["x"].max() - defense_tracking["x"].min()
        defense_width = defense_tracking["y"].max() - defense_tracking["y"].min()

        left_sided_defenders = defense_tracking["left_side"].sum()
        right_sided_defenders = defense_tracking["right_side"].sum()
        defenders_in_box = defense_tracking["in_tackle_box"].sum()

        players_in_motion = defense_tracking["in_motion"].sum()
        average_speed = defense_tracking["s"].mean()

        hull = ConvexHull(defense_tracking[["x", "y"]].to_numpy())
        hull_perimeter = hull.area
        hull_volume = hull.volume

        x_clusters = self._cluster_into_3_smallest_to_largest(defense_tracking[["x_rel_los"]].to_numpy())
        y_clusters = self._cluster_into_3_smallest_to_largest(defense_tracking[["y"]].to_numpy())

        return {
            "defense_x_centroid": x_centroid,
            "defense_x_rel_centroid": x_rel_centroid,
            "defense_y_centroid": y_centroid,
            "defense_depth": defense_depth,
            "defense_width": defense_width,
            "defenders_in_box": defenders_in_box,
            "defenders_left_side": left_sided_defenders,
            "defenders_right_side": right_sided_defenders,
            "defenders_in_motion": players_in_motion,
            "defense_average_speed": average_speed,
            "defense_hull_perimeter": hull_perimeter,
            "defense_hull_volume": hull_volume,
            "defense_depth_front_cluster_centroid": x_clusters["cluster_0_centroid"],
            "defense_depth_middle_cluster_centroid": x_clusters["cluster_1_centroid"],
            "defense_depth_back_cluster_centroid": x_clusters["cluster_2_centroid"],
            "defense_width_left_cluster_centroid": y_clusters["cluster_0_centroid"],
            "defense_width_middle_cluster_centroid": y_clusters["cluster_1_centroid"],
            "defense_width_right_cluster_centroid": y_clusters["cluster_2_centroid"],
            "defense_depth_back_cluster_count": x_clusters["cluster_0_count"],
            "defense_depth_middle_cluster_count": x_clusters["cluster_1_count"],
            "defense_depth_front_cluster_count": x_clusters["cluster_2_count"],
            "defense_width_left_cluster_count": y_clusters["cluster_0_count"],
            "defense_width_middle_cluster_count": y_clusters["cluster_1_count"],
            "defense_width_right_cluster_count": y_clusters["cluster_2_count"]
        }

    def _calculate_pre_snap_look_changes(self, line_set_tracking, ball_snap_tracking):
        pre_snap_location_change = line_set_tracking.\
            join(ball_snap_tracking.select(pl.col("nflId"), x_snap=pl.col("x"), y_snap=pl.col("y")), 
                on=["nflId"]).\
            with_columns(x_diff=np.abs(pl.col("x") - pl.col("x_snap")), 
                        y_diff=np.abs(pl.col("y") - pl.col("y_snap")),
                        diff=((pl.col("x") - pl.col("x_snap"))**2 + (pl.col("y") - pl.col("y_snap"))**2)**0.5)

        aggregated_change = pre_snap_location_change.\
            group_by(pl.col("team")).\
            agg(total_pairwise_x_change=pl.col("x_diff").sum(), 
                total_pairwise_y_change=pl.col("y_diff").sum(), 
                total_location_change=pl.col("diff").sum()).\
            to_dicts()
        
        output = {
            f"{row['team']}_{col}": row[col]
            for row in aggregated_change
            for col in ['total_pairwise_x_change', 'total_pairwise_y_change', 'total_location_change']
        }

        return output
        


    def get_model_features(self, gameId, playId):

        play_data = self.get_specific_play_data(gameId, playId)
        game_state_features = self.get_game_state_features(play_data["play_info"])

        if len(play_data["line_set_tracking"]) == 0:
            print(f"Play {gameId}-{playId} has no line set tracking data")
            return None
        
        if len(play_data["ball_snap_tracking"]) == 0:
            print(f"Play {gameId}-{playId} has no ball snap tracking data")
            return None

        pre_snap_look_change = self._calculate_pre_snap_look_changes(play_data["line_set_tracking"], play_data["ball_snap_tracking"])

        offense_line_set_spatial_features = self.compute_offense_spatial_features(play_data, play_data["line_set_tracking"].filter(pl.col("team") == "offense"))
        defense_line_set_spatial_features = self.compute_defense_spatial_features(play_data, play_data["line_set_tracking"].filter(pl.col("team") == "defense"))

        offense_ball_snap_spatial_features = self.compute_offense_spatial_features(play_data, play_data["ball_snap_tracking"].filter(pl.col("team") == "offense"))
        defense_ball_snap_spatial_features = self.compute_defense_spatial_features(play_data, play_data["ball_snap_tracking"].filter(pl.col("team") == "defense"))

        line_set_spatial_features = {**offense_line_set_spatial_features, **defense_line_set_spatial_features}
        ball_snap_spatial_features = {**offense_ball_snap_spatial_features, **defense_ball_snap_spatial_features}

        return {"game_state_features": game_state_features,
                "pre_snap_location_change": pre_snap_look_change,
                "line_set_spatial_features": line_set_spatial_features,
                "ball_snap_spatial_features": ball_snap_spatial_features,
                "play_type": play_data["play_info"]["playType"],
                "expected_points_added": play_data["play_info"]["expectedPointsAdded"]}
