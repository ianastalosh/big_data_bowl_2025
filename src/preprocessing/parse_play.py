class ParsedPlay:
    def __init__(self, gameId, playId):

        total_data = data.get_play_data(gameId, playId)

        self.play_info = self._create_play_info_dict(total_data["play_df"])
        self.tracking_df = total_data["tracking_df"]
        self.player_play_df = total_data["player_play_df"]

        self.x_los, self.y_los = self._get_los_ball_placement()
        self.x_first_down_marker = self._get_first_down_line()

        self.key_frames = self.get_key_event_frames()
        self.line_set_frame = self.key_frames["line_set"]
        self.ball_snap_frame = self.key_frames["ball_snap"]
        self.play_direction = self._get_original_play_direction()

        self.processed_tracking_df = self.process_tracking_data(self.tracking_df)

    def _create_play_info_dict(self, play_df):
        assert len(play_df) == 1, "DataFrame must have exactly one row"
        return play_df.to_dicts()[0]
    
    def _get_team_tracking_data(self, team):
        return self.tracking_df.filter(pl.col("club") == team)

    def _get_los_ball_placement(self):
        tracking_ball_lineset = self.tracking_df.filter((pl.col("club") == "football") & (pl.col("event") == "line_set"))
        lineset_x = tracking_ball_lineset.select("x").item()
        lineset_y = tracking_ball_lineset.select("y").item()
        return lineset_x, lineset_y
    
    def _get_original_play_direction(self):
        return self.tracking_df["playDirection"].item(0)

    def _get_first_down_line(self): 
        return self.x_los + self.play_info["yardsToGo"]
    
    def _get_event_frame(self, event):
        event_frame = self.tracking_df.filter(pl.col("event") == event).select("frameId").head(1)
        if event_frame.is_empty():
            raise ValueError(f"No {event} event found in tracking data")
        return event_frame.item()

    def get_key_event_frames(self):
        
        self.line_set_frame = self._get_event_frame("line_set")
        self.ball_snap_frame = self._get_event_frame("ball_snap")
        
        return {
            "line_set": self._get_event_frame("line_set"),
            "ball_snap": self._get_event_frame("ball_snap"),
            } 
    
    def _filter_events_before_line_set(self, tracking_df):
        filtered_df = tracking_df.filter(pl.col("frameId") >= self.line_set_frame).\
            with_columns(adjusted_frame_id = pl.col("frameId") - self.line_set_frame + 1)
        return filtered_df
    
    def _map_offense_and_defense_teams(self, tracking_df):
        possession_team = self.play_info["possessionTeam"]
        defensive_team = self.play_info["defensiveTeam"]
        
        return tracking_df.with_columns(
            team = pl.when(pl.col("club") == possession_team).then(pl.lit("offense")).\
                      when(pl.col("club") == defensive_team).then(pl.lit("defense")).\
                      otherwise(pl.lit("football"))
        )
        
    def _assign_location_zones(self, tracking_df):
        
        Y_DIVIDER_1 = 12
        Y_DIVIDER_2 = 160/6
        Y_DIVIDER_3 = 160/3 - 12

        X_DIVIDER_1 = self.x_los - 3
        X_DIVIDER_2 = self.x_los
        X_DIVIDER_3 = self.x_first_down_marker

        return tracking_df.with_columns(
            y_zone = pl.when(pl.col("y") <= Y_DIVIDER_1).then(pl.lit("A")).\
                        when(pl.col("y") <= Y_DIVIDER_2).then(pl.lit("B")).\
                        when(pl.col("y") <= Y_DIVIDER_3).then(pl.lit("C")).\
                        otherwise(pl.lit("D")),
            x_zone = pl.when(pl.col("x") <= X_DIVIDER_1).then(pl.lit("1")).\
                        when(pl.col("x") <= X_DIVIDER_2).then(pl.lit("2")).\
                        when(pl.col("x") <= X_DIVIDER_3).then(pl.lit("3")).\
                        otherwise(pl.lit("4"))).\
            with_columns(
                zone_location = pl.concat_str(["y_zone", "x_zone"], separator="")
            )

    def process_tracking_data(self, tracking_df):
        processed_tracking = self._map_offense_and_defense_teams(tracking_df)
        processed_tracking = self._assign_location_zones(processed_tracking)
        processed_tracking = self._filter_events_before_line_set(processed_tracking)
        return processed_tracking
    
    def compute_team_centroids(self, tracking_frame):

    
    def analyse_play_motion(self, tracking_df):
        lineset_tracking = tracking_df.filter(pl.col("frameId") == self.line_set_frame)
        ball_snap_frame = tracking_df.filter(pl.col("frameId") == self.ball_snap_frame)

    #     # TODO Filter out events before line_set 
    #     # TODO Map offense and defense 
    #     # # TODO create zones
    #     # TODO Filter out events before line_set
    #     # TODO Identify x, y centroids for offense, defense at line_set, ball_snap, etc.
    #     # TODO Identify most common patterns of movement for players in each team. Identify if there is correlation between type of motion

    # def analyse_play_motion(self):
    #     # TODO Reutnr 
        pass