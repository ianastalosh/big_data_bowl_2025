class ParsedPlay:
    def __init__(self, gameId, playId):

        total_data = data.get_play_data(gameId, playId)

        self.play_info = self._create_play_info_dict(total_data["play_df"])
        self.tracking_df = total_data["tracking_df"]
        self.player_play_df = total_data["player_play_df"]

        self.ball_tracking_df = self.get_team_tracking_data("football")

        self.x_los, self.y_los = self._get_los_ball_placement()

        self.key_frames = self.get_key_event_frames()
        self.line_set_frame = self.key_frames["line_set"]
        self.ball_snap_frame = self.key_frames["ball_snap"]

    def _create_play_info_dict(self, play_df):
        assert len(play_df) == 1, "DataFrame must have exactly one row"
        return play_df.to_dicts()[0]
    
    def get_team_tracking_data(self, team):
        return self.tracking_df.filter(pl.col("club") == team)
    
    def _get_los_ball_placement(self):
        tracking_ball_lineset = self.tracking_df.filter((pl.col("club") == "football") & (pl.col("event") == "line_set"))
        lineset_x = tracking_ball_lineset.select("x").item()
        lineset_y = tracking_ball_lineset.select("y").item()
        return lineset_x, lineset_y
    
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
    
    def map_offense_and_defense_teams(self):
        possession_team = self.play_info["possessionTeam"]
        defensive_team = self.play_info["defensiveTeam"]
        
        return self.tracking_df.with_column(
            team = pl.when(pl.col("club") == possession_team).then("offense").\
                      when(pl.col("club") == defensive_team).then("defense").\
                      otherwise("football")
        )
        
    def assign_location_zones(self):
        
        Y_DIVIDER_1 = 12
        Y_DIVIDER_2 = 160/6
        Y_DIVIDER_3 = 160/3 - 12

        X_DIVIDER = self.x_los - 3

        return self.tracking_df.with_columns(
            y_zone = pl.when(pl.col("y") <= Y_DIVIDER_1).then(pl.lit("LO")).\
                        when(pl.col("y") <= Y_DIVIDER_2).then(pl.lit("LI")).\
                        when(pl.col("y") <= Y_DIVIDER_3).then(pl.lit("RI")).\
                        otherwise(pl.lit("RO")),
            x_zone = pl.when(pl.col("x") <= X_DIVIDER).then(pl.lit("DEEP")).otherwise(pl.lit("LINE"))).\
            with_columns(
                zone_location = pl.concat_str(["x_zone", "y_zone"], separator=" - ")
            )

    # def process_tracking_data(self):
    #     ball_x, ball_y = self.get_ball_placement()
    #     self.map_offense_and_defense_teams(ball_x)
        
    #     # TODO Filter out events before line_set
    #     # TODO Identify x, y centroids for offense, defense at line_set, ball_snap, etc.
    #     # TODO Identify most common patterns of movement for players in each team. Identify if there is correlation between type of motion

    # def analyse_play_motion(self):
    #     # TODO Reutnr 
    #     pass