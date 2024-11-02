# Plotting functions for tracking data
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.animation as animation
from IPython.display import HTML
from config import Constants

class PlotPlay():
    def __init__(self, 
                 play_df, 
                 min_x=Constants.X_MIN, 
                 min_y=Constants.Y_MIN,
                 max_x=Constants.X_MAX, 
                 max_y=Constants.Y_MAX, 
                 colour1=Constants.TEAM_COLOUR_1,
                 colour2=Constants.TEAM_COLOUR_2, 
                 ball_colour=Constants.BALL_COLOUR):
        self.play_df = play_df
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self.colour1 = colour1
        self.colour2 = colour2
        self.ball_colour = ball_colour
        self.fig, self.ax = plt.subplots()
        self.frame_ids = play_df.select(pl.col("frameId").unique().sort()).to_series().to_list()
        plt.close()

    def _get_play_frame(self, frame_id):
        return self.play_df.filter(pl.col("frameId") == frame_id)
    
    def _create_team_colours_dict(self):
        unique_teams = self.play_df.filter(pl.col("club") != "football").select(pl.col("club").unique()).sort("club")
        team_colours = {
            unique_teams.item(0,0): self.colour1, 
            unique_teams.item(1,0): self.colour2,
            "football": self.ball_colour
        }
        return team_colours
    
    def _plot_field(self):
        pass

    def _plot_players_for_frame(self, frame_id):
        pass

    def plot_frame(self, frame_id):
        pass

    def _init_animation(self):
        self.ax.clear()
        self._plot_field()
        return []
    
    def _animate_frame(self, frame_id):
        self.ax.clear()
        self._plot_field()
        self._plot_players_for_frame(frame_id)
        return []
    
    def animate_play(self, interval=100, save_path=None):
        # Create new figure for animation
        self.fig, self.ax = plt.subplots()
        
        # Create animation
        anim = animation.FuncAnimation(
            self.fig,
            self._animate_frame,
            init_func=self._init_animation,
            frames=self.frame_ids,
            interval=interval,
            blit=True
        )
        
        # Save the animation if save_path is provided
        if save_path:
            if save_path.endswith('.gif'):
                anim.save(save_path, writer='pillow')
            elif save_path.endswith('.mp4'):
                anim.save(save_path, writer='ffmpeg')
            plt.close()

        plt.close()
        return HTML(anim.to_jshtml())



class PlotPlayHorizontal(PlotPlay):
    def __init__(self, play_df):
        super().__init__(play_df)
    
    def _plot_field(self):
        
        # Create limits
        self.ax.set_xlim(self.min_x - 10, self.max_x + 10)
        self.ax.set_ylim(self.min_y - 10, self.max_y + 10) 

        # Colour the endzones
        endzone1 = patches.Rectangle((0,0), 10, 160/3, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')
        endzone2 = patches.Rectangle((110,0), 10, 160/3, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')

        self.ax.add_patch(endzone1)
        self.ax.add_patch(endzone2)

        # Draw field outline
        self.ax.plot([0,120], [0,0], color="black")
        self.ax.plot([0,120], [160/3,160/3], color="black")
        self.ax.plot([0,0], [0,160/3], color="black")
        self.ax.plot([120,120], [0,160/3], color="black")

        # Draw yard lines
        for x in range(10,115,5):
            self.ax.plot([x,x], [0,160/3], color="black")

        # Draw hash marks
        for x in range(10,110,1):
            self.ax.plot([x,x], [160/6 - 3.0833 - 1, 160/6 - 3.0833], color="black", linewidth=0.5)
            self.ax.plot([x,x], [160/6 + 3.0833 + 1, 160/6 + 3.0833], color="black", linewidth=0.5)
            
        # Draw the yard numbers
        for x in range(10,50,10):
            self.ax.text(x+10, 160/6 - 3.0833 - 12, str(x), fontsize=10, color="black", ha='center')
            self.ax.text(x+10, 160/6 + 3.0833 + 12, str(x), fontsize=10, color="black", ha='center')
            self.ax.text(110-x, 160/6 - 3.0833 - 12, str(x), fontsize=10, color="black", ha='center')
            self.ax.text(110-x, 160/6 + 3.0833 + 12, str(x), fontsize=10, color="black", ha='center')
        self.ax.text(60, 160/6 - 3.0833 - 12, "50", fontsize=10, color="black", ha='center')
        self.ax.text(60, 160/6 + 3.0833 + 12, "50", fontsize=10, color="black", ha='center')

        self.ax.axis('off')

    def _plot_players_for_frame(self, frame_id):
        # Get the frame df
        frame_df = self._get_play_frame(frame_id)

        # Add the players
        players = frame_df.filter(pl.col("club") != "football")
        football = frame_df.filter(pl.col("club") == "football")

        team_colours = self._create_team_colours_dict()
        
        players = players.with_columns(
            colours=pl.col("club").replace(team_colours)
        )

        for row in players.iter_rows(named=True):
            self.ax.scatter(row["x"], row["y"], color=row["colours"], s=75, zorder=3)
            self.ax.text(row["x"], row["y"], row["jerseyNumber"], fontsize=8, ha='center', va='center', zorder=4)

        # Plot the ball
        self.ax.scatter(football["x"][0], football["y"][0], color="brown", s=50, zorder=3)

    def plot_frame(self, frame_id):
        # Reset the figure
        self.fig, self.ax = plt.subplots()
        plt.close()

        self._plot_field()
        self._plot_players_for_frame(frame_id)
        self.fig.show()
        return self.fig         

class PlotPlayVertical(PlotPlay):
    def __init__(self, play_df):
        super().__init__(play_df)
    
    def _plot_field(self):
        # Create limits
        self.ax.set_xlim(self.min_y - 10, self.max_y + 10)
        self.ax.set_ylim(self.min_x - 10, self.max_x + 10) 

        # Colour the endzones
        endzone1 = patches.Rectangle((0,0), 160/3, 10, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')
        endzone2 = patches.Rectangle((0,110), 160/3, 10, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')

        self.ax.add_patch(endzone1)
        self.ax.add_patch(endzone2)

        # Draw field outline
        self.ax.plot([0,0], [0,120], color="black")
        self.ax.plot([0,160/3], [0, 0], color="black")
        self.ax.plot([160/3,0], [120, 120], color="black")
        self.ax.plot([160/3,160/3], [120,0], color="black")

        # Draw yard lines
        for y in range(10,115,5):
            self.ax.plot([0,160/3], [y,y], color="black")

        # Draw hash marks
        for y in range(10,110,1):
            self.ax.plot([160/6 - 3.0833 - 1, 160/6 - 3.0833], [y,y], color="black", linewidth=0.5)
            self.ax.plot([160/6 + 3.0833 + 1, 160/6 + 3.0833], [y,y], color="black", linewidth=0.5)
            
        # Draw the yard numbers
        for y in range(10,50,10):
            self.ax.text(160/6 - 3.0833 - 12, y+10, str(y), fontsize=10, color="black", ha='right', va="center", rotation=-90)
            self.ax.text(160/6 + 3.0833 + 12, y+10, str(y), fontsize=10, color="black", ha='left', va="center", rotation=90)
            self.ax.text(160/6 - 3.0833 - 12, 110-y, str(y), fontsize=10, color="black", ha='right', va="center", rotation=-90)
            self.ax.text(160/6 + 3.0833 + 12, 110-y, str(y), fontsize=10, color="black", ha='left',va="center", rotation=90)
        self.ax.text(160/6 - 3.0833 - 12, 60, "50", fontsize=10, color="black", ha='right', va="center", rotation=-90)
        self.ax.text(160/6 + 3.0833 + 12, 60, "50", fontsize=10, color="black", ha='left', va="center", rotation=90)

        self.ax.axis('off')

    def _plot_players_for_frame(self, frame_id):
        # Get the frame df
        frame_df = self._get_play_frame(frame_id)

        # Add the players
        players = frame_df.filter(pl.col("club") != "football")
        football = frame_df.filter(pl.col("club") == "football")

        team_colours = self._create_team_colours_dict()
        
        players = players.with_columns(
            colours=pl.col("club").replace(team_colours)
        )

        for row in players.iter_rows(named=True):
            self.ax.scatter(row["y"], row["x"], color=row["colours"], s=75, zorder=3)
            self.ax.text(row["y"], row["x"], row["jerseyNumber"], fontsize=8, ha='center', va='center', zorder=4, clip_on=True)

        # Plot the football
        self.ax.scatter(football["y"][0], football["x"][0], color="brown", s=50, zorder=3)


    def plot_frame(self, frame_id):
        # Reset the figure
        self.fig, self.ax = plt.subplots()
        plt.close()

        self._plot_field()
        self._plot_players_for_frame(frame_id)
        self.fig.show()
        return self.fig    
    