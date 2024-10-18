# Plotting functions for tracking data
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.patches as patches

MIN_X = 0
MIN_Y = 0
MAX_X = 120
MAX_Y = 160/3

class PlotPlayFrameVertical():
    def __init__(self, df):
        self.df = df
        self.min_x = MIN_X
        self.min_y = MIN_Y
        self.max_x = MAX_X
        self.max_y = MAX_Y
        self.color1 = "red",
        self.color2 = "green"
    
    def plot(self):
        # Create a figure

        fig = plt.figure()

        # Create limits
        plt.xlim(self.min_x - 10, self.max_x + 10)
        plt.ylim(self.min_y - 10, self.max_y + 10) 

        # Colour the endzones
        endzone1 = patches.Rectangle((0,0), 10, 160/3, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')
        endzone2 = patches.Rectangle((110,0), 10, 160/3, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')

        plt.gca().add_patch(endzone1)
        plt.gca().add_patch(endzone2)

        # Draw field outline
        plt.plot([0,120], [0,0], color="black")
        plt.plot([0,120], [160/3,160/3], color="black")
        plt.plot([0,0], [0,160/3], color="black")
        plt.plot([120,120], [0,160/3], color="black")

        # Draw yard lines
        for x in range(10,115,5):
            plt.plot([x,x], [0,160/3], color="black")

        # Draw hash marks
        for x in range(10,110,1):
            plt.plot([x,x], [160/6 - 3.0833 - 1, 160/6 - 3.0833], color="black", linewidth=0.5)
            plt.plot([x,x], [160/6 + 3.0833 + 1, 160/6 + 3.0833], color="black", linewidth=0.5)
            
        # Draw the yard numbers
        for x in range(10,50,10):
            plt.text(x+10, 160/6 - 3.0833 - 12, str(x), fontsize=10, color="black", ha='center')
            plt.text(x+10, 160/6 + 3.0833 + 12, str(x), fontsize=10, color="black", ha='center')
            plt.text(110-x, 160/6 - 3.0833 - 12, str(x), fontsize=10, color="black", ha='center')
            plt.text(110-x, 160/6 + 3.0833 + 12, str(x), fontsize=10, color="black", ha='center')
        plt.text(60, 160/6 - 3.0833 - 12, "50", fontsize=10, color="black", ha='center')
        plt.text(60, 160/6 + 3.0833 + 12, "50", fontsize=10, color="black", ha='center')

        # Add the players

        players = self.df.filter(pl.col("club") != "football")
        unique_teams = players.select(pl.col("club").unique())

        football = self.df.filter(pl.col("club") == "football")

        team_colors = {
            unique_teams.item(0,0): "red", 
            unique_teams.item(1,0): "green",
            "football": "brown"
        }

        players = players.with_columns(
            colors=pl.col("club").replace(team_colors)
        )

        for row in players.iter_rows(named=True):
            plt.scatter(row["x"], row["y"], color=row["colors"], s=75, zorder=3)
            plt.text(row["x"], row["y"], row["jerseyNumber"], fontsize=8, ha='center', va='center', zorder=4)

        # Plot the football
        plt.scatter(football["x"][0], football["y"][0], color="brown", s=50, zorder=3)

        # Hide pltes
        plt.axis('off')

        fig.show()


class PlotPlayFrameHorizontal():
    def __init__(self, df):
        self.df = df
        self.min_x = MIN_X
        self.min_y = MIN_Y
        self.max_x = MAX_X
        self.max_y = MAX_Y
        self.color1 = "red",
        self.color2 = "green"
    
    def plot(self):
        # Create a figure
        fig = plt.figure()

        # Create limits
        plt.xlim(MIN_Y - 10, MAX_Y + 10)
        plt.ylim(MIN_X - 10, MAX_X + 10) 

        # Colour the endzones
        endzone1 = patches.Rectangle((0,0), 160/3, 10, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')
        endzone2 = patches.Rectangle((0,110), 160/3, 10, linewidth=1, edgecolor='darkviolet', facecolor='darkviolet')

        plt.gca().add_patch(endzone1)
        plt.gca().add_patch(endzone2)

        # Draw field outline
        plt.plot([0,0], [0,120], color="black")
        plt.plot([0,160/3], [0, 0], color="black")
        plt.plot([160/3,0], [120, 120], color="black")
        plt.plot([160/3,160/3], [120,0], color="black")

        # Draw yard lines
        for y in range(10,115,5):
            plt.plot([0,160/3], [y,y], color="black")

        # Draw hash marks
        for y in range(10,110,1):
            plt.plot([160/6 - 3.0833 - 1, 160/6 - 3.0833], [y,y], color="black", linewidth=0.5)
            plt.plot([160/6 + 3.0833 + 1, 160/6 + 3.0833], [y,y], color="black", linewidth=0.5)
            
        # Draw the yard numbers
        for y in range(10,50,10):
            plt.text(160/6 - 3.0833 - 12, y+10, str(y), fontsize=10, color="black", ha='right', va="center", rotation=-90)
            plt.text(160/6 + 3.0833 + 12, y+10, str(y), fontsize=10, color="black", ha='left', va="center", rotation=90)
            plt.text(160/6 - 3.0833 - 12, 110-y, str(y), fontsize=10, color="black", ha='right', va="center", rotation=-90)
            plt.text(160/6 + 3.0833 + 12, 110-y, str(y), fontsize=10, color="black", ha='left',va="center", rotation=90)
        plt.text(160/6 - 3.0833 - 12, 60, "50", fontsize=10, color="black", ha='right', va="center", rotation=-90)
        plt.text(160/6 + 3.0833 + 12, 60, "50", fontsize=10, color="black", ha='left', va="center", rotation=90)

        # Add the players

        players = self.df.filter(pl.col("club") != "football")
        unique_teams = players.select(pl.col("club").unique())

        football = self.df.filter(pl.col("club") == "football")

        team_colors = {
            unique_teams.item(0,0): "red", 
            unique_teams.item(1,0): "green",
            "football": "brown"
        }

        players = players.with_columns(
            colors=pl.col("club").replace(team_colors)
        )

        for row in players.iter_rows(named=True):
            plt.scatter(row["y"], row["x"], color=row["colors"], s=75, zorder=3)
            plt.text(row["y"], row["x"], row["jerseyNumber"], fontsize=8, ha='center', va='center', zorder=4, clip_on=True)

        # Plot the football
        plt.scatter(football["y"][0], football["x"][0], color="brown", s=50, zorder=3)

        # Hide axis
        plt.axis('off')

        fig.show()
