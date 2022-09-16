import matplotlib.pyplot as plt
import pandas as pd
import matplotlib
from datetime import datetime

def plot_province(df: pd.DataFrame, col_x: str, col_y: str, province: str, stdev: int=3):
    df_ = df[df["province"] == province]
    print("Shape of dataframe is : ", df_.shape)
    plt.style.use('ggplot')
    plt.figure(figsize=(25,5))
    x = df_[col_x].dt.strftime("%Y-%m-%d")
    y = df_[col_y]
    # Plot x and y
    plt.plot(x,y, "o", color="black", linestyle="--", )
    plt.axhline(y = y.mean(), color = 'gray', linestyle = '--')
    plt.axhline(y = y.mean()+stdev*y.std(), color = 'red', linestyle = '--')
    plt.axhline(y = y.mean()-stdev*y.std(), color = 'red', linestyle = '--')
    # Edit the plot
    plt.tick_params(axis='x',rotation=90)
    plt.legend([col_y,"mean",f"+{stdev} stdev", f"-{stdev} stdev"], loc="best")
    plt.title(province+" "+ df_.date.min().strftime("%Y-%m-%d")+" : "+df_.date.max().strftime("%Y-%m-%d"))
    plt.show()

def plot_metrics(df: pd.DataFrame, col_x: str, col_y1: str, col_y2: str):
    df_ = df.copy()
    print("Shape of dataframe is : ", df_.shape)
    plt.style.use('ggplot')
    plt.figure(figsize=(25,5))
    x = df_[col_x]
    y1 = df_[col_y1]
    y2 = df_[col_y2]
    # Plot x and y
    plt.plot(x,y1, "o", color="black", linestyle="--", )
    plt.plot(x,y2, "o", color="red", linestyle="--", )
    # Edit the plot
    plt.tick_params(axis='x',rotation=90)
    plt.legend([col_y1,col_y2], loc="best")
    plt.show()