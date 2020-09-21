#!/usr/bin/env python
# coding: utf-8

get_ipython().run_line_magic('load_ext', 'nb_black')


from pathlib import Path

import pandas as pd
import plotly.express as px

import plotly.io as pio

pio.renderers.default = "png"


all_incidents = pd.read_feather(
    Path("../data/processed/all_incidents.feather")
).sort_values("current_as_of")
all_incidents.head(5)


# # Basic summary statistics
# 
#  - Describe each of the columns

all_incidents.describe(
    include="all",
    datetime_is_numeric=True,
)


px.histogram(
    data_frame=all_incidents.groupby("incident_name").size_acres.max(),
    x="size_acres",
    title="Largest Extent (in acres) of Reported Wildfires",
)


start_date_counts = (
    all_incidents.groupby("incident_name")
    .date_of_origin.last()
    .dropna()
    .dt.date.value_counts()
)

px.bar(start_date_counts, title="Number of Incidents by Start Date")


# # Investigate causes of fires
# 
#  - What are all of the causes?
#  - What are the common groupings of causes?
#  - How prevalent has each been this season?
# 

sorted(all_incidents.cause.astype("str").unique().tolist())


# fall through ordering
clean_causes = {
    "lightning": "lightning",
    "powerline": "powerline",
    "shooting": "guns",
    "arson": "arson",
    "human": "human",
    "under invest": "unknown",
    "unknown": "unknown",
    "unkown": "unknown",
    "undetermined": "unknown",
    "nan": "unknown",
}


def _clean_causes(s):
    for old, new in clean_causes.items():
        if old in s.lower():
            return new

    return s


cleaned_causes = all_incidents.cause.astype("str").apply(_clean_causes)


px.bar(data_frame=cleaned_causes.value_counts(), title="Count of Causes of Fires")


# # Investigate fuels of fires
# 
#  - What are all of the fuels?
#  - What are the common groupings of causes?
#  - How do the fuels relate to growth rates?
# 

fuels = [
    "chaparral",
    "tall grass",
    "short grass",
    "grass",
    "timber",
    "brush",
    "rugged terrain",
    "sage",
    "understory",
    "litter",
    "fir",
    "pine",
]


def standardize_fuels(s):
    return pd.Series({f"fuel_{f}": f in s.lower() for f in fuels})


fuels = all_incidents.fuels_involved.astype("str").apply(standardize_fuels)

all_incidents = all_incidents.join(fuels)


px.bar(data_frame=all_incidents.filter(like="fuel_").sum().sort_values())


# # Plot fire progression over time as available
# 
#  - What do the different "burn rates" look like for these fires?
#  

most_recent_report = all_incidents.groupby("incident_name").last()
most_recent_report.head()


most_recent_report["burn_rate"].dropna().sort_values().tail(25)


most_recent_report["burn_rate"] = (
    most_recent_report.size_acres
    / (
        most_recent_report.current_as_of.dt.tz_convert(None)
        - most_recent_report.date_of_origin
    ).dt.days
)

# get the 25 fastest burning to plot
to_plot = most_recent_report["burn_rate"].dropna().sort_values().tail(25)

to_plot


f = px.bar(
    data_frame=most_recent_report["burn_rate"].dropna().sort_values().tail(25),
    orientation="h",
    title="25 Fastest burning fires",
)

# set the width and height for this plot
png_renderer = pio.renderers["png"]
png_renderer.width = 500
png_renderer.height = 800

f.show(renderer="png")

