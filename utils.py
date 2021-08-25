from time import time
import pandas as pd
import numpy as np


def get_time_in_seconds(duration, default="7d"):
    """
    :param duration: str
    'all' :overall time
     s: seconds
     m: minutes
     h: hours
     d: days
    :return: time in seconds
    """
    if duration == "all":
        return time()
    duration_number = duration[:-1]
    time_ = 0
    if not duration_number.isnumeric():
        duration = default

    if duration.endswith("s"):
        time_ = int(duration_number)
    elif duration.endswith("m"):
        time_ = 60 * int(duration_number)
    elif duration.endswith("h"):
        time_ = 60 * 60 * int(duration_number)
    elif duration.endswith("d"):
        time_ = 24 * 60 * 60 * int(duration_number)

    return time_


def get_time_bins(df, period):

    if period[-1] not in ["h", "d", "m", "y", "l"]:
        raise ValueError(
            "period must be in hour(h) , days(d), months(m), year(y) or 'all"
        )
    if not period[:-1].isnumeric():
        df["bins"] = "all"
        return df

    _period = int(period[:-1])
    time_period = get_time_in_seconds(period)
    t0 = df["ts"].iloc[0]  # making t0t0 be inclusive of bins
    latest_time = time()
    bins = list(np.arange(t0 - 2, latest_time, time_period))
    labels = [str(i * _period) + period[-1] for i in range(len(bins) - 1)]
    df["time_bins"] = pd.cut(x=df["ts"], bins=bins, labels=labels)
    return df, labels
