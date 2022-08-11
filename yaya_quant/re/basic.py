import numpy as np
import pandas as pd
import datetime
    

#basic  need trade.data in df
def is_trade_day(data, date):
    return date in data.df.index




