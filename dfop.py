# -*- coding: utf-8 -*-
"""
Created on Tue Feb  3 01:45:46 2015

@author: grobi

DataFrame Operations
"""

import pandas as pd

def markers(df):
    marker_context_list = pd.DataFrame()
    for line in df.values:
        if line[3] != -9999:
            marker_context_list[str(line[4])] = line
            marker_context_list.index = COLHEADS.index
    return marker_context_list