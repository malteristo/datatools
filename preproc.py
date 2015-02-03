# -*- coding: utf-8 -*-
"""
Created on Sun Feb  1 15:57:21 2015

@author: grobi
"""

# import modules and static variables
import os
import pandas as pd

# Switch off the HTML repesentation of DataFrames in iPython
pd.set_option('display.notebook_repr_html', False)

# Folder containing sub-folders with PP data
BASEDIR = '/home/grobi/Dropbox/data'
DATAFOLDER = 'tno2'
DATADIR = os.path.join(BASEDIR, DATAFOLDER, 'eout')
# Folder containing information read in from text files
READ_IN_FILES = os.path.join(BASEDIR, DATAFOLDER, 'helpfiles')
RANDI = pd.read_csv(os.path.join(READ_IN_FILES, 'randi.csv'), sep=';', header=None)
COLHEADS = pd.Series.from_csv(os.path.join(READ_IN_FILES, 'newheads.csv')).index
# Where the data is stored after it is read
HDFSTOR = '/home/grobi/Dropbox/data/tno2/dstor.h5'

# get the column heads to rewrite them and load the changed csv in later in the process
def get_colheads(df, d = READ_IN_FILES):    
    pd.Series(df.columns.values).to_csv(os.path.join(d, 'oldheads.csv'))
    print 'Column heads written to', os.path.join(d, 'oldheads.csv')