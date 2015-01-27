# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 21:03:45 2015

@author: grobi

receives a folder (BASEDIR) and runs the pimp.py on every folder in the BASEDIR
"""
# import modules and static variables
import os
from pandas import DataFrame, Series
import pandas as pd
# import numpy as np

# Switch off the HTML repesentation of DataFrames in iPython
pd.set_option('display.notebook_repr_html', False)

## Ask for folder and saves input to 'dirname'
#import tkinter, tkinter.filedialog
#root = tkinter.Tk()
#root.withdraw()
#dirname = tkinter.filedialog.askdirectory(parent=root,initialdir="/",title='Please select a directory')

# Folder containing sub-folders with PP data
BASEDIR = '/home/grobi/Data/'
# Folder containing information read in from text files
READ_IN_FILES = '/home/grobi/Dropbox/documents/MATLAB/TNO2/helpfiles/'
# Where the data is stored after it is read
DSTOR = '/home/grobi/Data/tno2/dstor.pkl'

# PIMP
def eimp(directory = 'tno2', pp = []):
    
    if not 'edat' in vars():    
        edat = edat_load()
        
    # report the directory where data is imported from   
    ddir = BASEDIR + directory + '/eout' 
    print 'Importing data from', str(ddir)
    
    # takes PP from 'plst' otherwise imports every PP in BASEDIR
    if pp:
        plst = [pp] if isinstance(pp, int) else pp
        print '%d folder(s) will be imported' % (len(plst))
    else:
        plst = [int(p) for p in os.listdir(ddir)]
        print 'All %d folders will be imported' % (len(plst))
    
    for p in sorted(plst):
        print 'Parsing data of participant folder %d' % (p)
        
        pdir = ddir + '/' + str(p) + '/'
        edat[str(p)] = dimp(edat, pdir)
        # break # premature break
        
    return edat

def dimp(edat, pdir, filename = ''):
    """
    Expects a folder and imports specified files from that folder
    returns a dataframe or a series of dataframe objects with the delected data from the PP in that folder
    """
    
    ### do something with edat, break it open to work in pdat ###    
    
    pdat = Series()    
    
    # Take all the files in the folder (be able to select/skip specific files.)
    if not filename:
        for fname in os.listdir(pdir):
            if ('_data' in fname) or ('_env' in fname):
                continue
            else:
                print 'Reading file:', fname
                pdat[str(fname)] = DataFrame.from_csv(pdir + fname, header = 0, sep = ';').reset_index()
                # break # premature break
    
    return pdat

    # read in / parse file name
    # read in first line information if available
    # read data
    # do data transformations that have to be done once
    # store data in format that can be read later, that can be expanded with data that is read in later

def edat_load(dstor = DSTOR):
    
    edat = pd.Series()
        
    if os.path.isfile(dstor):
        edat = pd.read_pickle(dstor)
    else:
        edat.to_pickle(dstor)
        
    return edat

def edat_save(edat, dstor = DSTOR):
    
    edat.to_pickle(dstor)

#edat = eimp()