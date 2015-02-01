# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 21:03:45 2015

@author: grobi

receives a folder (BASEDIR) and runs the pimp.py on every folder in the BASEDIR
"""
<<<<<<< HEAD

# import modules and static variables
import os
import pandas as pd
=======
# import modules and static variables
import os
from pandas import DataFrame, Series
import pandas as pd
# import numpy as np
>>>>>>> 01b534962b75a94fe33aff808c784761373058f6

# Switch off the HTML repesentation of DataFrames in iPython
pd.set_option('display.notebook_repr_html', False)

<<<<<<< HEAD
# Folder containing sub-folders with PP data
BASEDIR = '/home/grobi/Dropbox/Data'
DATAFOLDER = 'tno2'
DATADIR = os.path.join(BASEDIR, DATAFOLDER, 'eout')
# Folder containing information read in from text files
READ_IN_FILES = '/home/grobi/Dropbox/documents/MATLAB/TNO2/helpfiles'
RANDI = pd.read_csv(os.path.join(READ_IN_FILES, 'randi.csv'), sep=';', header=None)
# Where the data is stored after it is read
HDFSTOR = '/home/grobi/Dropbox/Data/tno2/dstor.h5'

# PIMP
def eimp(ddir = DATADIR, pp = []):

    # report the directory where data is imported from
    print 'Importing data from', ddir

    store = pd.HDFStore(HDFSTOR)

=======
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
    
>>>>>>> 01b534962b75a94fe33aff808c784761373058f6
    # takes PP from 'plst' otherwise imports every PP in BASEDIR
    if pp:
        plst = [pp] if isinstance(pp, int) else pp
        print '%d folder(s) will be imported' % (len(plst))
    else:
<<<<<<< HEAD
        plst = [int(p) for p in os.listdir(DATADIR)]
        print 'All %d folders will be imported' % (len(plst))

    for p in sorted(plst):
        print 'Parsing data of participant folder %d' % (p)

        store = dimp(store, ddir, p)
        # break # preamture break
    # store.close()

    return store

def dimp(store, ddir, p, filename = ''):
=======
        plst = [int(p) for p in os.listdir(ddir)]
        print 'All %d folders will be imported' % (len(plst))
    
    for p in sorted(plst):
        print 'Parsing data of participant folder %d' % (p)
        
        pdir = ddir + '/' + str(p) + '/'
        edat[str(p)] = dimp(edat, pdir)
        # break # premature break
        
    return edat

def dimp(edat, pdir, filename = ''):
>>>>>>> 01b534962b75a94fe33aff808c784761373058f6
    """
    Expects a folder and imports specified files from that folder
    returns a dataframe or a series of dataframe objects with the delected data from the PP in that folder
    """
    
<<<<<<< HEAD
    pdir = os.path.join(ddir, str(p))  
    
    if str(p) in store:
        store.remove(str(p))

    # Take all the files in the folder (be able to select/skip specific files.)
    if not filename:
        for fname in os.listdir(pdir):
            if ('_data' in fname) or ('_env' in fname) or ('r00' in fname):
                continue
            else:
                print 'Reading file:', fname
                pnr, rnr = fname_read(fname)
                tnr = run2trial(pnr,rnr) #find trial number for run number
                store[os.path.join(str(pnr),str(tnr))] = pd.DataFrame.from_csv(os.path.join(pdir, fname), header = 0, sep = ';').reset_index()
                # break # premature break

    return store
=======
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
>>>>>>> 01b534962b75a94fe33aff808c784761373058f6

    # read in / parse file name
    # read in first line information if available
    # read data
    # do data transformations that have to be done once
    # store data in format that can be read later, that can be expanded with data that is read in later

<<<<<<< HEAD
def run2trial(pnr, rnr, randi = RANDI):
    return randi.ix[pnr,rnr]

def fname_read(fname):
    pnr = int(os.path.splitext(fname)[0][2:4]) # pnr (same as p)
    rnr = int(os.path.splitext(fname)[0][-2:]) # rnr = run number
    
    pnr -= 1 #adjust for python indexing
    rnr -= 1
    
    return (pnr, rnr)

store = eimp()
=======
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
>>>>>>> 01b534962b75a94fe33aff808c784761373058f6
