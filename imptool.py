# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 21:03:45 2015

@author: grobi

receives a folder (BASEDIR) and runs the pimp.py on every folder in the BASEDIR
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
COLHEADS = pd.DataFrame.from_csv(os.path.join(READ_IN_FILES, 'newheads.csv'), header=None)
GT = pd.DataFrame.from_csv(os.path.join(READ_IN_FILES, 'grantable.csv'))
# Where the data is stored after it is read
HDFSTOR = '/home/grobi/Dropbox/data/tno2/dstor.h5'

#%%
### GTAB CLASS ###

class Gtab:
    """ 
    Grantable is where all the output from the analysis scripts is gathered for statistical post proc analysis
    The gran table as a dubble index with ppnumber and trialnumber (ppnumber * trialnumber = number of rows)
    
    The Gtab object is given to functions that extract data from the dataset
    these functions add new colums to the Gtab
    
    Attributes:
        table: the actual pandas DataFrame
        pp: number of participants in the table
        tnr: number of trials per participant
    """
    def __init__(self, pnr, tnr):
        self.pp = pnr
        self.trials = tnr        
        
        p1 = [[i] * self.trials for i in range(1,self.pp+1)]
        p2 = [inner for outer in p1 for inner in outer] # flatten list of lists
        t1 = range(1,self.trials+1) * self.pp
        index = pd.MultiIndex.from_arrays([p2, t1], names=['pp', 'trial'])
        self.table = pd.DataFrame(index=index)
    
    def get_table(self):
        return self.table
    
#    def add_col(self, name, data):
#        self.table[str(name)] = data
        
#%%
### PREPROCESSING ###

def get_colheads(df, d = READ_IN_FILES):    
    """
    get the column heads to rewrite them and load the changed csv in later in the process
    """
    
    pd.Series(df.columns.values).to_csv(os.path.join(d, 'oldheads.csv'))
    print 'Column heads written to', os.path.join(d, 'oldheads.csv')

#%%
### IMPORT DATA ###

def eimp(ddir = DATADIR, pp = []):
    """
    TEXT
    """

    # report the directory where data is imported from
    print 'Importing data from', ddir

    store = pd.HDFStore(HDFSTOR)

    # takes PP from 'plst' otherwise imports every PP in BASEDIR
    if pp:
        plst = [pp] if isinstance(pp, int) else pp
        print '%d folder(s) will be imported' % (len(plst))
    else:
        plst = [int(p) for p in os.listdir(DATADIR)]
        print 'All %d folders will be imported' % (len(plst))

    for p in sorted(plst):
        print 'Parsing data of participant folder %d' % (p)

        store = dimp(store, ddir, p)
        # break # preamture break
    store.close()

    return store

def dimp(store, ddir, p, filename = ''):
    """
    Expects a folder and imports specified files from that folder
    returns a dataframe or a series of dataframe objects with the delected data from the PP in that folder
    """  
    
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
                df = pd.DataFrame.from_csv(os.path.join(pdir, fname), header = 0, sep = ';').reset_index()
                df.columns = COLHEADS.index
                store[os.path.join('p' + str(pnr), 't' + str(tnr))] = df
                # break # premature break

    return store

def run2trial(pnr, rnr, randi = RANDI):
    return randi.ix[pnr,rnr]

def fname_read(fname):
    pnr = int(os.path.splitext(fname)[0][2:4]) # pnr (same as p)
    rnr = int(os.path.splitext(fname)[0][-2:]) # rnr = run number
    pnr -= 1 #adjust for python indexing
    rnr -= 1
    return (pnr, rnr)

store = eimp()

#%%
### DATAFRAME OPERATIONS ###


for key in store.keys():
    print store[key]

def markers(df):
    """
    higher order function that applies different function to selected dataframes
    chooses drivers and trials (between or within participants) depending on condition to be analized
    """
    
    marker_context_list = pd.DataFrame()
    for line in df.values:
        if line[3] != -9999:
            marker_context_list[str(line[4])] = line
            marker_context_list.index = COLHEADS.index
    return marker_context_list