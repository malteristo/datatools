# -*- coding: utf-8 -*-

#%% SETUP
import os
import pandas as pd

# Switch off the HTML repesentation of DataFrames in iPython
pd.set_option('display.notebook_repr_html', False)

# Folder containing sub-folders with PP data
BASEDIR = '/home/grobi/Dropbox/data'
DATAFOLDER = 'tno2'
DATADIR = os.path.join(BASEDIR, DATAFOLDER, 'eout')
# Where the data is stored after it is read in
HDFSTOR = os.path.join(BASEDIR, DATAFOLDER, 'dstor.h5')
# Folder containing information read in from text files
READ_IN_FILES = os.path.join(BASEDIR, DATAFOLDER, 'helpfiles')
# Load global lookup tables
RANDI = pd.read_csv(os.path.join(READ_IN_FILES, 'randi.csv'),
                    sep=';',
                    header=None)
COLHEADS = pd.DataFrame.from_csv(os.path.join(READ_IN_FILES, 'newheads.csv'),
                                 header=None)
SKIP_LIST = ['_data', '_env', 'r00']
#%% GTAB CLASS

class Gtab:
    """ 
    Grantable is where all the output from the analysis scripts is gathered
    for statistical post proc analysis.
    The gran table as a dubble index with ppnumber and trialnumber
    (ppnumber * trialnumber = number of rows)
    
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
        
        # create the index for the table
        # for each pp and trail combination
        
        # put each pp "trial" times in the list 
        p1 = [[i] * self.trials for i in range(1,self.pp+1)]
        # flatten list of lists p1        
        p2 = [inner for outer in p1 for inner in outer]
        # repeat the trial list pp times       
        t1 = range(1,self.trials+1) * self.pp
        # put it all together into an index
        index = pd.MultiIndex.from_arrays([p2, t1], names=['pp', 'trial'])
        # add the index to the dataframe        
        self.table = pd.DataFrame(index=index)
    
    def get_table(self):
        return self.table
    
    def add_col(self, name, data):
        self.table[str(name)] = data

#%% STORE CLASS

class Store:
    """
    Uses the HFDStore to store imported data frames.
    Takes care of operations with opening/closing the store.
    Allows query for dataframes.
    """
    
    def __init__(self, path_to_file):
        
        self.ptf = path_to_file
        
        # load the existing HDFStore file
        if os.path.isfile(self.ptf):
            self.store = pd.HDFStore(self.ptf)
            print "Loading data from disk"
        # or create one and run the import script (eimp)
        else:
            self.store = pd.HDFStore(self.ptf)
            print "Creating new HDFStore file in", self.ptf
            self.eimp()
    
    def eimp(self, pp = []):
        """
        Receives a particular list of folder names (pp)
        or generates list of all folder names in DATADIR
        runs folder import script (dimp) each folder in the list (plist)
        """
        if not self.store.is_open:
            self.store.open()
        
        # report the directory where data is imported from
        print 'Importing data from', DATADIR
    
        # takes PP from 'folderlist' otherwise imports every PP in BASEDIR
        if pp:
            folderlist = [pp] if isinstance(pp, int) else pp
            print '%d folder(s) will be imported' % (len(folderlist))
        else:
            folderlist = [int(foldername) for foldername in os.listdir(DATADIR)]
            print 'All %d folders will be imported' % (len(folderlist))
    
        for foldername in sorted(folderlist):
            print 'Parsing data of participant folder %d' % (foldername)
    
            self.dimp(foldername)
            # break # preamture break

    def dimp(self, foldername, filename = ''):
        """
        imports data files from data folder (foldername)
        puts a dataframe for each data file in the data folder into the store
        """  
        
        pdir = os.path.join(DATADIR, str(foldername))
        
        # Take all the files in the folder
        if not filename:
            for fname in os.listdir(pdir):
                # be able to skip files that contain substring from SKIP_LIST
                if any(substring in fname for substring in SKIP_LIST):
                    continue
                else:
                    # generate dataframe from file
                    print 'Reading file:', fname
                    df = pd.DataFrame.from_csv(os.path.join(pdir, fname),
                                               header = 0,
                                               sep = ';').reset_index()
                    df.columns = COLHEADS.index

                    # compute additional columns if necessary
                    # add colums that are computed from other data in df

                    # tgfrontleft
                    ncol_tgap(df, 'lfdtrav', 'dtrav', 'spd', 'tgfrontleft')
                    # tgfrontright
                    ncol_tgap(df, 'rfdtrav', 'dtrav', 'spd', 'tgfrontright')
                    #tgfront
                    ncol_tgap(df, 'fdtrav', 'dtrav', 'spd', 'tgfront')
                    #tgrear, neg
                    ncol_tgap(df, 'dtrav', 'rdtrav', 'rspd', 'tgrear', neg=True)
                    #tgrearleft, neg
                    ncol_tgap(df, 'dtrav', 'lrdtrav', 'lrspd', 'tgrearleft', neg=True)
                    #tgrearright, neg
                    ncol_tgap(df, 'dtrav', 'rrdtrav', 'rrspd', 'tgrearright', neg=True)
                    
                    # generate key from fname and store df under that key
                    self.store[make_key(fname)] = df
                    # break # premature break
    
    def gdf(self, pnr, tnr):
        """
        get_data_frame: returns the df under the given pnr and tnr
        """
        key = os.path.join('p%02d' % (pnr), 't%02d' % (tnr))
        return self.store[key]
    
    def gfn(self, pnr, tnr):
        """
        get_file_name: returns the file name that belings to the df
        """
        print fname_write(pnr,trial2run(pnr,tnr))
    
    def ist(self):
        """
        iterate store: returns an iterator over all the keys in self.store
        """
        for key in self.store.keys():
            return self.store[key]
            break
        
    def gimme(self):
        """
        Provides direct access to the HDFStore object
        """
        return self.store

#%% HELPER FUCTIONS


def get_colheads(df):    
    """
    Get the column heads in a csv in order to rewrite them
    and load the modified csv in, later in the process
    """
    pd.Series(df.columns.values).to_csv(os.path.join(READ_IN_FILES, 'oldheads.csv'))
    print 'Column heads written to', os.path.join(READ_IN_FILES, 'oldheads.csv')

def run2trial(pnr, rnr):
    return RANDI.ix[pnr-1,rnr-1]
    
def trial2run(pnr, tnr):
    return list(RANDI[RANDI == tnr].stack().index)[pnr-1][1]+1
    # adjusted for python indexing (pnr - 1, rnr -1)

def fname_read(fname):
    pnr = int(os.path.splitext(fname)[0][2:4]) # pnr (same as p)
    rnr = int(os.path.splitext(fname)[0][-2:]) # rnr = run number
    return pnr, rnr

def make_key(fname):
    """
    generates a key value from the filename for the HDFStore
    """
    pnr, rnr = fname_read(fname)
    tnr = run2trial(pnr,rnr) #find trial number for run number
    return os.path.join('p%02d' % (pnr), 't%02d' % (tnr))
    
def fname_write(pnr, tnr):
    rnr = trial2run(pnr, tnr)
    return 'pp%02dr%02d.csv' % (pnr, rnr)
    
def ms2kmh(ms):
    return ms * 3.6

def kmh2ms(kmh):
    return kmh / 3.6

def markers(df):
    """
    higher order function that applies a function to a selected dataframe
    chooses drivers and trials (between or within participants)
    depending on condition to be analized
    """
    marker_context_list = pd.DataFrame()
    for line in df.values:
        if line[3] != -9999:
            marker_context_list[str(line[4])] = line
            marker_context_list.index = COLHEADS.index
    return marker_context_list

def ncol_tgap(df, dtravlead, dtravfol, spdfol, new_col_name, neg=False):
    """
    Expects a df with columns for the:
    - distance traveled of the leading vehicle (m)
    - distance traveled of the following vehicle (m)
    - speed of the following vehicle in (m/s)
    Computes the time gap from these values
    and saves it in a new column in the df
    neg=True to make column values negative
    todo: account for vehicle sizes?
    """
    new_col = (df[dtravlead] - df[dtravfol]) / df[spdfol]
    if neg:
        new_col = -new_col
    df[new_col_name] = new_col
    return df