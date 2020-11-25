## Python Script to pull turnstile data from http://web.mta.info/developers/turnstile.html
## The purpose of the this script extract the data off the web and combine in to one file
## This complete file can be cleaned and used for data exploration and forcasting

## Template for extracting data taken from: 
## https://towardsdatascience.com/mta-turstile-data-my-first-taste-of-a-data-science-project-493b03f1708a
## changes made: Date pulled from start date to current date rather than for X number of weeks 

#import packages
import pandas as pd

#initialise the date for the first week of the dataset (week ending on start_date)
start_date = filedate = pd.datetime(2020,4,7)

#initialise the regex for the MTA turnstile url
filename_regex = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_{}.txt"

filelist = []

while filedate < pd.datetime.now():

    # create the appropriate filename for the week
    filedate_str = str(filedate.year)[2:4] + str(filedate.month).zfill(2) + str(filedate.day).zfill(2)
    filename = filename_regex.format(filedate_str)
    # read the file and append it to the list of files to be concacated
    df = pd.read_csv(filename, parse_dates=['DATE'], keep_date_col=True)
    filelist.append(df)

    # advance to the next week
    filedate += pd.Timedelta(days=7)

mta_test = pd.concat(filelist, axis=0, ignore_index=True)
mta_test.rename(columns={'EXITS                                                               ':'EXITS'}, inplace=True)

# aggrigating the data to the day. 

## gets the number of entries and exits
mta_entries = mta_test.groupby(['STATION','C/A','UNIT','SCP','DATE']).ENTRIES.max() - mta_test.groupby(['STATION','C/A','UNIT','SCP','DATE']).ENTRIES.min()
mta_exits = mta_test.groupby(['STATION','C/A','UNIT','SCP','DATE']).EXITS.max() - mta_test.groupby(['STATION','C/A','UNIT','SCP','DATE']).EXITS.min()

## flattens the data
mta_entries_flat = mta_entries.reset_index()
mta_exits_flat = mta_exits.reset_index()

mta_entries_exits = pd.merge(mta_entries_flat, mta_exits_flat, how='outer')

## append weekday and traffic column to dataset
mta_entries_exits['WEEKDAY'] = mta_entries_exits['DATE'].dt.day_name()
mta_entries_exits['TRAFFIC'] = mta_entries_exits['ENTRIES'] + mta_entries_exits['EXITS']
mta_entries_exits['WEEKDAY_INDEX'] = mta_entries_exits['DATE'].dt.weekday

## aggrigate by station rather than by individual turnstile
mta_bystation = mta_entries_exits.groupby(['STATION','DATE','WEEKDAY', 'WEEKDAY_INDEX']).sum().reset_index()

PennTS = mta_bystation.loc[mta_bystation.STATION == '34 ST-PENN STA']
PennTS.to_csv('PennTS.csv')

