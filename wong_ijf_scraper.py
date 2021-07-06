#!/usr/bin/env python
# coding: utf-8

# # IJF Web Scraper - Technical Test - Nova Scotia

# Michael Wing-Cheung Wong

# # Module Calls

# In[4]:


############### PYTHON 3



#Module Imports
import time
import os
import platform
import urllib
import re
import json
import datetime
import warnings
import glob

from pathlib import Path


import matplotlib
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
# import selenium
import bs4

# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup

get_ipython().run_line_magic('matplotlib', 'inline')


print("Python ver. "+platform.python_version())
print("numpy ver. "+np.__version__)
print("pandas ver. "+pd.__version__)
print("seaborn ver. "+sns.__version__)
print("matplotlib ver. "+matplotlib.__version__)
# print("selenium ver. "+selenium.__version__)
print("bs4 ver. "+bs4.__version__)

pd.set_option('display.max_colwidth', None)


# # Scraper and Data Read-in

# In[41]:


def get_pages(filename, link, output_dir=None, subpages=None, sleep_time=5):
    """
    cURL function to download a page and/or subpages in batches of pages, converting them to dictionaries and saving them
    as JSON files per batch

    Parameters
    ----------
    filename : str
        Filename string of the xml file to be created
    link : str
        Link of the page to be downloaded, optionally contains the subpages to be downloaded using format() method
    output_dir : str, optional
        Directory of exported files, default = None/current directory
    subpages : list, optional
        List of subpages to be downloaded, default = None
    sleep_time : int, optional
        Number of seconds for cURL to wait until another query is made, by default 5 seconds

    Returns
    -------
    df : dataframe object
        The final dataframe object for the page
    """
    
#     if os.path.exists(filename+".csv"):
#         # Check if a csv file with that filename already exists, ask user for confirmation of overwrite
#         while True:
#             skip = input('''A data CSV file already exists with that name, 
#                                 do you wish to download all the data again, select no to load the existing file? (Y/N)''')
#             if skip.lower().strip() == "n":
#                 return pd.read_csv(filename+".csv")
#             elif skip.lower().strip() == "y":
#                 break
#             else:
#                 print("That's not a valid response. Please try again.")
    
    batch_dict = {}

    # Loop through queries until no pages remain
    if subpages is not None:
        for number, name in enumerate(subpages):
            
            if "/" in name:
                output_path = filename+"_"+str(number)+".xml"
            else:            
                output_path = filename+"_"+str(name)+".xml"

            if output_dir is not None:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                path = os.path.join(output_dir, output_path)
            else:
                path = output_path

#             curl_query = '''
#             curl -d "{} -o "{}"
#             '''.format(link.format(name),output_path)
#             os.system(curl_query)


            query = "{}".format(link.format(name))
            print("\nCurrently querying:",query)

            response = urllib.request.urlopen(query)
            html = response.read()
#             print(html)

            with open(path, 'wb') as f:
                f.write(html)
            print("Exported to",path)

#             filein = open(output_path,encoding="utf-8")
#             print("\nBeginning soup on file",output_path)
#             soup = bs4.BeautifulSoup(filein.read())#, "lxml")
#             print("Done soup")

            # Sleep to allow the page to get some well-deserved rest
            time.sleep(sleep_time)

            batch_dict[name] = html
            
    else:
        output_path = filename+".xml"
        
        if output_dir is not None:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            path = os.path.join(output_dir, output_path)
        else:
            path = output_path

#         curl_query = '''
#         curl -d "{} -o "{}"
#         '''.format(link,output_path)

        query = "{}".format(link)
        print("\nCurrently querying:",query)

        response = urllib.request.urlopen(query)
        html = response.read()

        with open(path, 'wb') as f:
            f.write(html)
        print("Exported to",path)

#         filein = open(output_path,encoding="utf-8")
#         print("\nBeginning soup on file",output_path)
#         soup = bs4.BeautifulSoup(filein.read())#, "lxml")
#         print("Done soup")

        # Sleep to allow the page to get some well-deserved rest
        time.sleep(sleep_time)

        batch_dict["page"] = html

    return batch_dict


# In[6]:


# For acquiring main page data
lobbyist_list_batch_dict = get_pages(
                            filename="novascotia_lobbyists",
                              link="https://novascotia.ca/sns/lobbyist/search.asp?page={}&slt_searchType=&txt_search=&RdStatus=&RdType=&dept_agency=",
                              output_dir="data/novascotia_lobbyist_lists",
                              subpages=list(range(0,1351,25)),
                              sleep_time=5
)

# display(lobbyist_list_batch_dict)


# # Nova Scotia Lobbying Data Cleaning and Analysis

# In[37]:


def lobbyist_xml_to_df(input_dir, filename, batch_dict=None, lobbyist_df=None):
    
    """
    Class to clean XML files using Beautifulsoup to extract relevant information. Compiles all XML files from directory to single DataFrame.
    Produces a dataframe of lobbyist names and respective lobby session links if it does not already exists.
    Otherwise produces a dataframe of all relevant lobbyist information.
    Exports dataframes as csv files.

    Parameters
    ----------
    input_path : str
        Path of source XML files
        
    filename : str
        Name of relevant XML files, same as that specified previously by scraper
    
    batch_dict : dict, optional
        Dictionary of xml files (name:xml), default = None
        
    lobbyist_df : dataframe object, optional
        Dataframe object consisting of all lobbyists, used to append more information default = None

    Returns
    -------
    lobbyist_df : dataframe object
        Dataframe object consisting of all lobbyists from XML files

    """    
    
    def lobbyist_df_generator(input_dir, filename):
        for name in glob.glob(os.path.join(input_dir,filename+"*.xml")):

            with open(name, "r", encoding="utf-8") as filein:
                print("\nBeginning soup on file ",name)
                try:
                    soup = bs4.BeautifulSoup(filein.read())#, "lxml")
                except UnicodeDecodeError as e:
                    print(e)
                    print("File "+name+" was unable to be parsed. Continuing ...")
                    continue
                print("\nCleaning data ...")

                # From inspection, find all elements that have lobbyist links
                lobbyists = [lobbyist.get_text() for lobbyist in soup.find_all("a", href=re.compile("/sns/lobbyist/search.asp\?regid=*"))]
                lobbyist_names = lobbyists[::3]
                lobbyist_employers = lobbyists[1::3]
                lobbyist_clients = lobbyists[2::3]
                lobbyist_links = [lobbyist["href"] for lobbyist in soup.find_all("a", href=re.compile("/sns/lobbyist/search.asp\?regid=*"))][::3]


                # Elements are ordered by name, company, name, company, and so on. So convert to dataframe.
                try:
                    lobbyist_df = lobbyist_df.append(pd.DataFrame({
                                                    "Lobbyist Name":lobbyist_names,
                                                    "Lobbyist Employer":lobbyist_employers,
                                                    "Lobbyist Client":lobbyist_clients,
                                                    "Lobbyist Link":lobbyist_links
                                                }))

                except UnboundLocalError:
                    lobbyist_df = pd.DataFrame({
                                                    "Lobbyist Name":lobbyist_names,
                                                    "Lobbyist Employer":lobbyist_employers,
                                                    "Lobbyist Client":lobbyist_clients,
                                                    "Lobbyist Link":lobbyist_links
                                                })

                print("Data cleaned") 

        lobbyist_df = lobbyist_df.reset_index()
        lobbyist_df = lobbyist_df.drop(columns=["index"])
        lobbyist_df.to_csv(filename+".csv")

        return lobbyist_df

        
    def lobbyist_df_appender(input_dir, filename, lobbyist_df):
        pass
    
    
    
    
    
    if lobbyist_df is None:
        warnings.warn("No existing lobbyist dataframe is provided. Creating lobbyist-link dataframe.")
        return lobbyist_df_generator(input_dir, filename)
    else:
        return lobbyist_df_appender(input_dir, filename, lobbyist_df)


# In[38]:


novascotia_lobbyist_df = lobbyist_xml_to_df(input_dir="data/novascotia_lobbyist_lists",filename="novascotia_lobbyists")#,lobbyist_df=novascotia_lobbyist_df)
display(novascotia_lobbyist_df)


# In[43]:


# Use retrieved links to update lobbyist profiles
subpages = list(novascotia_lobbyist_df["Lobbyist Link"])[:21]
print(list(novascotia_lobbyist_df["Lobbyist Link"])[:21])


lobbyist_info_batch_dict = get_pages(
                            filename="novascotia_lobbyists_individual",
                              link="https://novascotia.ca{}",
                              output_dir="data/novascotia_lobbyist_infos",
                              subpages=subpages,
                              sleep_time=5
)


# In[ ]:


# novascotia_lobbyist_df = pd.read_csv("data/novascotia_lobbyist_lists/novascotia_lobbyists.csv")

# novascotia_lobbyist_df = lobbyist_xml_to_df(input_dir="data/novascotia_lobbyist_lists",filename="novascotia_lobbyists",lobbyist_df=novascotia_lobbyist_df)
# display(novascotia_lobbyist_df)

