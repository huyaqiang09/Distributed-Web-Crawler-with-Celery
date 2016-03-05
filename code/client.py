"""
@author: jguo
"""
from __future__ import absolute_import

from celery import Celery

import os
import time
import datetime
import multiprocessing

# for save webpage as local html file
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# for get page and user list
import re
import requests
from bs4 import BeautifulSoup
from lxml import etree
TOTAL_USERS = 0

app = Celery('proj',
             broker='amqp://guest:guest@localhost:5672//',
             #backend='amqp', #Celery bug fixed here
             )

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_RESULT_BACKEND='amqp',
    CELERY_TASK_RESULT_EXPIRES=3600,	
)

# For each user found in this page, call save2html() 
def scrapeSinglePage(pageIndex, url = "https://www.kaggle.com/users?page="):
    cnt = 0
    r = requests.get(url + str(pageIndex))
    soup = BeautifulSoup(r.content,'lxml')
    # retrieve user link in one page
    for ul in soup.find_all("a", class_='profilelink'):
        userName = ul['href'][1:]
        #save2html(userName)
        cnt = cnt +1
        app.send_task('proj.task.save2html', args=(userName,))        
    if pageIndex % 100 ==0:
        print("-- processing page: " + str(pageIndex))
    if cnt != 100 :
        print("-- non-100-users page found: "+str(pageIndex))
    return cnt

# Web scaper main function
# find all pages, then call scrapeSinglePage() for each page
def main_scrapeAllPages(url, n):
    global TOTAL_USERS
    print("Start time: " + str(datetime.datetime.now()))  
    # retrieve how many pages (every pages has at most 100 users)
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'lxml')

    if int(n) == 0:
        maxPage = 1
        for x in soup.find(id='user-pages').find_all(href=re.compile("page=")):
            if x.get_text().isdigit():             
                page = int(x.get_text())
                if  page > maxPage:
                    maxPage = page
    else:
        maxPage = int(n)
    
    print("Total Pages found: ["+ str(maxPage) +"]")
    # process all pages 
    #for pageIndex in range(1, maxPage+1):
    #    scrapeSinglePage(pageIndex)        
    pool = multiprocessing.Pool() # use all available cores
    TOTAL_USERS = pool.map(scrapeSinglePage, range(1, maxPage+1))    
    pool.close()
    pool.join()   
    
    sum = 0
    for x in TOTAL_USERS:
        sum = sum + x
        
    print("End time: " + str(datetime.datetime.now()))
    print("Total processed user number: [" + str(sum) +"]")
    print("Total html files to be processed: " + str(sum*2))
    return    


# main function
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='set max page manually.')
    parser.add_argument('n', type=int, nargs='*',
                   default='0')
    args = parser.parse_args()    
    main_scrapeAllPages("https://www.kaggle.com/users", args.n[-1])


