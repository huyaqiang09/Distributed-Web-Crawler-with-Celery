import boto
import boto.s3

import os.path
import sys
import traceback

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

START_TIME = datetime.datetime.now()
TOTAL_CNT = 0

def check_exists(b, remote_filename):    
    k = boto.s3.key.Key(b, remote_filename)
    if k.exists():
        print(str(k))
    else:
        print("-------------------------TEMINATED-------------------------")
        print("Start time:" + str(START_TIME))
        print("File not found: " + remote_filename)
        print("Checked users: " +str(TOTAL_CNT))    
        print("End time:" + str(datetime.datetime.now()))
        sys.exit(1)    
        
# For each user found in this page, call save2html() 
def checkSinglePage(pageUrl, bucket):
    global TOTAL_CNT
    #global totalUsers
    #global root
    r = requests.get("https://www.kaggle.com/users?page=" + str(pageUrl))
    soup = BeautifulSoup(r.content,'lxml')
    # retrieve user link in one page
    for ul in soup.find_all("a", class_='profilelink'):
        userName = ul['href'][1:]        
        userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
        check_exists(bucket, userName+"/"+userName+".html")
        check_exists(bucket, userName+"/"+userName+"_results.html")
        TOTAL_CNT = TOTAL_CNT +1

        
# Web scaper main function
# find all pages, then call scrapeSinglePage() for each page
def main_checkAllPages(url, bucket):
    global TOTAL_CNT
    global START_TIME
    START_TIME = datetime.datetime.now()
    
    # retrieve how many pages (every pages has at most 100 users)
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'lxml')    
    maxPage = 1
    for x in soup.find(id='user-pages').find_all(href=re.compile("page=")):
        if x.get_text().isdigit():             
            page = int(x.get_text())
            if  page > maxPage:
                maxPage = page
                
    print("Total Pages found:"+ str(maxPage))
    # process all pages
    pagesUrl = url+ "?page="
    for pageIndex in range(1, maxPage+1):
            print(pageIndex)
            checkSinglePage(str(pageIndex), bucket)        
            print("checked page " + str(pageIndex))
       
    print("-------------------------SUCESS-------------------------")
    print("Start time:" + str(START_TIME))
    #print("Total users: " +str(TOTAL_CNT)+ " ,all checked")    
    print("End time:" + str(datetime.datetime.now()))
    return         
    
# main function
if __name__ == "__main__":

    # Fill these in - you get them when you sign up for S3
    AWS_ACCESS_KEY_ID = 'AKIAIMVB7CGPOS6XN53A'
    AWS_ACCESS_KEY_SECRET = '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'
    # Fill in info on data to upload
    # destination bucket name
    bucket_name = 'bia660-crawler'
    
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)
    
    bucket = conn.get_bucket(bucket_name)
            
    main_checkAllPages("https://www.kaggle.com/users", bucket)      
    #for i in range(5):
    #    print(check_exists(bucket, "userName/userName.html"))
    #print("end")        
