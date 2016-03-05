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
ROOT = "https://www.kaggle.com"

# Fill these in - you get them when you sign up for S3
AWS_ACCESS_KEY_ID = 'AKIAIMVB7CGPOS6XN53A'
AWS_ACCESS_KEY_SECRET = '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'
# Fill in info on data to upload
# destination bucket name
bucket_name = 'bia660-crawler'
    
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)
    
BUCKET = conn.get_bucket(bucket_name)

from upload import Upload

def save2html_profile(userName, root, bucket =BUCKET):
    br = webdriver.PhantomJS()
    br.get(root + '/' + userName)    
    # wait until dynamic content like "Skills" is loaded
    try:
        element = WebDriverWait(br, 6).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'col-2')]")) 
    )
    except Exception as e:
        print("no-results: "+ userName)
    finally:
        html = br.page_source.encode('utf-8') #bytes
        br.quit()           
        
    # change relative path to absolute path
    html = html.replace(b"src=\"//", b"src=\"https://")
    html = html.replace(b"src=\"/", ("src=\""+root+"/").encode())    
    html = html.replace(b"href=\"//", b"href=\"https://")
    html = html.replace(b"href=\"/", ("href=\""+root+"/").encode())
    
    # Output to html file:
    # create directory (if not exist) and 
    """
    filename = saveDir+userName+"/"+userName+".html"
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    with open(filename, "wb") as f:
        f.write(html)
        f.close()
     """
     
    # sent result to s3 drive TODO: clean files	
    #Upload.upload(os.path.dirname(filename),userName+"/")	
    userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
    Upload.upload_wo_createfile(bucket, html, '!CHECK/', userName+".html")
    return

def save2html_result(userName, root, bucket =BUCKET):
    br = webdriver.PhantomJS()
    br.get(root + "/"+ userName+"/results")
    #time.sleep(10)
    # wait until dynamic content is loaded
    try:
        element = WebDriverWait(br, 6).until(
            EC.presence_of_element_located((By.XPATH, "//tbody"))
        )
    except Exception as e:
        print("no-results: "+ userName)
    finally:
        html = br.page_source.encode('utf-8')
        br.quit()
    
    # change relative path to absolute path    
    html = html.replace(b"src=\"//", b"src=\"https://")
    html = html.replace(b"src=\"/", ("src=\""+root+"/").encode())    
    html = html.replace(b"href=\"//", b"href=\"https://")
    html = html.replace(b"href=\"/", ("href=\""+root+"/").encode())
    
    # output to html file: 
    # create directory (if not exist) and file
    """
    filename = saveDir+userName+"/"+userName+"_results.html"
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    with open(filename, "wb") as f:
        f.write(html)
        f.close()
    """
        
    # sent result to s3 drive TODO: clean files
    #Upload.upload(os.path.dirname(filename),userName+"/")
    userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
    Upload.upload_wo_createfile(bucket, html, '!CHECK/', userName+"_results.html")	
    return
    
def check_exists(b, userName):
    global ROOT
    userURL = userName
    userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
    k = boto.s3.key.Key(b, userName+"/"+userName+".html")
    if not k.exists():   
        print("******File not found: " + userName+"/"+userName+".html")
        save2html_profile(userURL, ROOT, b)
    
    j = boto.s3.key.Key(b, userName+"/"+userName+"_results.html")
    if not j.exists():
        print("**File not found: " + userName+"/"+userName+"_results.html")
        save2html_result(userURL, ROOT, b)

        
# For each user found in this page, call save2html() 
def checkSinglePage(pageIndex, bucket =BUCKET):
    global TOTAL_CNT
    #global totalUsers
    #global root
    r = requests.get("https://www.kaggle.com/users?page=" + str(pageIndex))
    soup = BeautifulSoup(r.content,'lxml')
    # retrieve user link in one page
    for ul in soup.find_all("a", class_='profilelink'):
        userName = ul['href'][1:]        
        #userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
        check_exists(bucket, userName)
        #TOTAL_CNT = TOTAL_CNT +1

        
# Web scaper main function
# find all pages, then call scrapeSinglePage() for each page
def main_checkAllPages(url, bucket=BUCKET):
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
                
    #maxPage =708            
    print("Total Pages found:"+ str(maxPage))
    # process all pages
    pool = multiprocessing.Pool() # use all available cores
    pool.map(checkSinglePage, range(1, maxPage+1))    
    pool.close()
    pool.join()   
    """
    for pageIndex in range(1, maxPage+1):
            print(pageIndex)
            checkSinglePage(str(pageIndex), bucket)        
            print("checked page " + str(pageIndex))
    """  
    print("-------------------------SUCESS-------------------------")
    print("Start time:" + str(START_TIME))
    print("Total users: " +str(TOTAL_CNT)+ " ,all checked")    
    print("End time:" + str(datetime.datetime.now()))
    return         
    
# main function
if __name__ == "__main__":            
    main_checkAllPages("https://www.kaggle.com/users")         
