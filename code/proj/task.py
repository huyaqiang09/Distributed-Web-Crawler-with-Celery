"""
@author: jguo
"""
from __future__ import absolute_import

from proj.celery import app
from proj.upload import Upload

import os
import time
import datetime
import errno
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

ROOT = "https://www.kaggle.com"
SAVE_PATH = "/home/ec2-user/foo/"
BUCKET = Upload.init()

#fix http://stackoverflow.com/questions/22674950/python-multiprocessing-job-to-celery-task-but-attributeerror


# Given userName, save user's profile and competion result as html file on disk
@app.task(bind=True)
def save2html(self, userName,saveDir = SAVE_PATH, bucket = BUCKET): 
    """
    from celery.signals import worker_process_init
    from multiprocessing import current_process

    @worker_process_init.connect
    def fix_multiprocessing(**kwargs):
        try:
            current_process()._config
        except AttributeError:
            current_process()._config = {'semprefix': '/mp'}
            
    from multiprocessing import Process
    #multiprocessing.freeze_support()
    Process(target=save2html_profile, args=(userName)).start()
    Process(target=save2html_result, args=(userName)).start()
   
    pool = multiprocessing.Pool()
    pool.apply_async(save2html_profile,[userName])
    pool.apply_async(save2html_result,[userName])
    pool.close()
    pool.join()
    """
    try:
        save2html_profile(userName)
        save2html_result(userName)
        userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
        print(userName)
    except Exception as e:
        userName=userName.split('/')[-1] #bug fix "/users/176217/lionel-pigou"
        Upload.upload_wo_createfile(bucket, userName, "!EEOR", userName+".txt", "text/plain")
        raise self.retry(countdown=5, exc=e, max_retries=3)
    return

def save2html_profile(userName, saveDir = SAVE_PATH, root = ROOT, bucket = BUCKET):
    br = webdriver.PhantomJS()
    br.get(ROOT + '/' + userName)    
    # wait until dynamic content like "Skills" is loaded
    try:
        element = WebDriverWait(br, 20).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'col-2')]")) 
    )
    except Exception as e:
        print("no-profile: "+ userName)
    finally:
        html = br.page_source.encode('utf-8') #bytes
        br.quit()           
        
    # change relative path to absolute path
    html = html.replace(b"src=\"//", b"src=\"https://")
    html = html.replace(b"src=\"/", ("src=\""+ROOT+"/").encode())    
    html = html.replace(b"href=\"//", b"href=\"https://")
    html = html.replace(b"href=\"/", ("href=\""+ROOT+"/").encode())
    
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
    Upload.upload_wo_createfile(bucket, html, userName+'/', userName+".html")
    return

def save2html_result(userName, saveDir = SAVE_PATH, root = ROOT, bucket = BUCKET):
    br = webdriver.PhantomJS()
    br.get(ROOT + "/"+ userName+"/results")
    #time.sleep(10)
    # wait until dynamic content is loaded
    try:
        element = WebDriverWait(br, 20).until(
            EC.presence_of_element_located((By.XPATH, "//tbody"))
        )
    except Exception as e:
        print("no-results: "+ userName)        
    finally:
        html = br.page_source.encode('utf-8')
        br.quit()
        
    # change relative path to absolute path    
    html = html.replace(b"src=\"//", b"src=\"https://")
    html = html.replace(b"src=\"/", ("src=\""+ROOT+"/").encode())    
    html = html.replace(b"href=\"//", b"href=\"https://")
    html = html.replace(b"href=\"/", ("href=\""+ROOT+"/").encode())
    
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
    Upload.upload_wo_createfile(bucket, html, userName+'/', userName+"_results.html")	
    return