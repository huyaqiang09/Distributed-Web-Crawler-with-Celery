# Distributed-Web-Crawler-with-Celery
Python: selenium, beautifulsoup2, celery, rabbitmq, Amazon AWS(EC2, S3)


client.py : running on one machine, is the "main" script to collect urls and send them to workers

            python client.py or python client.py aNumber (only 1~that number pages be tested)

proj folder: running on many workers to do tasks to save htmls to s3 storage

    __init__.py
    celery.py
    task.py
    upload.proj
    
admin folder: after client and workers finished all tasks, there are some scripts to verify its completeness

    check.py: to find the first missing html file during the process if some error happends
    checkAndUpload.py: to check which is missing but also upload the missing ones to s3
    count.py：to find the total user numbers on kaggle.com
    upload.py: is used by checkAndUpload, same with that one in proj folder
    

you need to install all necessary libs before you run the code
