import boto
import boto.s3

import os.path
import sys

class Upload:
    def init():
       # Fill these in - you get them when you sign up for S3
       AWS_ACCESS_KEY_ID = 'AKIAIMVB7CGPOS6XN53A'
       AWS_ACCESS_KEY_SECRET = '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'
       # Fill in info on data to upload
       # destination bucket name
       bucket_name = 'bia660-crawler'
    
       conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)    
       bucket = conn.get_bucket(bucket_name)
       return bucket
    
    def upload(bucket, sourceDir, destDir):   
       uploadFileNames = []
       for (sourceDir, dirname, filename) in os.walk(sourceDir):
           uploadFileNames.extend(filename)
           break
    
       #print("filesNames")
       #print(uploadFileNames)
        
       for filename in uploadFileNames:
           sourcepath = os.path.join(sourceDir,filename)
           destpath = os.path.join(destDir, filename)
           print('Uploading %s to Amazon S3 bucket %s' % \
                  (sourcepath, bucket_name))

           filesize = os.path.getsize(sourcepath)

           k = boto.s3.key.Key(bucket)
           k.key = destpath
           k.set_contents_from_filename(sourcepath,
                    cb=None, num_cb=10)
                    
    def upload_wo_createfile(bucket, str, destDir, filename, content_type="text/html"):
           destpath = os.path.join(destDir, filename)
           k = boto.s3.key.Key(bucket)
           k.key = destpath
           k.content_type = content_type
           k.set_contents_from_string(str) 
    