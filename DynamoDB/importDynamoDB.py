#!/usr/bin/python
import csv
import sys
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table



def main():
    
    #conn = boto.dynamodb.connect_to_region('us-east-1',
    #    aws_access_key_id='AKIAIUUSLBP7GFHBNSDA',
    #    aws_secret_access_key='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')
    #print "connect to the DynamoDB"
    table = Table('pictures') 
    print "connect to the DynamoDB table"
    
    with open ("caltech-256.csv",'rw') as csvfile:
        reader = csv.reader(csvfile)
        for category, picture, s3url in reader:
            
            #print category + ' ' picture + ' ' + s3url
            if picture=='Picture':
                continue 
            
            table.put_item(data={
               	'category': category,
               	'picture': int(picture),
               	'content': s3url,
               	})  
if __name__ == "__main__":
    main()