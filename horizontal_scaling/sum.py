#!/usr/bin/python

import csv

def main():
    
    value = 0.0
    with open ('out.csv','rb') as csvfile: 
    
        for row in csvfile:
            value =  value + (float)(str(row.split(",")[1]))
            #print str(row.split(",")[1].strip())
            
        print value
    
if __name__ == "__main__":
    main()