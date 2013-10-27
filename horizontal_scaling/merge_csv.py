#!/usr/bin/python

import csv

def main():
    
    fout = open("out.csv","a")
    
    for num in range(0,52):
        f = open("week"+str(num)+".csv")
        
        for line in f:
            fout.write(line)
            print(line)
        f.close
    fout.close
    
    
if __name__ == "__main__":
    main()