#!/usr/bin/python

import csv

def main():
    

    value = 0
    for num in range(1, 52):
        f = open("week"+str(num)+".csv","a")
        i = 0
        with open("week0.csv", 'rw') as csvfile:
            for row in csvfile:
                try:
                   value = float(str(row.split(",")[1].strip()))*(1 + 0.12*(float)(num)/52)
                   line = str(i) + "," + str(value)
                   print str(line)+"\n"
                   f.write(str(line)+"\n")
                except ValueError,e:
                   print e
                i = i + 1
                
       
if __name__ == "__main__":
    main()
    
