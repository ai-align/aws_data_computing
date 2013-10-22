#!/usr/bin/python


import mysql.connector
import csv
con = mysql.connector.connect(user ='u', password = 'pdb15319root', database = 'test')
cur = con.cursor()

def main():

    with open('songs.csv', 'rb') as csvfile:
        for row in csvfile:
            print row
            cur.excute("INSERT INTO A songs (track_id, title, song_id, release, artist_id, artist_mbid, artist_name, duration, artist_familiarity, artist_hotttnesss, year) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s')", tuple(row[0].split()))
            
            
cur.commit()
cur.close
        
if __name__ == "__main__":
    main()
    
