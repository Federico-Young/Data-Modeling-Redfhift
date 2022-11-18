Schema

========================
# Credentials

Create a dwh.cfg file with your credentials for S3, IAM Role and Amazon Redshift.

# Fact table

The fact table is called songplay table.
This table is transactional. Each transaction is a unique song_play_id.

# Dimension tables

## User table:

* userId
* firstName
* lastName 
* gender
* level

## song table:

* song_id 
* title
* artist_id 
* year 
* duration

## Artist table

* artist_id 
* artist_name 
* artist_location 
* artist_latitude 
* artist_longitude

## Time table:

* ts timestamp 
* hour 
* day 
* week 
* month 
* year 
* weekday

How to Run the code

==================================

To run the code write the following command in the terminal:

`python3 file.py`

and replace the file for the file name you want to run.
