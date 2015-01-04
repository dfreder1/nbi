nbi
===

Some various scripts for processing National Bridge Inventory data. Currently, running
scripts a, b, and c will create a postgres database from the raw undelimited ascii files
available from FHWA.

Requirements:
Python 2.7 and modules psycopg2, csv, and time.
Postgres
State FIPS code files 'US_state_FIPS.csv' and 'US_county_FIPS.csv' available online from USEPA

a.py:
Run this script to create a table within a postgres database. Change the name of the
table created, the database name, etc.

b.py:
After downloading the raw undelimited NBI ascii files from the NBI website, change the name of the input 
file and output file and file location down near line 200

c.py
Change the input and output appropriately around line 10.

