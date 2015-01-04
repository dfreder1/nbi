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
This script uses the module psycopg2 to create a table in a Postgres database
that has 131 rows of values corresponding 
to the data in the NBI records.
Credit to Chad Cooper for most of the code.  This is meant to be a non-ArcGIS
alternative for working with NBI data. 

Run this script to create a table within a postgres database. Change the name of the
table created, the database name, etc.

b.py:
This script processes the raw undelimited NBI data available at http://www.fhwa.dot.gov/bridge/nbi/ascii.cfm
The raw data is parsed, in some cases functions are called which add information, such as translating the state
code into an actual state name, translating the route prefix into it's text description (3='State highway')
etc.

The output of this script is an intermediate comma-separated-value text file. Then a separate script is then used to 
write that text data into a postgres database table.
After downloading the raw undelimited NBI ascii files from the NBI website, change the name of the input 
file and output file and file location down near line 200

c.py
This script reads data from the intermediate text csv file created by b.py
and writes it to a table inside a postgres database.

Change the input and output appropriately around line 10.

