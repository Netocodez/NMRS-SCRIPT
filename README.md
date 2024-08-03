THIS IS A MAKESHIFT TO GENERATING NMRS ART LINE LIST FROM YOUR NORMAL NMRS WEB INTERFACE

This is a python script that connects to NMRS database and pulls data using a query

the app assumes that below is your database details:
host='localhost',
database='openmrs',
user='root',
password='root'

the query will take about 20 minutes to run depending on the size of your database
it will also prompt you to save once it's done with the query

please don't close the app untill you see file dialogue to save
also don't assume that the row counting means that the query has completed (it only checks number of rows to be processed)
