# MemoryBox

The goal of MemoryBox is to track a small number of transactions and the details of the 
transactions for a critical time period.

MemoryBox maintains structured data as JSONB from the source as well as text and binary 
files in a PostGreSQL database. MemoryBox requires a source connection to the database and
templated SQL queries for information tracking.
