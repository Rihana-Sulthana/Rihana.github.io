# Rihana.github.io
## Description
This is a simple code used to compare data from two databases Oracle and Snowflake.

## Files
- The config.cfg file has configuration required to connect to both the databases
- The constants.py file has constants used in the conn.py, which includes query,some exceptions columns, and any modification to be done on specific column types
- the conn.py file has the code to compare data from the two databases

## Features
-  The conn.py file takes constants and config file name as input without the file extensions
-  the constants file is imported dynamically so that the code base is used to compare any tables
-  The config file is used to grab database connection credentials
-  This code base is used to compare any two tables available in oracle and snowflake with added conditions.

