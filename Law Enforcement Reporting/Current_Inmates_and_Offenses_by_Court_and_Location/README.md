# Current Inmates and Offenses by Court and Location

Folder contents:
  1) Stored procedure: usp_CurrentInmatesAndOffensesByCourtAndLocation.sql
  2) Table-valued function: tvf_Split.sql
  
Purpose: These scripts comprise an ETL process that extracts data from a source system (containing inmate data from a law enforcement corrections bureau), applies logic to filter the data for the sentencing court(s) and facility/facilities provided by the user, and returns a data set to be consumed by a reporting tool (i.e. Crystal Reports).

The report has been used by law enforcement decision-makers to determine which current inmates must be housed at the local corrections facility/facilities, and which can legally be relocated to another corrections facility outside of the county.

For more background information, see the section titled "Jail transfers limited" in the following newspaper article:

http://www.bellinghamherald.com/news/local/article54592765.html
