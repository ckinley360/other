# Active Holds by Court within Date Range

Folder contents:
  1) Stored procedure: usp_ActiveHoldsByCourtWithinDateRange.sql
  
Purpose: This stored procedure contains an ETL process that extracts data from a source system (containing inmate data from a law enforcement corrections bureau), applies logic to prepare the data for use in a reporting tool (i.e. Crystal Reports), where it is used to calculate the number of inmates held by specified court(s) within a date range provided by the user. This logic includes breaking up date ranges into individual days; filtering out ineligible temporary release days; and applying a set of conditional filters for different scenarios involving release on personal recognizance, billing dates, offense dispositions, sentence dates, booking dates, and disposition dates.

The report has been used to provide insight to councils, committees, executives, law enforcement, and citizens in constructing a jail use agreement and in planning for a new, multi-million dollar jail facility.

For more background information, see the following newspaper article:

http://www.bellinghamherald.com/news/local/article129286939.html
