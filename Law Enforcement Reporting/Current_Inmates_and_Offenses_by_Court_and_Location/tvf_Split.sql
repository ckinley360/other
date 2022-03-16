/*Christian Kinley
  2/25/2016
  
  Purpose: Receives two input parameters: 
               1. A string holding a delimited list of values
               2. A one-character delimiter such as a comma
           Extracts each individual value from the string and returns each value as a separate
	   row of the returned table.
  Notes: I did not design this function. I took Ole Michelsen's function (see source below), 
         modified the name and parameter data types, and used it for my "Current Inmates and Offenses
	 by Court and Location" stored procedure.
  Source: https://ole.michelsen.dk/blog/split-string-to-table-using-transact-sql.html */
  

CREATE FUNCTION [dbo].[tvfSplit]
(
    @String VARCHAR(1000),
    @Delimiter CHAR(1)
)
RETURNS TABLE
AS
RETURN
(
    WITH Split(stpos,endpos)
    AS(
        SELECT 0 AS stpos, CHARINDEX(@Delimiter,@String) AS endpos
        UNION ALL
        SELECT endpos+1, CHARINDEX(@Delimiter,@String,endpos+1)
            FROM Split
            WHERE endpos > 0
    )
    SELECT 'Id' = ROW_NUMBER() OVER (ORDER BY (SELECT 1)),
        'Data' = SUBSTRING(@String,stpos,COALESCE(NULLIF(endpos,0),LEN(@String)+1)-stpos)
    FROM Split
);
