/*Christian Kinley
  2/25/2016
  
  T-SQL Stored Procedure: Current Inmates and Offenses by Court and Location
  Purpose: Produces a data set of all current inmates and their offenses, filtered by input court(s) and
           location(s). It will show a current inmate if he/she has active sentence(s) only from one or more
           of the input courts and if he/she is currently in one of the input locations.
		   
           This stored procedure is meant to be used as a data source in Crystal Reports, where the user
	   can provide the input parameter values.*/

CREATE PROCEDURE [ProcedureB]
	@Courts varchar(1000), --Input parameters to receive a comma-delimited string of values, with no spaces between values.
	@Locations varchar(1000)
AS                          
BEGIN
--------------------------------------------------------------------------------------------------------
	/*Assign the multiple values in the parameters to the corresponding table variables, one value per row.*/
	DECLARE @CourtsTV AS TABLE
	(
		ID int,
		Court char(5)
	);

	DECLARE @LocationsTV AS TABLE
	(
		ID int,
		Location char(17)
	);

	INSERT INTO @CourtsTV(ID, Court)
		SELECT Id, Data
		FROM [dbo].[tvfSplit](@Courts, ',');

	INSERT INTO @LocationsTV(ID, Location)
		SELECT Id, Data
		FROM [dbo].[tvfSplit](@Locations, ',');

--------------------------------------------------------------------------------------------------------
    /*Extract the data from source system and insert into SQL Server table.*/
    TRUNCATE TABLE TableA; --Clear out old data.

    INSERT INTO TableA(InmateNumber, LastNm, FirstNm, MidNm, Bdate, Gender, Location, Category1, Offense,
                       Court, Disposition, DateOfDispo, ScheduledCmpDt)
    	SELECT *
	FROM OPENQUERY([LinkedServerA], 'SELECT <Columns>
                                         FROM <Source System Tables>
					 WHERE <Filters>');

--------------------------------------------------------------------------------------------------------
    /*Apply the filter and insert into another table.*/
    TRUNCATE TABLE TableB; --Clear out old data.

    INSERT INTO TableB(InmateNumber, LastNm, FirstNm, MidNm, Bdate, Gender, Location, Category1, Offense,
                       Court, Disposition, DateOfDispo, ScheduledCmpDt)
    	SELECT *
        FROM TableA AS A
        WHERE
            /*Want inmates with active sentence(s) from input court(s)*/
            EXISTS (SELECT * 
                    FROM TableA AS B
		    WHERE((Court IN (SELECT Court FROM @CourtsTV) AND Disposition = 'Sentenced' AND ScheduledCmpDt >= GETDATE()) --Active sentence from input courts.
                          ) AND A.InmateNumber = B.InmateNumber		  
		    )
             AND
	    /*Do not want inmates with active sentence(s), or that could potentially be given an active sentence, from other courts.*/
            NOT EXISTS (SELECT *
                        FROM TableA AS C
			WHERE(    (Court NOT IN (SELECT Court FROM @CourtsTV) AND Disposition = 'Sentenced' AND ScheduledCmpDt >= GETDATE()) --Active sentence from non-input courts.
	                      OR  (Court NOT IN (SELECT Court FROM @CourtsTV) AND (Disposition = '' OR Disposition IS NULL)) --Sentence from non-input courts that could potentially become an active sentence.
			           ) AND A.InmateNumber = C.InmateNumber
			      );

--------------------------------------------------------------------------------------------------------
    /*Query for the result set*/
	SELECT *
	FROM TableB
	WHERE Category1 IN (SELECT Location FROM @LocationsTV) --Filter for input locations.
	ORDER BY LastNm, FirstNm, MidNm, InmateNumber; --Sort the data because cannot sort properly in Crystal Reports (grouping problem).
	                                                     --In CR, group by inmate number to ensure correct inmate, and sort group in original order.
END
