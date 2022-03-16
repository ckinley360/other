/*Christian Kinley
  2/6/2016
  
  T-SQL Stored Procedure: Active Holds by Court within Date Range
  Purpose: Produces a data set of eligible hold days for inmates, which will be used in 
           Crystal Reports, where it will be filtered by court(s) and a date range, and then counted.*/

CREATE PROCEDURE [ProcedureC]
AS
BEGIN

------------------------------------------------------------------------------------------------
/*Extract data from source system and insert into SQL Server table.*/
TRUNCATE TABLE TableA; --Clear out old data.

INSERT INTO TableA(InmateNumber, LastNm, FirstNm, MidNm, 
                   BookingNumber, InDate, OutDate,
		   OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes, 
		   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate)
    SELECT *
    FROM OPENQUERY([LinkedServerA], 'SELECT <Columns>
	                             FROM <Source System Tables>
				     WHERE <Filters>');

----------------------------------------------------------------------------------------------------
/*Extract data from another table in source system and insert into another SQL Server table.
  This data will be used to filter out temporary release days.*/
TRUNCATE TABLE TableX; --Clear out old data.

INSERT INTO TableX(BookingNumber, CreditArriveDt, CreditReleaseDt, ReleaseType)
    SELECT *
    FROM OPENQUERY([LinkedServerA], 'SELECT <Columns>
	                             FROM <Source System Tables>');
										
--------------------------------------------------------------------------------------------------
/*Replace NULL release/end dates with current datetime. NULL release/end dates are interpreted as not-yet-determined,
  so today's datetime will be used instead for the sake of calculations.*/
UPDATE TableA
SET OutDate = GETDATE()
WHERE OutDate IS NULL;

UPDATE TableX
SET CreditReleaseDt = GETDATE()
WHERE CreditReleaseDt IS NULL;

UPDATE TableA
SET BillingEndDate = GETDATE()
WHERE BillingEndDate IS NULL;

--------------------------------------------------------------------------------------------------
/*Create a "tally table" for the booking days. This breaks up the booking date ranges (InDate & OutDate)
  into individual days (BookingDay) to allow day-level calculations. Algorithm was inspired by 
  Dwain Camps' algorithm explained in his article: https://dwaincsql.com/2014/03/27/tally-tables-in-t-sql/
  Thank you to Luis Cazares on SQLServerCentral.com for sharing this algorithm with me.*/
TRUNCATE TABLE TableB; --Clear out old data.

/*Create tally table.*/
WITH
E(n) AS(
    SELECT n FROM (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0))E(n)
),
E2(n) AS(
    SELECT a.n FROM E a, E b
),
E4(n) AS(
    SELECT a.n FROM E2 a, E2 b
),
cteTally(n) AS(
    SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) - 1 n
    FROM E4
),
cteResult AS(
    SELECT InmateNumber, LastNm, FirstNm, MidNm, 
	   BookingNumber, DATEADD( dd, DATEDIFF( dd, 0, h.InDate) + n, 0) AS CalDay, OutDate,
           OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes, 
	   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate
    FROM TableA h
        JOIN cteTally t ON h.InDate < DATEADD( dd, DATEDIFF( dd, 0, h.OutDate), -t.n + 1)
    GROUP BY InmateNumber, LastNm, FirstNm, MidNm, 
	     BookingNumber, DATEADD( dd, DATEDIFF( dd, 0, h.InDate) + n, 0), OutDate, 
	     OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes, 
	     SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate
)                                                                

/*Insert data from the tally table into TableB.*/
INSERT INTO TableB(InmateNumber, LastNm, FirstNm, MidNm, 
                   BookingNumber, BookingDay, OutDate, 
		   OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes, 
		   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate) 
	SELECT InmateNumber, LastNm, FirstNm, MidNm, 
	       BookingNumber, CalDay, OutDate,
               OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes,
	       SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate
	FROM cteResult
	ORDER BY InmateNumber, BookingNumber, CalDay, OffenseNumber;

------------------------------------------------------------------------------
/*For each record for each BookingNumber, find next CreditArriveDt and previous CreditReleaseDt and insert into
  TableY. This data will be used to filter out "temporary release" days.
  Algorithm source: https://blog.sqlauthority.com/2013/09/22/sql-server-how-to-access-the-previous-row-and-next-row-value-in-select-statement/ */
TRUNCATE TABLE TableY; --Clear out old data.

WITH CTE AS 
(
SELECT rownum = ROW_NUMBER() OVER (ORDER BY A.BookingNumber, CreditArriveDt),
       A.BookingNumber, A.CreditArriveDt, A.CreditReleaseDt, A.ReleaseType
FROM TableX A
)
INSERT INTO TableY(PreviousCreditReleased, BookingNumber, CreditArriveDt, CreditReleaseDt,
                   ReleaseType, NextCreditArrived)
    SELECT
       prev.CreditReleaseDt PreviousValue,
       CTE.BookingNumber, CTE.CreditArriveDt, CTE.CreditReleaseDt, CTE.ReleaseType,
       nex.CreditArriveDt NextValue
    FROM CTE
       LEFT JOIN CTE prev ON (prev.rownum = CTE.rownum - 1) AND (prev.BookingNumber = CTE.BookingNumber)
       LEFT JOIN CTE nex ON (nex.rownum = CTE.rownum + 1) AND (nex.BookingNumber = CTE.BookingNumber) 
    WHERE CTE.ReleaseType = 'Temporary Release'
    ORDER BY BookingNumber, CreditArriveDt;

------------------------------------------------------------------------------
/*Convert all fields that are DATETIME type into DATE type to simplify comparisons.*/
UPDATE TableB
SET OffenseDate = CAST(OffenseDate AS DATE);

UPDATE TableB
SET SentenceStartDate = CAST(SentenceStartDate AS DATE);

UPDATE TableB
SET SentenceEndDate = CAST(SentenceEndDate AS DATE);

UPDATE TableB
SET SentPrjCmpDt = CAST(SentPrjCmpDt AS DATE);

UPDATE TableB
SET BillingStartDate = CAST(BillingStartDate AS DATE);

UPDATE TableB
SET BillingEndDate = CAST(BillingEndDate AS DATE);

UPDATE TableX
SET CreditArriveDt = CAST(CreditArriveDt AS DATE);

UPDATE TableX
SET CreditReleaseDt = CAST(CreditReleaseDt AS DATE);

UPDATE TableY
SET PreviousCreditReleased = CAST(PreviousCreditReleased AS DATE);

UPDATE TableY
SET CreditArriveDt = CAST(CreditArriveDt AS DATE);

UPDATE TableY
SET CreditReleaseDt = CAST(CreditReleaseDt AS DATE);

UPDATE TableY
SET NextCreditArrived = CAST(NextCreditArrived AS DATE);

---------------------------------------------------------------------
/*Replace newline or carriage return character in OffenseNotes with a space to make it more legible.*/
UPDATE TableB
SET OffenseNotes = REPLACE(REPLACE(OffenseNotes, CHAR(13), ' '), CHAR(10), ' ')
WHERE OffenseNotes IS NOT NULL
    AND OffenseNotes <> '';

--------------------------------------------------------------------------------------------------
/*Filter out temporary release days from TableB and insert remaining 
  booking days into TableC.*/
TRUNCATE TABLE TableC; --Clear out old data.

INSERT INTO TableC(InmateNumber, LastNm, FirstNm, MidNm, 
                   BookingNumber, BookingDay, OutDate,
                   OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes,
		   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate)
    SELECT InmateNumber, LastNm, FirstNm, MidNm,
           A.BookingNumber, BookingDay, OutDate,
           OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes,
	   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate
    FROM TableB AS A
        LEFT JOIN TableY AS B
	    ON A.BookingNumber = B.BookingNumber
    WHERE 
    (
     B.BookingNumber IS NOT NULL  --Indicates that there is a match.
     AND A.BookingDay <= B.CreditReleaseDt
     OR( (A.BookingDay >= B.NextCreditArrived) AND (B.NextCreditArrived IS NOT NULL) )
     )
     OR
     (
      B.BookingNumber IS NULL  --Indicates that there is not a match.
      );

----------------------------------------------------------------------------------------------------
/*Filter data in TableC based on logic defined by law enforcement subject matter expert, and insert
  into final table - TableD.*/
TRUNCATE TABLE TableD; --Clear out old data.

--Filter the data and insert into final table.
INSERT INTO TableD(InmateNumber, LastNm, FirstNm, MidNm, 
                   BookingNumber, BookingDay, OutDate, 
		   OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes,
		   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate)
    SELECT InmateNumber, LastNm, FirstNm, MidNm, 
           BookingNumber, BookingDay, OutDate, 
	   OffenseNumber, OffenseDate, CourtCode, CourtName, Disposition, DateOfDispo, OffenseNotes, 
	   SentenceStartDate, SentPrjCmpDt, SentenceEndDate, BillingStartDate, BillingEndDate
	FROM TableC
	WHERE ( --BEGIN LOGIC BLOCK #1
	         ((OffenseNotes LIKE '%[^a-z]PR[^a-z]%' OR OffenseNotes LIKE 'PR[^a-z]%' OR OffenseNotes LIKE '%[^a-z]PR' OR OffenseNotes LIKE '%[^a-z]P/R[^a-z]%' OR OffenseNotes LIKE 'P/R[^a-z]%' OR OffenseNotes LIKE '%[^a-z]P/R') AND (BookingDay >= BillingStartDate AND BookingDay <= BillingEndDate)) --If Personal Recognizance (PR or P/R), then include only booking days that fall within BillingStartDate & BillingEndDate.
             OR ((OffenseNotes NOT LIKE '%[^a-z]PR[^a-z]%' AND OffenseNotes NOT LIKE 'PR[^a-z]%' AND OffenseNotes NOT LIKE '%[^a-z]PR' AND OffenseNotes NOT LIKE '%[^a-z]P/R[^a-z]%' AND OffenseNotes NOT LIKE 'P/R[^a-z]%' AND OffenseNotes NOT LIKE '%[^a-z]P/R') OR (OffenseNotes IS NULL OR OffenseNotes = '')) --If not Personal Recognizance or if OffenseNotes IS NULL, then don't do anything. Just continue on to the next line of logic.
	      ) --END LOGIC BLOCK #1
	   AND ( --BEGIN LOGIC BLOCK #2
	          (Disposition = '' AND (BookingDay >= ISNULL(OffenseDate, BillingStartDate) AND BookingDay <= BillingEndDate)) --Case 1: Disposition is blank.
	          OR (Disposition = 'SEN' AND (BookingDay >= SentenceStartDate AND BookingDay <= ISNULL(SentPrjCmpDt, BillingEndDate)) OR (BookingDay >= OffenseDate AND BookingDay <= ISNULL(DateOfDispo, SentenceStartDate))) --Case 2: Disposition = 'SEN', which means Sentenced.
	          OR ((Disposition <> '' AND Disposition <> 'SEN') AND (BookingDay >= OffenseDate AND BookingDay <= DateOfDispo)) --Case 3: Disposition is neither blank nor 'SEN'.
	       ) --END LOGIC BLOCK #2

END
