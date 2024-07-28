CREATE VIEW analyticsdata.STG_CONSOLIDATED AS (
SELECT 
	T1.idso,
	T1.idcustomer,
	T1.iditem,
	T1.createddate								AS soCreatedDate,
	T1.duedate									AS soDueDate,
	T1.shipdate									AS soShipDate,
	T1.qty,
	T1.qtyfullfilled,
	T1.qty - T1.qtyfullfilled					AS qtyPending,
	T1.qtyshipped,
	T1.sostatus,
	T2.createddate								AS woCreatedDate,
	T2.closedDate								AS woClosedDate,
	T2.scrapqty,
	1										  	AS flagSum,
	DATE(DATE_TRUNC('month', T1.createddate))	AS soMonth,
	T1.qty * T3.price							AS salePrice,
	CASE 
		WHEN T1.shipdate > T1.duedate AND T1.shipdate IS NOT NULL
		THEN 1
		ELSE 0
	END				AS flagDelay,
	CASE 
		WHEN T1.shipdate IS NOT NULL
		THEN T1.shipdate - T1.createddate
		ELSE NULL
	END				AS deliveryTime,
	CASE 
		WHEN T2.createddate IS NOT NULL
		THEN T2.closedDate - T2.createddate
		ELSE NULL
	END				AS fabricationTime,
	CASE 
		WHEN T2.createddate IS NOT NULL
		THEN (T2.closedDate - T2.createddate) * 8 * 3600 / T1.qtyfullfilled
		ELSE NULL
	END				AS cicleTime,
	CASE 
		WHEN T1.shipdate IS NOT NULL
		THEN T1.shipdate - T2.closedDate
		ELSE NULL
	END				AS dispatchTime,
	(SELECT 
       	AVG(sensor.temp)
     FROM 
        analyticsdata.sensordata sensor 
     WHERE 
        sensor.createdAt BETWEEN EXTRACT(EPOCH FROM T2.createddate) AND EXTRACT(EPOCH FROM T2.closedDate)
        AND 
        sensor.mach = T3.maq	
    	) 			AS avgMoldTemp
FROM analyticsdata.salesorder T1
LEFT JOIN analyticsdata.workorder T2
ON T1.idso = T2.idso AND T1.iditem = T2.iditem
LEFT JOIN analyticsdata.item T3
ON T1.iditem = T3.iditem
);


CREATE VIEW analyticsdata.STG_SENSORDATA AS (
SELECT 
	to_timestamp(createdat)		AS createdat,
	mach, 
	"temp"
FROM analyticsdata.sensordata
);

