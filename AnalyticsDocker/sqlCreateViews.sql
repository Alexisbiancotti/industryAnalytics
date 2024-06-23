CREATE VIEW STG_CONSOLIDATED AS (
SELECT 
	T1.idso,
	T1.idcustomer,
	T1.iditem,
	T1.createddate	AS soCreatedDate,
	T1.duedate		AS soDueDate,
	T1.shipdate		AS soShipDate,
	T1.qty,
	T1.qtyfullfilled,
	T1.qtyshipped,
	T1.sostatus,
	T2.createddate	AS woCreatedDate,
	T2.closedDate	AS woClosedDate,
	T2.scrapqty,
	1				AS flagSum,
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
		WHEN T1.shipdate IS NOT NULL
		THEN T1.shipdate - T2.closedDate
		ELSE NULL
	END				AS dispatchTime
FROM public.salesorder T1

LEFT JOIN public.workorder T2

ON T1.idso = T2.idso AND T1.iditem = T2.iditem
);