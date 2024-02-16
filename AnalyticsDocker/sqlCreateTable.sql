--Sensores
create table sensorData (
createdAt 	bigint			PRIMARY KEY,
mach 		varchar(10)		NOT NULL,
temp        float(2)		NOT NULL
);

create table item (
	idItem 				serial 				PRIMARY KEY,
	name 				varchar(12) 		CHECK(name in ('Sifon Simple','Sifon PVC','Sifon Doble')),
	price	 			float(2) 			CHECK(price > 0),
	family 				varchar(12) 		CHECK(family in ('Family A','Family B')),
	cicleTime	 		float(2) 			CHECK(cicleTime > 0)
);

create table customer (
	idCustomer 			serial 				PRIMARY KEY,
	name 				varchar(12) 		CHECK(name in ('Customer a','Customer b','Customer c','Customer d')),
	country 			varchar(12) 		CHECK(country in ('Argentina','Brazil','Uruguay'))
);

--ERP
create table salesOrder (
	idSO 				serial 			PRIMARY KEY,
	idCustomer 			int 			NOT NULL,
	idItem 				int 			NOT NULL,
	createdDate 		date 			NOT NULL,
	dueDate 			date 			CHECK(dueDate > createdDate ),
	shipDate 			date 			CHECK(shipDate > createdDate ),
	qty					int 			CHECK(qty > 0),
	qtyFullfilled 		int				CHECK(qtyFullfilled <= qty),
	qtyShipped			int				CHECK(qtyShipped <= qtyFullfilled),
	soStatus			varchar(12)		CHECK(soStatus in ('Approved', 'Partially Fulfilled', 'Fulfilled', 'Partially Shipped', 'Shipped')),
	UNIQUE (idSO, idItem),
	FOREIGN KEY (idItem)
		REFERENCES item (idItem),
	FOREIGN KEY (idCustomer)
		REFERENCES customer (idCustomer)
);

create table workOrder (
	idWO				serial 			PRIMARY KEY,
	idSO 				int 			NOT NULL,
	idItem 				int 			NOT NULL,
	createdDate 		date 			NOT NULL,
	qtyCreated			int 			CHECK(qtyCreated > 0),
	scrapQty			int 			CHECK(qtyCreated > 0),
	FOREIGN KEY (idSO , idItem) references salesOrder(idSO, idItem)	
);



