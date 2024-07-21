create table sensorData (
createdAt 	bigint			PRIMARY KEY,
mach 		varchar(10)		NOT NULL,
temp        float(2)		NOT NULL
);

create table item (
	idItem 				int 				PRIMARY KEY,
	name 				varchar(12) 		CHECK(name in ('Sifon Simple','Sifon PVC','Sifon Doble')),
	price	 			float(2) 			CHECK(price > 0),
	family 				varchar(12) 		CHECK(family in ('Family A','Family B')),
	cicleTime	 		float(2) 			CHECK(cicleTime > 0),
	cicleDev	 		float(2) 			CHECK(cicleDev > 0),
	maq 				varchar(6) 			CHECK(family in ('Iny 1','Iny 2','Iny 3')),
);

create table customer (
	idCustomer 			int 				PRIMARY KEY,
	name 				varchar(12) 		NOT NULL,
	country 			varchar(12) 		CHECK(country in ('Argentina','Brazil','Uruguay'))
);

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
	soStatus			varchar(50)		CHECK(soStatus in ('Approved', 'Partially Fulfilled', 'Fulfilled', 'Partially Shipped', 'Shipped')),
	UNIQUE (idSO, idItem),
	FOREIGN KEY (idItem)
		REFERENCES item (idItem),
	FOREIGN KEY (idCustomer)
		REFERENCES customer (idCustomer)
);

create table salesOrderAirflow (
	idSO 				serial 			PRIMARY KEY,
	idCustomer 			int 			NOT NULL,
	idItem 				int 			NOT NULL,
	createdDate 		date 			NOT NULL,
	dueDate 			date 			CHECK(dueDate > createdDate ),
	shipDate 			date 			CHECK(shipDate > createdDate ),
	qty					int 			CHECK(qty > 0),
	qtyFullfilled 		int				CHECK(qtyFullfilled <= qty),
	qtyShipped			int				CHECK(qtyShipped <= qtyFullfilled),
	soStatus			varchar(50)		CHECK(soStatus in ('Approved', 'Partially Fulfilled', 'Fulfilled', 'Partially Shipped', 'Shipped')),
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
	closedDate 			date 			,
	qtyCreated			int 			CHECK(qtyCreated > 0),
	scrapQty			int 			CHECK(qtyCreated > 0),
	FOREIGN KEY (idSO , idItem) references salesOrder(idSO, idItem)	
);

create table workOrderAirflow (
	idWO				serial 			PRIMARY KEY,
	idSO 				int 			NOT NULL,
	idItem 				int 			NOT NULL,
	createdDate 		date 			NOT NULL,
	closedDate 			date 			,
	qtyCreated			int 			CHECK(qtyCreated > 0),
	scrapQty			int 			CHECK(qtyCreated > 0),
	FOREIGN KEY (idSO , idItem) references salesOrder(idSO, idItem)	
);


create table quota (
	period 				date 			NOT NULL,
	idItem 				int 			NOT NULL,
	quota				int 			CHECK(quota > 0),
	UNIQUE (period, idItem)
);

create table quotaAirflow (
	period 				date 			NOT NULL,
	idItem 				int 			NOT NULL,
	quota				int 			CHECK(quota > 0),
	UNIQUE (period, idItem)
);


