
--Sensores
create table test (
createdAt 	bigint			PRIMARY KEY,
mach 		varchar(10)		NOT NULL,
temp        float(2)		NOT NULL
);


--ERP
create table salesOrder (
	idSO 				serial 			PRIMARY KEY,
	idCustomer 			int 			NOT NULL,
	idItem 				int 			NOT NULL,
	createdDate 		date 			NOT NULL,
	dueDate 			date 			CHECK(dueDate > createdDate ),
	shipDate 			date 			CHECK(closeDate > createdDate ),
	qty					int 			CHECK(qty > 0),
	qtyFullfilled 		int				CHECK(qtyFullfilled <= qty),
	qtyShipped			int				CHECK(qtyShipped <= qtyFullfilled),
	soStatus			varchar(12)		CHECK(soStatus in ('Approved', 'Partially Fulfilled', 'Fulfilled', 'Partially Shipped', 'Shipped')),
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
	FOREIGN KEY (idSO)
		REFERENCES salesOrder (idSO),
	FOREIGN KEY (idItem)
		REFERENCES salesOrder (idItem)
);

create table item (
	idItem 				serial 		PRIMARY KEY,
	name 				int 		CHECK(name in ('Item a','Item b','Item c','Item d')),
	family 				int 		CHECK(family in ('Family a','Family b')),
	cicleTime	 		float(2) 	CHECK(cicleTime > 0)
);

create table customer (
	idCustomer 			serial 		PRIMARY KEY,
	name 				int 		CHECK(name in ('Customer a','Customer b','Customer c','Customer d')),
	country 				int 		CHECK(country in ('Argentina','Brazil','Uruguay'))
);

