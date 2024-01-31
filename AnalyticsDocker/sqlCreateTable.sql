
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
	closeDate 			date 			CHECK(closeDate > createdDate ),
	qty					int 			CHECK(qty > 0),
	qtyFullfilled 		int				CHECK(qtyFullfilled <= qty),
	qtyShipped			int				CHECK(qtyShipped <= qtyFullfilled),
	soStatus			varchar(12)		CHECK(soStatus in ('approved','partially fulfilled','fulfilled','shipped')),
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
	name 				int 		CHECK(itemName in ('item a','item b','item c','item d')),
	family 				int 		CHECK(itemName in ('family a','family b')),
	cicleTime	 		float(2) 	CHECK(cicleTime > 0)
);

create table customer (
	idCustomer 			serial 		PRIMARY KEY,
	name 				int 		CHECK(itemName in ('customer a','customer b','customer c','customer d')),
	region 				int 		CHECK(itemName in ('region a','region b'))
);

