-- Examples of tasks done in pgadmin4 query tool
-- Task 1: The ? in VARCHAR should be replaced with an integer representing the maximum length of the values in that column.

SELECT length(max(cast(card_number as Text)))
FROM orders_table
GROUP BY card_number
ORDER BY length(max(cast(card_number as Text))) desc
LIMIT 1; 
-- largest number = 19

ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
	ALTER COLUMN product_quantity TYPE SMALLINT;


-- Task 2
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;

ALTER TABLE dim_users
    ALTER COLUMN country_code TYPE VARCHAR(255); -- Adjust size based on your requirements

ALTER TABLE dim_users
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Task 4
UPDATE dim_products
SET weight_class =
	CASE 
		WHEN weight < 2.0 THEN 'Light'
		WHEN weight >= 2 
			AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 
			AND weight <140 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
	END;

-- Task 9:  Primary keys were added to the columns in the dim tables. Before the same columns in the orders_table were made foreign keys.
-- Adds primary keys in dim_tables
ALTER TABLE dim_card_details
	ADD CONSTRAINT pk_card_nuber PRIMARY KEY (card_number);
	
ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);
	
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
	
ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
	
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);
    
-- adss the foreign keys to the orders table
ALTER TABLE orders_table
	ADD FOREIGN KEY (card_number)
	REFERENCES dim_card_details(card_number);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times(date_uuid);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (product_code)
	REFERENCES dim_products(product_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (store_code)
	REFERENCES dim_store_details(store_code);
	
ALTER TABLE orders_table
	ADD FOREIGN KEY (user_uuid)
	REFERENCES dim_users(user_uuid);
	