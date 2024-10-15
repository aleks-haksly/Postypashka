/*CОЗДАНИЕ ТАБЛИЦ*/
CREATE TABLE users (
  id INT,
  name STRING,
  age INT
)
PARTITIONED BY (country STRING)
CLUSTERED BY (id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Вставка данных с указанием значений для разделов
INSERT INTO users PARTITION (country='USA') VALUES
(1, 'John Doe', 28),
(3, 'Bob Johnson', 45),
(7, 'Tom Brown', 33);

INSERT INTO users PARTITION (country='Canada') VALUES
(2, 'Jane Smith', 34);

INSERT INTO users PARTITION (country='UK') VALUES
(4, 'Alice White', 30);

INSERT INTO users PARTITION (country='Germany') VALUES
(5, 'Michael Green', 41);

INSERT INTO users PARTITION (country='France') VALUES
(6, 'Laura Black', 25);

REATE TABLE orders (
  order_id INT,
  user_id INT,
  order_amount DOUBLE
)
PARTITIONED BY (order_date STRING)
CLUSTERED BY (user_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Вставка данных с указанием значений для разделов
INSERT INTO orders PARTITION (order_date='2023-08-01') VALUES
(101, 1, 250.75);

INSERT INTO orders PARTITION (order_date='2023-08-05') VALUES
(102, 2, 300.50);

INSERT INTO orders PARTITION (order_date='2023-08-10') VALUES
(103, 1, 125.00);

INSERT INTO orders PARTITION (order_date='2023-08-12') VALUES
(104, 3, 500.00);

INSERT INTO orders PARTITION (order_date='2023-08-15') VALUES
(105, 5, 175.99);

INSERT INTO orders PARTITION (order_date='2023-08-18') VALUES
(106, 6, 200.25);

INSERT INTO orders PARTITION (order_date='2023-08-20') VALUES
(107, 7, 320.89);

INSERT INTO orders PARTITION (order_date='2023-08-22') VALUES
(108, 4, 450.50);


CREATE EXTERNAL TABLE external_users (
  id INT,
  name STRING,
  age INT
)
PARTITIONED BY (country STRING)
CLUSTERED BY (id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/external/users';

-- Вставка данных с указанием значений для разделов
INSERT INTO external_users PARTITION (country='USA') VALUES
(1, 'John Doe', 28),
(3, 'Bob Johnson', 45),
(7, 'Tom Brown', 33);

INSERT INTO external_users PARTITION (country='Canada') VALUES
(2, 'Jane Smith', 34);

INSERT INTO external_users PARTITION (country='UK') VALUES
(4, 'Alice White', 30);

INSERT INTO external_users PARTITION (country='Germany') VALUES
(5, 'Michael Green', 41);

INSERT INTO external_users PARTITION (country='France') VALUES
(6, 'Laura Black', 25);

CREATE EXTERNAL TABLE external_orders (
  order_id INT,
  user_id INT,
  order_amount DOUBLE
)
PARTITIONED BY (order_date STRING)
CLUSTERED BY (user_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/external/orders';

-- Вставка данных с указанием значений для разделов
INSERT INTO external_orders PARTITION (order_date='2023-08-01') VALUES
(101, 1, 250.75);

INSERT INTO external_orders PARTITION (order_date='2023-08-05') VALUES
(102, 2, 300.50);

INSERT INTO external_orders PARTITION (order_date='2023-08-10') VALUES
(103, 1, 125.00);

INSERT INTO external_orders PARTITION (order_date='2023-08-12') VALUES
(104, 3, 500.00);

INSERT INTO external_orders PARTITION (order_date='2023-08-15') VALUES
(105, 5, 175.99);

INSERT INTO external_orders PARTITION (order_date='2023-08-18') VALUES
(106, 6, 200.25);

INSERT INTO external_orders PARTITION (order_date='2023-08-20') VALUES
(107, 7, 320.89);

INSERT INTO external_orders PARTITION (order_date='2023-08-22') VALUES
(108, 4, 450.50);


SELECT order_id, user_id FROM orders;

SELECT /*+MAPJOIN(users)*/u.name, o.order_id, o.order_amount, o.order_date
FROM users u
JOIN orders o ON u.id = o.user_id;

SELECT u.name, COUNT(o.order_id) AS total_orders, SUM(o.order_amount) AS total_amount
FROM users u 
JOIN orders o ON u.id = o.user_id 
GROUP BY u.name
HAVING COUNT(o.order_id) > 1;

SELECT *
FROM orders 
WHERE order_date > '2023-08-10';

INSERT OVERWRITE DIRECTORY '/user/hive/ouput/orders_with_sum'
SELECT o.order_id, o.user_id, o.order_amount, SUM(order_amount) OVER (PARTITION BY o.user_id ORDER BY o.order_date) as order_sum
FROM orders o;


CREATE EXTERNAL TABLE external_users (
	id INT,
	name STRING,
	age INT
)
PARTITIONED BY (country STRING)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC
LOCATION '/user/hive/external/users';


INSERT INTO external_users PARTITION (country='USA') VALUES
(1, 'John Doe', 28),
(3, 'Bob Johnson', 45),
(7, 'Tom Brown', 33);

CREATE EXTERNAL TABLE external_orders (
 order_id INT,
 user_id INT,
 order_amount DOUBLE
)
PARTITIONED BY (order_date STRING)
CLUSTERED BY(user_id) INTO 4 BUCKETS
STORED AS PARQUET
LOCATION '/user/hive/external/orders';

INSERT OVERWRITE TABLE external_users 
SELECT id, name, age, country
FROM users;

SET hive.auto.convert.join = True;
INSERT OVERWRITE DIRECTORY 'user/hive/output/orders_with_users'
SELECT /*+ MAPJOIN(u) */ o.order_id, o.order_amount, u.name, o.order_date
FROM external_orders o
JOIN external_users u ON o.order_id = u.id; 

SELECT order_id, user_id, order_date, YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(order_date, 'yyyy-MM-dd'))) as order_year
FROM orders;
