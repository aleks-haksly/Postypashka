
INSERT INTO TABLE transactions PARTITION (transaction_date) 
VALUES 
(1, 101, '2024-01-15', 200.50), 
(2, 102, '2024-02-20', 150.75), 
(3, 103, '2024-03-12', 300.00),
(4, 104, '2024-01-22', 400.10),
(5, 101, '2024-02-05', 250.00);

INSERT INTO TABLE customers PARTITION (country) 
VALUES 
(101, 'John Doe', 45, 'USA'), 
(102, 'Jane Smith', 52, 'Canada'), 
(103, 'Michael Johnson', 34, 'USA'),
(104, 'Emily Davis', 60, 'UK'),
(105, 'Alex Green', 29, 'Canada');

INSERT INTO TABLE external_transactions PARTITION (transaction_date)
VALUES 
(6, 105, '2024-01-11', 100.25),
(7, 106, '2024-02-15', 220.00),
(8, 101, '2024-03-05', 310.45);

/*Задачи:
Создание таблиц с разбиением на разделы и кластеризацией:

Создайте две таблицы: transactions и customers.
Таблица transactions должна содержать следующие поля: transaction_id (INT), customer_id (INT), transaction_date (STRING), amount (DOUBLE).
Таблица customers должна содержать поля: customer_id (INT), name (STRING), age (INT), country (STRING).
Разбейте таблицу transactions на разделы по столбцу transaction_date и кластеризуйте по customer_id (3 бакета).
Разбейте таблицу customers на разделы по столбцу country и кластеризуйте по customer_id (3 бакета).
Работа с внешней таблицей:

Создайте внешнюю таблицу external_transactions, указывая местоположение в HDFS (или локальной файловой системе), которое ссылается на ваши данные о транзакциях.
Вставьте несколько строк данных в созданную внешнюю таблицу.
Использование виртуальных полей:

Напишите запрос, который добавляет виртуальное поле tax в таблицу transactions, где налог составляет 15% от суммы транзакции.
Напишите запрос, который выводит список всех клиентов, добавляя виртуальное поле customer_status, которое принимает значение VIP, если клиенту более 50 лет, иначе — Regular.
Map Join и Insert Overwrite:

Напишите запрос для объединения таблиц transactions и customers, используя Map Join. Получите список транзакций вместе с именами клиентов.
Результат объединения вставьте в новую таблицу transactions_with_customers с помощью INSERT OVERWRITE.
Работа с агрегацией и виртуальными полями:

Напишите запрос, который вычисляет общую сумму транзакций за каждый месяц, используя виртуальные поля для извлечения года и месяца из transaction_date.
Дополнительное задание (по желанию):
Создайте запрос, который использует встроенные виртуальные поля INPUT__FILE__NAME и выводит имя файла для каждой транзакции в таблице external_transactions.*/


ALTER TABLE $tablename SET TBLPROPERTIES('EXTERNAL'='False');
DROP TABLE $tablename


net stop winnat
docker start container_name
net start winnat



CREATE TABLE transactions (
  transaction_id INT,
  customer_id INT,
  amount DOUBLE
)
PARTITIONED BY (transaction_date STRING)
CLUSTERED BY (customer_id) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO TABLE transactions PARTITION (transaction_date) 
VALUES 
(1, 101, 200.50, '2024-01-15'), 
(2, 102, 150.75, '2024-02-20'), 
(3, 103, 300.00, '2024-03-12'),
(4, 104, 400.10, '2024-01-22'),
(5, 101, 250.00, '2024-02-05');


CREATE TABLE customers (
  customer_id INT,
  name STRING,
  age INT
)
PARTITIONED BY (country STRING)
CLUSTERED BY (customer_id) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO TABLE customers PARTITION (country) 
VALUES 
(101, 'John Doe', 45, 'USA'), 
(102, 'Jane Smith', 52, 'Canada'), 
(103, 'Michael Johnson', 34, 'USA'),
(104, 'Emily Davis', 60, 'UK'),
(105, 'Alex Green', 29, 'Canada');


DESCRIBE FORMATTED transactions;

CREATE EXTERNAL TABLE external_transactions (
  transaction_id INT,
  customer_id INT,
  amount DOUBLE
)
PARTITIONED BY (transaction_date STRING)
CLUSTERED BY (customer_id) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/external/transactions';

INSERT INTO TABLE external_transactions PARTITION (transaction_date)
VALUES 
(6, 105,  100.25, '2024-01-11'),
(7, 106,  220.00, '2024-02-15'),
(8, 101,  310.45, '2024-03-05');


SELECT t.transaction_id, t.customer_id, t.amount, t.transaction_date, t.amount * 0.15 as tax FROM  transactions t ;



SELECT *, (CASE WHEN c.age > 50 THEN 'VIP' ELSE 'Regular' END) as customer_status FROM customers c 