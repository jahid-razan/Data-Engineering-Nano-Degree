
-- create customer table
CREATE TABLE "customer"
   (
       "customer_id" CHAR(10),
       "name" VARCHAR,
       "street" VARCHAR,
       "house_nr" INTEGER,
       "city" VARCHAR,
       "ZIP" CHAR(6),
       "phone" TEXT,
      CONSTRAINT customer_custid_pk PRIMARY KEY(customer_id)   
   );

-- insert some test data into the customer table
INSERT INTO customer VALUES('C000000001','Ramesh Sharma','XYZ','123','Eindhoven','5612SZ', '0684-678951');
INSERT INTO customer VALUES('C000000002','Rahul Rastogi','PQR','456','Amsterdam','1567PQ', '1234-678951');
INSERT INTO customer VALUES('C000000003','Parul Gandhi','STHVDF','432','Den Haag','8903TY', '8905-677890');
INSERT INTO customer VALUES('C000000004','Naveen Aedekar','AHNVR','567','Utrecht','5689WE', '0875-678951');
INSERT INTO customer VALUES('C000000005','Chitresh Barwe','FJEERFJ','866','Rotterdam','8045UT', '7890-678951');
INSERT INTO customer VALUES('C000000006','Amit Borkar','JERFJEJ','178','Gronigen','9090IU', '4567-678951');
INSERT INTO customer VALUES('C000000007','Nisha Damle','OHCCHR','156','Hoofddorp','8935HT', '4523-678951');
INSERT INTO customer VALUES('C000000008','Abhishek Dutta','LKQTOIZ','145','Tilburg','9467GK', '3890-678951');

INSERT INTO customer VALUES('C000000009','Rhahul Khamma','XYZ','123','Eindhoven','5612SZ', '0684-678951');
INSERT INTO customer VALUES('C000000010','Hr Kr','PQR','456','Amsterdam','1567PQ', '1234-678951');
INSERT INTO customer VALUES('C000000011','Parul Pande','STHVDF','432','Den Haag','8903TY', '8905-677890');
INSERT INTO customer VALUES('C000000012','Naveen Kumar','AHNVR','567','Utrecht','5689WE', '0875-678951');
INSERT INTO customer VALUES('C000000013','Cg Barwe','FJEERFJ','866','Rotterdam','8045UT', '7890-678951');
INSERT INTO customer VALUES('C000000014','AR Borkar','JERFJEJ','178','Gronigen','9090IU', '4567-678951');
INSERT INTO customer VALUES('C000000015','N Damle','OHCCHR','156','Hoofddorp','8935HT', '4523-678951');
INSERT INTO customer VALUES('C000000016','A Dutta','LKQTOIZ','145','Tilburg','9467GK', '3890-678951');




-- create account table 
CREATE TABLE "account" (
  "account_id" CHAR(18),
  "type" VARCHAR,
  "balance" NUMERIC,
  "customer_id" VARCHAR(10),
  CONSTRAINT account_id PRIMARY KEY(account_id),
  CONSTRAINT account_custid_fk FOREIGN KEY(customer_id) REFERENCES customer
  ON DELETE SET NULL
);



-- insert some test data into the account table
INSERT INTO account VALUES('NL98KNAB4871952010','Savings', 3455890.12789, 'C000000001');
INSERT INTO account VALUES('NL76KNAB1163935948','Savings', 122345.478, 'C000000002');
INSERT INTO account VALUES('NL45KNAB8434248247','Current', 424905.12789, 'C000000003');
INSERT INTO account VALUES('NL29KNAB3506581023','Savings', 6178934.478, 'C000000004');

INSERT INTO account VALUES('NL04KNAB2640990624','Current', 70890.89, 'C000000005');
INSERT INTO account VALUES('NL22KNAB7762494464','Savings', 69345.478, 'C000000006');
INSERT INTO account VALUES('NL23KNAB7537184356','Savings', 658930.356, 'C000000007');

INSERT INTO account VALUES('NL05KNAB2640990625','Current', 49890.89, 'C000000008');
INSERT INTO account VALUES('NL29KNAB7762494465','Savings', 482345.478, 'C000000009');
INSERT INTO account VALUES('NL333KNAB753718436','Fixed-term', 458930.356, 'C000000010');

INSERT INTO account VALUES('NL08KNAB2640990644','Current', 68690.89, 'C000000011');
INSERT INTO account VALUES('NL45KNAB7762494445','Savings', 62345.478, 'C000000012');
INSERT INTO account VALUES('NL53KNAB7537184367','Fixed-term', 568930.356, 'C000000013');

INSERT INTO account VALUES('NL05KNAB6787979519','Current', 8954.532, 'C000000014');
INSERT INTO account VALUES('NL06KNAB6787979523','Savings', 5709.532, 'C000000015');
INSERT INTO account VALUES('NL07KNAB6787979532','Fixed-term', 4389.532, 'C000000016');



-- create transaction table 
CREATE TABLE "transaction" (
  "trans_id" CHAR(12),
  "account_id" CHAR(18),
  "trans_date" TIMESTAMP,
  "trans_type" CHAR(1),
  "trans_amount" NUMERIC,
  CONSTRAINT id PRIMARY KEY(trans_id),
  CONSTRAINT transaction_accountid_fk FOREIGN KEY(account_id) REFERENCES account
  ON DELETE SET NULL
);
-- insert some test data into the transaction table 
INSERT INTO "transaction" VALUES('462614584116','NL98KNAB4871952010','2020-12-06 07:03:51','D', 5890.12);
INSERT INTO "transaction" VALUES('840195911048','NL76KNAB1163935948','2020-08-24 21:22:53','W', 345.478);
INSERT INTO "transaction" VALUES('134643622073','NL45KNAB8434248247','2020-10-24 00:34:17','D', 245345.12789);
INSERT INTO "transaction" VALUES('956424293568','NL29KNAB3506581023', '2020-09-11 09:08:56','W', 934.478);
INSERT INTO "transaction" VALUES('546522245784','NL45KNAB8434248247','2020-05-03 12:49:07','D', 90.89);
INSERT INTO "transaction" VALUES('412492111701','NL22KNAB7762494464','2020-08-22 19:31:37','D', 345.478);
INSERT INTO "transaction" VALUES('331202704354','NL23KNAB7537184356','2020-05-07 02:06:03','W', 134930.356);





-- create loan table
CREATE TABLE "loan" (
  "loan_id" CHAR(12),
  "account_id" CHAR(18),
  "year" CHAR(4),
  "loan_amount" NUMERIC,
  CONSTRAINT loan_id PRIMARY KEY(loan_id),
  CONSTRAINT loan_accountid_fk FOREIGN KEY(account_id) REFERENCES account
  ON DELETE SET NULL
);



-- insert some test data into the loan table 
INSERT INTO "loan" VALUES('E19Q6B660HFZ','NL98KNAB4871952010','2020', 6000.00);
INSERT INTO "loan" VALUES('4CVNNJ5R0ZP8','NL76KNAB1163935948','2020', 3000.12);
INSERT INTO "loan" VALUES('IAGXH3DRIEKQ','NL45KNAB8434248247','2020', 4000.32);
INSERT INTO "loan" VALUES('0N3KR20Q4P3B','NL23KNAB7537184356','2020', 5400.62);
INSERT INTO "loan" VALUES('4P1XRJH4YFDR','NL53KNAB7537184367','2020', 6000000.12);







--compliance view
CREATE VIEW current_balance  as(WITH t1 as (SELECT l.loan_id,
                   l.account_id,
            l.loan_amount
      		FROM loan l
            WHERE l.year= '2020')

SELECT c.name as client_name,
       a.customer_id as client_id,
       a.account_id,
       a.type as account_type,
       a.balance,
       t1.loan_amount,
       CASE WHEN t1.loan_amount>0 THEN a.balance - t1.loan_amount 
       ELSE a.balance END as current_deposit
       
FROM account a
LEFT JOIN t1
ON t1.account_id = a.account_id
JOIN customer c
ON a.customer_id = c.customer_id);


-- payable view 
CREATE VIEW payable as (
    SELECT client_name,
           account_type,
           current_deposit,
           
        CASE WHEN current_deposit<0 THEN 0
             WHEN current_deposit> 100000 THEN 100000
             ELSE current_deposit
             END as mendatory_payable
        
       
FROM current_balance
WHERE current_deposit >0);







-- Shows all the people ORDER BY mendatory payables, higher amount first
SELECT client_name, 
       account_type,
       current_deposit, mendatory_payable
FROM payable
ORDER BY 3 DESC ;

-- COUNT the total number of people that KNAB needs to pay
SELECT COUNT(*) as total_number
FROM payable;

-- COUNT the total amount of money that KNAB needs to pay as mendatory payables
SELECT SUM(mendatory_payable) as total_mendatory_payable
FROM payable


-- COUNT the total number of person with more than 100 k in the bank
SELECT count(*) as more_than_100k_payable
FROM payable
WHERE current_deposit >= 100000;


-- percentage of clients with more than 100k payable

    -- count the number of clients with 100 k payables
    WITH t1 as 
            (SELECT COUNT(*) as more_than_100k
            FROM payable
            WHERE mendatory_payable = 100000),

    -- count total number of clients
    t2 as (SELECT COUNT(*) as total_clients
            FROM payable)

    -- find the percentage of clients with 100 k payables
    SELECT t1.more_than_100k,
        t2.total_clients,
        round(t1.more_than_100k*100/t2.total_clients::numeric,2) as percentage_more_than_100k
    FROM t1, t2;
 
-- find the number of client per quartile

CREATE VIEW quartile as (
    
    WITH t1 as (
    
    SELECT client_name,
           current_deposit,
       CASE WHEN  mendatory_payable < 25000 THEN 'first_quartile (0-25k)'
           WHEN mendatory_payable >= 25000 AND mendatory_payable  < 50000 THEN
           'second_quartile (25-50 k)'
           WHEN mendatory_payable >= 50000 AND mendatory_payable <75000 THEN
          'third_quartile (50-100 k)'
          WHEN mendatory_payable >= 75000 THEN 'fourth_quartile (75-100 k)'
        END AS percentile  
     FROM payable)
     
SELECT percentile, COUNT(t1.percentile) as client_per_quartile
FROM t1
GROUP By 1);

-- percentage per quartile

SELECT percentile as category,
       client_per_quartile,
       round(client_per_quartile*100/SUM(client_per_quartile) OVER()::numeric,2) percentage
FROM quartile
GROUP BY 1,2
ORDER BY 2 DESC;




-- trigger to create the blance table after every transaction
CREATE OR REPLACE FUNCTION account_update() 
RETURNS TRIGGER AS $account$
    BEGIN
        
        DECLARE curr_balance NUMERIC;
        SELECT balance INTO curr_balance from account WHERE account_id=NEW.account_id;

       IF NEW.trans_type='W' THEN
            update account set balance = curr_balance - NEW.amount WHERE account_id=NEW.account_id;
        elseif NEW.trans_type='D' then
        UPDATE account SET balance = curr_balance + NEW.amount WHERE account_id=NEW.account_id;

    end if;
    END
$account$ LANGUAGE plpgsql;

CREATE TRIGGER account_balance_update
AFTER INSERT OR UPDATE OR DELETE ON transaction
FOR EACH ROW 
EXECUTE PROCEDURE account_update();