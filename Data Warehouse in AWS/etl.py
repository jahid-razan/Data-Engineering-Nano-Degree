import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    This procedure loads the table from the s3 buckets to the Redshift cluster 
     INPUTS: 
    * cur the cursor variable
    * input: insert_copy_queries list that contains the twho staging tables where data will be copied
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    This procedure insert data into the Fact and the dimensions table
    input: insert_table_queries list where data will be inserted
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    This procedure reads the dwh.cfg file that contains the configuration data of the AWS warehouse and 
    
    input: 
    Host address: weblink, string
    database user name: string 
    Database password: string 
    Database port: integer 
     
    output: staging tables

    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()