import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop the staging, fact, and dimension tables if they exist."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create the staging, fact, and dimension tables."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
   
    
    drop_tables(cur, conn)
    print("Drop tables run successfully")
    create_tables(cur, conn)
    print("Create tables run successfully")

    conn.close()


if __name__ == "__main__":
    main()