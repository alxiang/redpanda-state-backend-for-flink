import argparse
import numpy as np
import psycopg2
import time
from datetime import datetime


def timestamp(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0

def do_application_logic(target, records):
    max_ind = 0
    freshness = []
    for row in records:
        vec_str, ind, ts = row[0], row[1], row[2]
        freshness.append(time.time() * 1000 - timestamp(ts))
        max_ind = max(max_ind, ind)

        vec = np.array(vec_str.split(","))
        # Do some computation for realistic measuremtn
        dist = np.linalg.norm(target - vec) 
        

    avg_freshness = np.mean(freshness)
    print(f"[DATA_FRESHNESS]: {avg_freshness}")

    return max_ind


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('num_vectors', type=int, default=1_000_000, nargs='?')  # number of vectors to produce
    parser.add_argument('vector_size', type=int, default=64, nargs='?')  # vector size
    args = parser.parse_args()

    target = np.random.rand(args.vector_size)
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            user="admin",
            password="quest",
            host="127.0.0.1",
            port="8812",
            database="qdb"
        )
        cursor = connection.cursor()

        # Keep querying from vectortable until max_index == num_vectors-1    
        max_index = 0
        while max_index < args.num_vectors-1:
            postgreSQL_select_Query = 'SELECT * FROM vectortable'
            cursor.execute(postgreSQL_select_Query)
            print('Selecting recent rows from test table using cursor.fetchall')
            records = cursor.fetchall()
            max_index = do_application_logic(target, records)

            time.sleep(0.1)

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL:", error)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("PostgreSQL connection is closed")


if __name__ == "__main__":
    main()