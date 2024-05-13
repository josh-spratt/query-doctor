import redshift_connector
import os
import argparse


def explain_query(conn: redshift_connector.Connection, query: str):
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute("explain\n" + query)
    result: tuple = cursor.fetchall()
    for row in result:
        print(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query")
    args = parser.parse_args()
    env = os.environ
    query = args.query

    # Connects to Redshift cluster using AWS credentials
    conn = redshift_connector.connect(
        host=env["HOST"],
        database=env["DATABASE"],
        user=env["USER"],
        password=env["PASSWORD"],
    )
    explain_query(conn, query)


if __name__ == "__main__":
    main()
