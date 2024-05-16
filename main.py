import redshift_connector
import os
import argparse
import re
import json


def explain_query(conn: redshift_connector.Connection, query: str):
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute("explain\n" + query)
    result: tuple = cursor.fetchall()
    return result


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
    query_result = explain_query(conn, query)

    regex_join_pattern = r"(XN Hash (Left|Right|))? Join (DS_DIST_ALL_NONE|DS_DIST_NONE|DS_BCAST_INNER|DS_BCAST_OUTER|DS_DIST_BOTH|DS_DIST_INNER|DS_DIST_OUTER|DS_DIST_ALL_INNER|DS_DIST_ALL_OUTER)"
    for row in query_result:
        print(row[0])
        match = re.search(regex_join_pattern, row[0])
        if match:
            # join_type, redistribution_type = match.groups()
            print(match.groups())
            # join_type will be either 'Left', 'Right', or None
            # redistribution_type will always be 'DS_DIST_ALL_NONE'
            # print(f"Line: {row[0]}")
            # print(f"  Join Type: {join_type}")
            # print(f"  Redistribution Type: {redistribution_type}")
        else:
            print(f"Line: {row[0]} - No Match")
        print("=" * 80)


if __name__ == "__main__":
    main()
