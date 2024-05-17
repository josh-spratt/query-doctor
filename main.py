import redshift_connector
import os
import argparse
import re
import json

JOIN_REDISTRIBUTIONS = {
    "DS_DIST_NONE": {
        "description": "No redistribution is required, because corresponding slices are collocated on the compute nodes.",
        "details": "DS_DIST_NONE and DS_DIST_ALL_NONE are good. They indicate that no distribution was required for that step because all of the joins are collocated.",
    },
    "DS_DIST_ALL_NONE": {
        "description": "No redistribution is required, because the inner join table used DISTSTYLE ALL. The entire table is located on every node.",
        "details": "DS_DIST_NONE and DS_DIST_ALL_NONE are good. They indicate that no distribution was required for that step because all of the joins are collocated.",
    },
    "DS_DIST_INNER": {
        "description": "The inner table is redistributed.",
        "details": "DS_DIST_INNER means that the step probably has a relatively high cost because the inner table is being redistributed to the nodes. DS_DIST_INNER indicates that the outer table is already properly distributed on the join key. Set the inner table's distribution key to the join key to convert this to DS_DIST_NONE.",
    },
    "DS_DIST_OUTER": {
        "description": "The outer table is redistributed.",
        "details": "DS_DIST_OUTER means that the step probably has a relatively high cost because the outer table is being redistributed to the nodes. DS_DIST_OUTER indicates that the inner table is already properly distributed on the join key. Set the outer table's distribution key to the join key to convert this to DS_DIST_NONE.",
    },
    "DS_BCAST_INNER": {
        "description": "A copy of the entire inner table is broadcast to all the compute nodes.",
        "details": "DS_BCAST_INNER and DS_DIST_BOTH are not good. Usually these redistributions occur because the tables are not joined on their distribution keys. If the fact table does not already have a distribution key, specify the joining column as the distribution key for both tables. If the fact table already has a distribution key on another column, evaluate whether changing the distribution key to collocate this join improve overall performance. If changing the distribution key of the outer table isn't an optimal choice, you can achieve collocation by specifying DISTSTYLE ALL for the inner table.",
    },
    "DS_DIST_ALL_INNER": {
        "description": "The entire inner table is redistributed to a single slice because the outer table uses DISTSTYLE ALL.",
        "details": "DS_DIST_ALL_INNER is not good. It means that the entire inner table is redistributed to a single slice because the outer table uses DISTSTYLE ALL, so that a copy of the entire outer table is located on each node. This results in inefficient serial runtime of the join on a single node, instead taking advantage of parallel runtime using all of the nodes.",
    },
    "DS_DIST_BOTH": {
        "description": "Both tables are redistributed.",
        "details": "DS_BCAST_INNER and DS_DIST_BOTH are not good. Usually these redistributions occur because the tables are not joined on their distribution keys. If the fact table does not already have a distribution key, specify the joining column as the distribution key for both tables. If the fact table already has a distribution key on another column, evaluate whether changing the distribution key to collocate this join improve overall performance. If changing the distribution key of the outer table isn't an optimal choice, you can achieve collocation by specifying DISTSTYLE ALL for the inner table.",
    },
}


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
    aggregate_plan = ""
    for row in query_result:
        aggregate_plan += row[0].strip()
    records = [x.strip() for x in aggregate_plan.split("->")]

    join_count = 0
    for record in records:
        join_match = re.search(regex_join_pattern, record)
        if join_match:
            join_count += 1
            print("+" + "=" * 50 + f" Join {join_count} " + "=" * 50 + "+")
            if not join_match.group(2):
                join_type = "Inner"
                print(f" > Join {join_count} is an {join_type} join.")
            else:
                join_type = join_match.group(2)
                print(f" > Join {join_count} is a {join_type} join.")
            join_redistribution = join_match.group(3)

            print(f" > {JOIN_REDISTRIBUTIONS[join_redistribution]['description']}")
            print(f" > {JOIN_REDISTRIBUTIONS[join_redistribution]['details']}")


if __name__ == "__main__":
    main()
