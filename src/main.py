import redshift_connector
import os
import argparse
from execution_plan_node import ExecutionPlanNode


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query")
    parser.add_argument("-f", "--file-mode")
    return parser.parse_args()


def connect_to_redshift() -> redshift_connector.Connection:
    """Connects to Redshift cluster using AWS credentials"""
    env = os.environ
    return redshift_connector.connect(
        host=env["REDSHIFT_HOST"],
        database=env["REDSHIFT_DATABASE"],
        user=env["REDSHIFT_USERNAME"],
        password=env["REDSHIFT_PASSWORD"],
    )


def explain_query(conn: redshift_connector.Connection, query: str) -> tuple[list]:
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute("explain\n" + query)
    result: tuple = cursor.fetchall()
    return result


def parse_result(query_result: tuple[list]) -> ExecutionPlanNode:
    query_result[0][0] = "-> " + query_result[0][0]
    root = ExecutionPlanNode("Query Plan", -1)
    stack = [root]
    for row in query_result:
        if row[0].lstrip()[0:2] == "->":
            leading_whitespace = len(row[0]) - len(row[0].lstrip())
            execution_step = row[0].lstrip().replace("->", "").lstrip().split("  ")[0]
            current_node = ExecutionPlanNode(execution_step, leading_whitespace)

            while stack and stack[-1].depth >= leading_whitespace:
                stack.pop()

            stack[-1].add_child(current_node)

            stack.append(current_node)
    return root


def main():
    args = parse_arguments()
    if args.file_mode:
        with open(args.file_mode, "r") as f:
            explain_plan_lines = tuple([x] for x in f.readlines())
            query_tree = parse_result(explain_plan_lines)
            query_tree.print_tree()
    elif args.query:
        conn = connect_to_redshift()
        query_result = explain_query(conn, args.query)
        query_tree = parse_result(query_result)
        query_tree.print_tree()


if __name__ == "__main__":
    main()
