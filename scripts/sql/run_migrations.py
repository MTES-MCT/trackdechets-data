import argparse
import sys
import os
import time

def main():
    parser = argparse.ArgumentParser(description="Run a SQL file on a ClickHouse-compatible database with confirmation.")
    parser.add_argument("--file", required=True, help="Path to the SQL file to run.")
    parser.add_argument("--DB_HOST", required=True, help="Database host.")
    parser.add_argument("--DB_PORT", required=True, type=int, help="Database port.")
    parser.add_argument("--DB_USER", required=True, help="Database user.")
    parser.add_argument("--DB_PASSWORD", required=True, help="Database password.")

    args = parser.parse_args()

    # Read SQL file
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(current_dir, args.file), "r", encoding="utf-8") as f:
            sql = f.read()
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)


    try:
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=args.DB_HOST,
            port=args.DB_PORT,
            username=args.DB_USER,
            password=args.DB_PASSWORD,
            database="default",
            send_receive_timeout=300,
        )
    
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        sys.exit(2)

    import sqlparse

    statements = sqlparse.split(sql)
    for i, statement in enumerate(statements, 1):
        cleaned_statement = statement.strip()
        if not cleaned_statement:
            continue  # skip empty statements

        print(f"\nStatement {i}:\n{'-'*40}\n{cleaned_statement}\n{'-'*40}")
        confirm = input("Do you want to execute this statement? (y/N): ").strip().lower()
        if confirm != "y":
            print("Skipped.")
            continue

        try:
            client.command(cleaned_statement)
            print("Executed successfully.")
            time.sleep(1)
        except Exception as e:
            print(f"Error executing statement {i}: {e}")
            sys.exit(3)





if __name__ == "__main__":
    main()
