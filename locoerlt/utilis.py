import sys
from pathlib import Path
import os
import mariadb
from dotenv import find_dotenv, load_dotenv


def get_project_root() -> Path:
    return Path(__file__).parent.parent


PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_PROCESSED = os.path.join(PATH_TO_PROJECT_ROOT, "data", "processed")
PATH_RAW = os.path.join(PATH_TO_PROJECT_ROOT, "data", "raw")
PATH_INTERIM_RUNNING = os.path.join(PATH_INTERIM, "running")


def connect_to_server_db(database_nm, user_nm="root"):
    """
    Function to connect to a particular database on the server.
    Returns
    -------
    conn_: mariadb.connection
        Connection object to access the data in MariaDB Server.
    """
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Connect to MariaDB Platform
    try:
        conn_ = mariadb.connect(
            user=user_nm,
            password=os.environ.get("MARIA_DB_PASSWORD"),
            host="127.0.0.1",
            port=3306,
            database=database_nm,
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)
    return conn_


if __name__ == "__main__":
    ...
