import sys
import inflection
import re
from pathlib import Path
import os
import glob
import mysql.connector as mariadb
import time
import datetime
import pandas as pd
import shapefile
from dotenv import find_dotenv, load_dotenv
from io import StringIO


def get_project_root() -> Path:
    return Path(__file__).parent.parent


PATH_TO_PROJECT_ROOT = get_project_root()
PATH_INTERIM = os.path.join(PATH_TO_PROJECT_ROOT, "data", "interim")
PATH_PROCESSED = os.path.join(PATH_TO_PROJECT_ROOT, "data", "processed")
PATH_RAW = os.path.join(PATH_TO_PROJECT_ROOT, "data", "raw")
PATH_INTERIM_RUNNING = os.path.join(PATH_INTERIM, "running")
xwalk_ssc_desc_4_rr_grp_netgrp = StringIO(
    """scc_description_level_4,rr_group,rr_netgrp
    Line Haul Locomotives: Class I Operations,Class I,Freight
    Line Haul Locomotives: Class II / III Operations,Class III,Freight
    Line Haul Locomotives: Passenger Trains (Amtrak),Passenger,Freight
    Line Haul Locomotives: Commuter Lines,Commuter,Freight
    Yard Locomotives,Class I,Industrial
    Yard Locomotives,Commuter,Industrial
    Yard Locomotives,Class III,Industrial
    Yard Locomotives,Commuter,Yard
    Yard Locomotives,Class I,Yard
    Yard Locomotives,Class III,Yard
"""
)


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


def get_out_file_tsmp():
    """Get the date timestamp for file output."""
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
    return st


def cleanup_prev_output(dir_filenm_pat: os.path) -> None:
    """
    Delete all files with pattern `filenm_pat` from directory `dir`
    """
    for file in glob.glob(dir_filenm_pat):
        os.remove(file)
        print(f"Deleting file: {file}")


def read_shapefile(path_shp_file):
    """
    Read a shapefile into a Pandas dataframe with a 'coords'
    column holding the geometry information. This uses the pyshp
    package
    """
    sf = shapefile.Reader(path_shp_file, encoding="Shift-JIS")
    fields = [x[0] for x in sf.fields][1:]
    records = [y[:] for y in sf.records()]
    # shps = [s.points for s in sf.shapes()]
    df = pd.DataFrame(columns=fields, data=records)
    # df = df.assign(coords=shps)
    return df


def get_snake_case_dict(columns):
    """ Get columns in snake_case."""
    return {col: re.sub(r"\W+", "_", inflection.underscore(col)) for col in columns}


if __name__ == "__main__":
    path_natrail2020 = os.path.join(
        PATH_RAW, "North_American_Rail_Lines", "North_American_Rail_Lines.shp"
    )
