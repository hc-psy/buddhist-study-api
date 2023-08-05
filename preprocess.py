from connector import connect_with_connector
from sqlalchemy.sql import text
from constants import CONTINENTS
from tqdm import tqdm

db = connect_with_connector()
conn = db.connect()


def use_conn(query: str, params=None):
    query_text = text(query)
    if params:
        records = conn.execute(query_text, params)
    else:
        records = conn.execute(query_text)
    return records


def make_list_dicts(column: list, records):
    records_dicts = []
    row = records.fetchone()
    while row is not None:
        records_dicts.append({column[i]: value for i, value in enumerate(row)})
        row = records.fetchone()
    return records_dicts


def make_list_tuples(records):
    records_tuples = []
    row = records.fetchone()
    while row is not None:
        records_tuples.append(tuple(row))
        row = records.fetchone()
    return records_tuples


def create_BY_LONLATLAN_table():

    print("Checking if BY_LONLATLAN table exists...")
    check_table_query = """
    SHOW TABLES LIKE 'BY_LONLATLAN';
    """
    table_exists = use_conn(check_table_query)

    if not table_exists.scalar():  # If table doesn't exist, create it.
        print("Creating BY_LONLATLAN table ...")
        create_table_query = """
        CREATE TABLE BY_LONLATLAN (
            continent VARCHAR(100),
            country VARCHAR(100),
            city VARCHAR(100),
            lon FLOAT,
            lat FLOAT,
            lan VARCHAR(50),
            total_click INT,
            total_user INT,
            total_book INT
        );
        """
        use_conn(create_table_query)
    else:
        print("BY_LONLATLAN table already exists. Continuing...")

    print("Fetching data from FULL_DATA table ...")
    for continent in CONTINENTS:
        print(f"1. {continent} is processing and fetching ...")
        query_literal = f"""
            SELECT 
                '{continent}' AS continent,
                country,
                city,
                lon,
                lat,
                lan,
                COUNT(*) AS total_click,
                COUNT(DISTINCT ip) AS total_user,
                COUNT(DISTINCT book_id) AS total_book
            FROM 
                FULL_DATA
            WHERE
                continent = '{continent}'
            GROUP BY 
                country, city, lon, lat, lan;
        """

        records = use_conn(query_literal)
        records_tuples = make_list_tuples(records)
        results = records_tuples

        print(f"2. {continent} is processing and inserting ...")

        insert_query = """
        INSERT INTO BY_LONLATLAN (continent, country, city, lon, lat, lan, total_click, total_user, total_book)
        VALUES (:continent, :country, :city, :lon, :lat, :lan, :total_click, :total_user, :total_book)
        """

        # Prepare the list of dictionaries to be inserted
        data_to_insert = [
            {
                "continent": result[0],
                "country": result[1],
                "city": result[2],
                "lon": result[3],
                "lat": result[4],
                "lan": result[5],
                "total_click": result[6],
                "total_user": result[7],
                "total_book": result[8]
            }
            for result in results
        ]

        conn.commit()
        with conn.begin():  # Begin a transaction
            # Insert data within transaction
            conn.execute(text(insert_query), data_to_insert)

        print(f"3. {continent} is processed and inserted ...")


conn.close()
