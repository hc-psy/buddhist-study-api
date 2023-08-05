from connector import connect_with_connector
from sqlalchemy.sql import text
from constants import CONTINENTS
from tqdm import tqdm
import pandas as pd
db = connect_with_connector()
conn = db.connect()


def use_conn(query: str, params=None):
    query_text = text(query)
    if params:
        records = conn.execute(query_text, params)
    else:
        records = conn.execute(query_text)
    return records


def make_list_dicts(column: list, records, batch_size=1000):
    records_dicts = []

    while True:
        rows = records.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            records_dicts.append(
                {column[i]: value for i, value in enumerate(row)})

    # row = records.fetchone()
    # while row is not None:
    #     records_dicts.append({column[i]: value for i, value in enumerate(row)})
    #     row = records.fetchone()

    return records_dicts


# old usage
# records = use_conn(get_geo_by_location_query, params) <--- this way should use :param_name
# column = list(records.keys())
# results = make_list_dicts(column, records)

def get_geo_by_location(continent: str, country: str, city: str):
    get_geo_by_location_query = """
        SELECT continent, country, city, lon, lat, lan, total_click, total_user, total_book
        FROM BY_LONLATLAN
        WHERE
            (%(continent)s = 'All Continents' OR continent = %(continent)s)
            AND (%(country)s = 'All Countries' OR country = %(country)s);
    """
    params = {
        "continent": continent,
        "country": country,
    }
    # directly read the query into a pandas DataFrame
    df = pd.read_sql_query(get_geo_by_location_query, db, params=params)

    if city == "All Cities":
        geo_metrics = df.drop(
            columns=['continent', 'country', 'city']).to_dict('records')
    else:
        geo_metrics = df[df['city'] == city].drop(
            columns=['continent', 'country', 'city']).to_dict('records')

    # Group by 'continent' and 'country', then get the sum of 'total_users' and 'total_clicks'
    if country != "All Countries":
        aggregated_data = df.groupby(
            ['city'])[['total_user', 'total_click', 'total_book']].sum().reset_index()
        label_list = aggregated_data['city'].tolist()
    else:
        aggregated_data = df.groupby(['country'])[
            ['total_user', 'total_click', 'total_book']].sum().reset_index()
        label_list = aggregated_data['country'].tolist()

    results = {
        "geo_metrics": geo_metrics,
        "pie_metrics": {
            "labels": label_list,
            "total_user": aggregated_data['total_user'].tolist(),
            "total_click": aggregated_data['total_click'].tolist(),
            "total_book": aggregated_data['total_book'].tolist(),
        }
    }

    return results
