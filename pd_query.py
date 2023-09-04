import pandas as pd
import numpy as np
from processor import (get_geo_by_location_processor,
                       get_weekly_by_location_processor,
                       get_weekly_id_geo_processor,
                       get_search_processor,
                       get_network_processor,
                       get_arcs_points_processor,
                       get_arcs_arcs_processor)

# cache pd dataframes

df_pattern = pd.read_csv('data/pattern.csv')
df_weekly = pd.read_csv('data/weekly_trend.csv',
                        na_values=[], keep_default_na=False)
df_book = pd.read_csv('data/book.csv', na_values=[], keep_default_na=False)
df_topic_map = pd.read_csv('data/topic_map.csv')
df_user_map = pd.read_csv('data/lat_lon_map.csv')

# cache np arrays

H_n = np.load('data/book_NMF_288430x10.npy')
H_t = np.load('data/book_Transformer_288430x10.npy').astype('float64')

H_u = np.load('data/user_NMF_10406x10.npy')


def get_geo_by_location(continent: str, country: str, city: str):

    condition_continent = (continent == 'All Continents') | (
        df_pattern['continent'] == continent)
    condition_country = (country == 'All Countries') | (
        df_pattern['country'] == country)

    filtered_pattern_df = df_pattern[condition_continent & condition_country]

    filtered_pattern_df.drop(columns=['continent'], inplace=True)

    return get_geo_by_location_processor(filtered_pattern_df, country, city)


def get_weekly_by_location(continent: str, country: str):
    # 有特定國家，所以兄弟是同一大洲的國家
    if country != "All Countries":
        # 兄弟 是 同一大洲的國家 且 不是特定國家 且 不是所有國家 （不是該州資料）
        df_sib = df_weekly[(df_weekly['continent'] == continent) &
                           (df_weekly['country'] != 'All Countries') &
                           (df_weekly['country'] != country)]
        df_this = df_weekly[(df_weekly['continent'] == continent) &
                            (df_weekly['country'] == country)]

        return get_weekly_by_location_processor(df_sib, df_this, 'country', df_book)

    # 沒有特定國家，是某一大洲，所以兄弟是其他大洲
    # 是全世界，所以兄弟是大洲
    else:
        if continent != "All Continents":
            df_sib = df_weekly[(df_weekly['continent'] != continent) &
                               (df_weekly['country'] == 'All Countries') &
                               (df_weekly['continent'] != 'All Continents')]

            df_this = df_weekly[(df_weekly['continent'] == continent) &
                                (df_weekly['country'] == 'All Countries')]

        else:
            df_sib = df_weekly[(df_weekly['continent'] != 'All Continents') &
                               (df_weekly['country'] == 'All Countries')]

            df_this = df_weekly[df_weekly['continent'] == 'All Continents']

        return get_weekly_by_location_processor(df_sib, df_this, 'continent', df_book)


def get_weekly_id_geo():
    return get_weekly_id_geo_processor(df_weekly)


def get_search(query: str):
    query_stripped = query.strip()
    return get_search_processor(df_topic_map, query_stripped)


def get_network(query: list, method: str):
    query_stripped = [q.strip() for q in query]

    if method == 'NMF':
        return get_network_processor(df_topic_map, H_n, query_stripped)
    elif method == 'Transformer':
        return get_network_processor(df_topic_map, H_t, query_stripped)


def get_arcs_points():
    return get_arcs_points_processor(df_user_map)


def get_arcs_arcs(lat_lon_name: str):
    return get_arcs_arcs_processor(df_user_map, lat_lon_name, H_u)
