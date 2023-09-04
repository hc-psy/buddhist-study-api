import pandas as pd
import numpy as np
from utils import str2dict, make_book_race
from constants import WEEKS
from fuzzywuzzy import fuzz

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

cache_weekly_id_geo = []


scaler = MinMaxScaler(feature_range=(0.1, 1))


def get_geo_by_location_processor(df: pd.DataFrame, country: str, city: str):
    if city == "All Cities":
        geo_metrics = df.drop(
            columns=['country', 'city']).to_dict('records')
    else:
        geo_metrics = df[df['city'] == city].drop(
            columns=['country', 'city']).to_dict('records')

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


def get_weekly_by_location_processor(df_sib: pd.DataFrame, df_this: pd.DataFrame, group_by: str, df_book: pd.DataFrame):

    df_this = df_this.copy()
    this_lan = str2dict(df_this, 'language_weekly_visits')
    this_book_race = make_book_race(df_book, df_this['top_10_books_weekly'])

    sib_data = []
    for name, group in df_sib.groupby(group_by):

        sib_data.append({
            "place": group[group_by].iloc[0],
            "v1": group['visit_type1_counts'].tolist(),
            "v2": group['visit_type2_counts'].tolist(),
            "v3": group['visit_type3_counts'].tolist(),
            "v4": group['visit_type4_counts'].tolist(),
        })

    df_this['week'] = df_this['week'].apply(lambda x: x.split('/')[0][-5:])

    results = {
        "week": df_this['week'].tolist(),  # week or week_id
        "place": df_this[group_by].tolist()[0],
        "total_users": df_this['unique_ips_weekly'].tolist(),
        "v1": df_this['visit_type1_counts'].tolist(),
        "v2": df_this['visit_type2_counts'].tolist(),
        "v3": df_this['visit_type3_counts'].tolist(),
        "v4": df_this['visit_type4_counts'].tolist(),
        "lan": this_lan,
        "book": this_book_race,
        "sib": sib_data,
    }

    return results


def get_weekly_id_geo_processor(df: pd.DataFrame):

    if len(cache_weekly_id_geo) == 0:
        for i, w in enumerate(WEEKS):
            points_strings_list = df[(df['week'] == w) & (
                df['country'] != "All Countries")]['weekly_most_common_points'].tolist()
            for ps in points_strings_list:
                if ps:
                    points = ps.split(';')
                    for p in points:
                        p = p.split(':')
                        g = p[0].split('_')

                        count = int(p[1])
                        lat = float(g[0])
                        lon = float(g[1])

                        cache_weekly_id_geo.append({
                            "lat": lat,
                            "lon": lon,
                            "count": count,
                            "time": i,
                        })

    weeks = [w.split('/')[0][-5:] for w in WEEKS]

    return {
        'weeks': weeks,
        'data': cache_weekly_id_geo
    }


def get_search_processor(df: pd.DataFrame, query: str):
    df = df.rename(columns={'Key': 'original_topic'}, inplace=False)

    # Compute fuzz ratio for each original_topic
    df['fuzz_ratio'] = df['original_topic'].apply(
        lambda x: fuzz.ratio(x, query)
    )

    # Filter those with fuzz_ratio > 50 and sort by fuzz_ratio descending
    filtered_df = df[df['fuzz_ratio'] > 50].sort_values(
        by='fuzz_ratio', ascending=False
    )

    # Take the top 5
    result = filtered_df.head(
        10)[['original_topic']].to_dict('records')

    return result


def get_top_20(i, H):

    top_20_similarities = np.zeros((1, 20))  # 用於保存每行的最高20個相似度
    top_20_indices = np.zeros((1, 20), dtype=int)  # 用於保存最高20個相似度對應的行索引
    similarity = cosine_similarity([H[i]], H)[0]  # 計算第i行與所有行的相似度
    # 取最高的20個相似度（不包括自己，因此是-21:-1）
    top_20 = np.argsort(similarity)[-21:-1][::-1]
    top_20_similarities = similarity[top_20]
    top_20_indices = top_20

    return top_20_similarities, top_20_indices


def find_books(input_data_list, df):
    results = []
    if all(isinstance(item, int) for item in input_data_list):  # 如果所有輸入都是整數（即索引）
        for input_data in input_data_list:
            if input_data in df.index:
                results.append(df.loc[input_data, 'Key'])
    elif all(isinstance(item, str) for item in input_data_list):  # 如果所有輸入都是字符串（即書名）
        for input_data in input_data_list:
            rows = df[df['Key'] == input_data]
            if not rows.empty:
                results.append(rows.index[0])
    else:
        return None

    return results


def generate_network_data(df, H, book_name, category=1):

    nodes = []
    links = []
    book_index = find_books([book_name], df)
    if book_index is None or len(book_index) == 0:
        return ""

    top_20_similarities, top_20_indices = get_top_20(book_index[0], H)
    normalized_top_20_sim = scaler.fit_transform(
        top_20_similarities.reshape(-1, 1)).flatten()

    nodes.append({
        "name": book_name,
        "category": category
    })

    for k, j in enumerate(top_20_indices):
        nodes.append({
            "name": df.loc[j, 'Key'],
            "category": category
        })

        links.append({
            "source": book_name,
            "target": df.loc[j, 'Key'],
            "value": normalized_top_20_sim[k]
        })

    network_data = {
        "nodes": nodes,
        "links": links
    }
    return network_data


def integrate_network_data(net1, net2):
    # 整合节点
    nodes = net1['nodes'].copy()
    node_names = [node['name'] for node in nodes]
    for node in net2['nodes']:
        if node['name'] not in node_names:
            nodes.append(node)
            node_names.append(node['name'])

    # 整合链接并重新标准化
    links = net1['links'].copy()
    link_pairs = [tuple(sorted([link['source'], link['target']]))
                  for link in links]

    for link in net2['links']:
        pair = tuple(sorted([link['source'], link['target']]))
        if pair not in link_pairs:
            links.append(link)
            link_pairs.append(pair)

    # 对所有链接的值进行标准化
    values = [link['value'] for link in links]
    normalized_values = scaler.fit_transform(
        np.array(values).reshape(-1, 1)).flatten()

    for i, link in enumerate(links):
        link['value'] = normalized_values[i]

    return {
        'nodes': nodes,
        'links': links
    }


def get_network_processor(df, H, book_name: list):

    for i, name in enumerate(book_name):
        if i == 0:
            net = generate_network_data(df, H, name, i)
        else:
            net = integrate_network_data(
                net, generate_network_data(df, H, name, i))

    return net


def get_arcs_points_processor(df: pd.DataFrame):
    result_list = []
    for index, row in df.iterrows():
        coords = row['Key'].split('_')
        if len(coords) == 2:
            latitude, longitude = map(float, coords)
            result_dict = {
                'name': row['Key'],
                'lat': latitude,
                'lon': longitude
            }
            result_list.append(result_dict)
    return result_list


def get_arcs_arcs_processor(df: pd.DataFrame, lat_lon: str, H: np.ndarray):
    if not lat_lon:
        return []

    query_id = df[df['Key'] == lat_lon].index[0]
    top_20_similarities, top_20_indices = get_top_20(query_id, H)
    normalized_top_20_sim = scaler.fit_transform(
        top_20_similarities.reshape(-1, 1)).flatten()

    from_lat, from_lon = map(float, lat_lon.split('_'))
    from_data = {
        "name": lat_lon,
        "coordinates": [from_lon, from_lat]
    }

    coordinates = []
    for k, i in enumerate(top_20_indices):
        if i in df.index:
            name = df.loc[i, 'Key']
            coords = name.split('_')
            lat, lon = map(float, coords)
            coordinates.append({
                "from": from_data,
                "to": {
                    "name": name,
                    "coordinates": [lon, lat]
                },
                "value": normalized_top_20_sim[k]
            })

    return coordinates
