import pandas as pd
from utils import str2dict, make_book_race
from constants import WEEKS


cache_weekly_id_geo = []


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
        assert len(group['visit_type1_counts'].tolist()) == len(WEEKS)
        assert len(group['visit_type2_counts'].tolist()) == len(WEEKS)
        assert len(group['visit_type3_counts'].tolist()) == len(WEEKS)
        assert len(group['visit_type4_counts'].tolist()) == len(WEEKS)

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
