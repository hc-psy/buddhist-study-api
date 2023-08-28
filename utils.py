import pandas as pd
import numpy as np


all_keys = set()


def str2dict(df: pd.DataFrame, column_name: str, is_global: bool = True):
    result = {}

    if is_global:
        global all_keys
    else:
        all_keys = set()

    # 先找出所有可能的鍵
    for entry in df[column_name]:
        entry = str(entry)
        if entry:
            pairs = entry.split(';')
            for pair in pairs:
                key, _ = pair.strip().replace("'", "").split(':')
                all_keys.add(key)

    # 再根據所有可能的鍵來建立結果
    for entry in df[column_name]:
        entry = str(entry)
        pairs = entry.split(';') if entry else []
        current_data = {}

        for pair in pairs:
            key, value = pair.strip().replace("'", "").split(':')
            current_data[key] = int(value)

        for key in all_keys:
            result.setdefault(key, []).append(current_data.get(key, 0))

    return result


def make_book_race(df_book: pd.DataFrame, input_list: pd.Series):
    # 將 DataFrame 轉換為字典以便快速查找
    book_id_to_topic_dict = df_book.set_index(
        'book_id')['original_topic'].to_dict()

    # 初始化最終的結果列表
    final_result_list = []

    # 初始化暫存字典
    temp_result_dict = {}

    # 迭代每一個列表元素
    for i, item in enumerate(input_list):
        pairs = item.split(';') if item else []

        # 初始化預設的0值列表
        default_zero_list = [0] * len(input_list)

        # 迭代每一個鍵值對
        for pair in pairs:
            book_id_str, value_str = pair.split(':')
            book_id = int(book_id_str)
            value = int(value_str)

            original_topic = book_id_to_topic_dict.get(book_id, "Unknown")

            if book_id not in temp_result_dict:
                temp_result_dict[book_id] = {
                    'name': original_topic, 'value': default_zero_list.copy()}

            temp_result_dict[book_id]['value'][i] = value

    # 轉換結果到最終的列表
    for key, value_dict in temp_result_dict.items():
        final_result_list.append(value_dict)

    return final_result_list
