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
        print(entry)
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
