from scipy.spatial import KDTree
import pandas as pd
import xarray as xr
import os
import numpy as np
import sys
import time

# 查询最近点
def find_nearest(lon, lat):
    dist, idx = kdtree.query([(lon, lat)])
    return idx[0]


df = pd.read_csv(r"E:\pCO2\Coastal\USwest\result\Coastal_USwest_unique_withfilename.csv")
# df2 = pd.read_csv(r"E:\pCO2\SouthernOceans\result\unique_filenames.csv")
l2a_filelist_unique = list(set(list(df['filelist'])))

# 将 l2a_filelist_unique 存储到 CSV 文件中
# unique_df = pd.DataFrame(l2a_filelist_unique, columns=['l2a_filename_unique'])
# unique_df.to_csv(r"E:\pCO2\SouthernOceans\result\unique_filenames.csv", index=False)

path_server = 'Z:/172_18_31_54/'

# 定义输出CSV文件路径
output_csv = r"E:\pCO2\Coastal\USwest\result\result_match_with_nasal2.csv"

# 创建一个空的 DataFrame
df_result = pd.DataFrame(columns=[
    'point_id', 'Longitude', 'Latitude',
    'Rrs_412', 'Rrs_443', 'Rrs_469', 'Rrs_488', 'Rrs_531',
    'Rrs_547', 'Rrs_555', 'Rrs_645', 'Rrs_667', 'Rrs_678',
    'Chlor_a', 'L2_Flags', 'Matched_Longitude', 'Matched_Latitude'
])
df_result.to_csv(output_csv, index=False)  # 创建文件并写入表头

# 初始化一个空的DataFrame用于缓存数据
buffer_df = pd.DataFrame(columns=df_result.columns)

# 定义输出文件路径
error_file = 'error.log'

geophysical_path = 'geophysical_data'
location_path = 'navigation_data'
# 逐个打开匹配到的l2a文件
total_num = len(l2a_filelist_unique)
num = 0
for filename in l2a_filelist_unique:
    num = num + 1
    try:
        satellite_filename = filename.replace("L1A_LAC", "L2A_LAC_ZD_-3")
    except Exception as e:
        continue
    print(filename)
    year_str = filename[11:15]
    path_satellite = rf"\\172.18.31.59\linux59\data\pCO2\USwest\result\{satellite_filename}"
    if os.path.exists(path_satellite):
        # 找出所有与该文件名相同的行
        matching_rows = df[df["filelist"] == filename]
        print("该l2a文件匹配到的点位共有： ", matching_rows.shape[0])
        # 判断该文件能否正常打开
        try:
            # 尝试打开文件
            xr.open_dataset(path_satellite)
            # 打开所需要提取的数据集
            geophysical_data = xr.open_dataset(path_satellite, group = geophysical_path)
            Rrs_412 = geophysical_data['Rrs_412'].values.flatten()
            Rrs_443 = geophysical_data['Rrs_443'].values.flatten()
            Rrs_469 = geophysical_data['Rrs_469'].values.flatten()
            Rrs_488 = geophysical_data['Rrs_488'].values.flatten()
            Rrs_531 = geophysical_data['Rrs_531'].values.flatten()
            Rrs_547 = geophysical_data['Rrs_547'].values.flatten()
            Rrs_555 = geophysical_data['Rrs_555'].values.flatten()
            Rrs_645 = geophysical_data['Rrs_645'].values.flatten()
            Rrs_667 = geophysical_data['Rrs_667'].values.flatten()
            Rrs_678 = geophysical_data['Rrs_678'].values.flatten()
            # chlor_a = geophysical_data['chlor_a'].values.flatten()
            l2_flags = geophysical_data['l2_flags'].values.flatten()
            # 打开位置信息
            location_data = xr.open_dataset(path_satellite, group = location_path)
            lon_array = location_data['longitude'].values.flatten()
            lat_array = location_data['latitude'].values.flatten()

            lon_lat_points = np.column_stack((lon_array, lat_array))
            kdtree = KDTree(lon_lat_points)
            # 开始对每个点位进行Rrs提取
            for index, row in matching_rows.iterrows():
                # 获取当前行的特定列信息
                longitude = row['lon']
                latitude = row['lat']
                point_id = row['point_id']
                nearest_idc = find_nearest(longitude, latitude)
                rrs412 = Rrs_412[nearest_idc]
                rrs443 = Rrs_443[nearest_idc]
                rrs469 = Rrs_469[nearest_idc]
                rrs488 = Rrs_488[nearest_idc]
                rrs531 = Rrs_531[nearest_idc]
                rrs547 = Rrs_547[nearest_idc]
                rrs555 = Rrs_555[nearest_idc]
                rrs645 = Rrs_645[nearest_idc]
                rrs667 = Rrs_667[nearest_idc]
                rrs678 = Rrs_678[nearest_idc]
                # chla = chlor_a[nearest_idc]
                l2_flag = l2_flags[nearest_idc]
                match_lon = lon_array[nearest_idc]
                match_lat = lat_array[nearest_idc]

                # 将数据添加到 DataFrame 中
                df_temp = pd.DataFrame({
                    'point_id': [point_id],
                    'Longitude': [longitude],
                    'Latitude': [latitude],
                    'Matched_Longitude': [match_lon],
                    'Matched_Latitude': [match_lat],
                    'Rrs_412': [rrs412],
                    'Rrs_443': [rrs443],
                    'Rrs_469': [rrs469],
                    'Rrs_488': [rrs488],
                    'Rrs_531': [rrs531],
                    'Rrs_547': [rrs547],
                    'Rrs_555': [rrs555],
                    'Rrs_645': [rrs645],
                    'Rrs_667': [rrs667],
                    'Rrs_678': [rrs678],
                    # 'Chlor_a': [chla],
                    'L2_Flags': [l2_flag]
                })

                buffer_df = pd.concat([buffer_df, df_temp], ignore_index=True)
        # print("open this file successful\n")
        except Exception as e:
            # 打开文件，将标准输出流追加到文件
            print(f"文件{filename}无法打开：{e}")
            with open(error_file, 'a+') as f:
                # 将标准输出流重定向到文件
                sys.stdout = f
                # 执行输出操作
                print(f"文件{filename}无法打开：{e}")
            # 恢复标准输出流
            sys.stdout = sys.__stdout__  # 恢复标准输出流到默认值
            continue
    else:
        print(f"don't have this file {filename}\n")
    print(f"now is {num}, total is {total_num}\n")

    if (num % 100 == 0):
        buffer_df.to_csv(output_csv, mode='a', header=False, index=False)
        buffer_df = buffer_df.iloc[0:0]  # Clear the buffer


# 写入剩余的数据
if not buffer_df.empty:
    buffer_df.to_csv(output_csv, mode='a', header=False, index=False)
