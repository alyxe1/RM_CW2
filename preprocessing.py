# -*- coding: utf-8 -*-
# @Author : Xuanhe Er
# @Time   : 24/04/2023 18:40
import pandas as pd
import os

path = 'population_data'
# path = 'deathrate_data'
file_name_list = os.listdir(path)
file_name_list.sort()
print(file_name_list)
df_list = []
country_dict = {'AUS':'Australia',
                'CAN':'Canada',
                'CHE':'Switzerland',
                'DEUTNP':'Germany',
                'ESP':'Spain',
                'FIN':'Finland',
                'FRACNP':'France',
                'GBR_NP':'United Kingdom',
                'ISL':'Iceland',
                'ITA':'Italy',
                'JPN':'Japan',
                'NOR':'Norway',
                'SWE':'Sweden',
                'USA':'United States of America',
                }
for file_name in file_name_list:
    df = pd.read_table(f"population_data/{file_name}", sep=r'\s{2,}', skiprows=2, engine='python')
    df_list.append(df)
# print(df_list[0].head(10))
# print(df_list[0].info())

# deal with  -+ population_data
for i in range(len(df_list)):
    rows = [x for x in df_list[i].index if ('-' in str(df_list[i].loc[x]['Year']) or '+' in str(df_list[i].loc[x]['Year']))]
    df_list[i] = df_list[i].drop(rows, axis=0)
    df_list[i]["Year"] = df_list[i]["Year"].astype('int')
print(df_list[0].info())
print(df_list[0].loc[df_list[0]["Year"]=="1949+"])

code_pointer = 0
def filter_raw_df(df, code_pointer):

    result_df = df.loc[df["Year"] >= 1950]
    result_df = result_df.loc[result_df["Year"] <= 2020]
    result_df = result_df.loc[result_df["Age"] != '110+']
    result_df["Age"] = result_df["Age"].astype('int')
    result_df["Country"] = country_dict.get(file_name_list[code_pointer].split(".")[0])
    return result_df
result_df = filter_raw_df(df_list[0], code_pointer)

print(result_df.head(20))
# print(result_df.tail(20))
# print(result_df.loc[result_df["Age"]=='110+'])
print(result_df.info())
df_list.pop(0)
for df in df_list:
    code_pointer+=1
    result_df = pd.concat([result_df, filter_raw_df(df, code_pointer)], ignore_index=True)

result_df.to_excel('./preprocessed_data.xlsx')

result_df2 = result_df.groupby(['Country', 'Year'],as_index=False).sum()
result_df2.drop('Age', axis=1, inplace=True)
# print(result_df2.head(10))
result_df2.to_excel('./preprocessed_data2.xlsx')


