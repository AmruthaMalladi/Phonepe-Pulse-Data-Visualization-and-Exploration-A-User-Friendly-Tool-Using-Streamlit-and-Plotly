# =================================================== / / E T L / / ================================================== #

# ================================================== / IMPORT LIBRARY / ======================================================== #

# [clone libraries]
import requests
import subprocess

# [pandas and file handling libraries]
import pandas as pd
import os
import json

# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

# ===================================================== / CLONING / ============================================================== #

# Specify the GitHub repository URL
response = requests.get('https://api.github.com/repos/PhonePe/pulse')
repo = response.json()
clone_url = repo['clone_url']

# Specify the local directory path
clone_dir = "C:/Amrutha/Phonepe Pulse data"

# Clone the repository to the specified local directory
subprocess.run(["git", "clone", clone_url, clone_dir], check=True)

# =============================================== / DATA PROCESSING / =========================================================== #

# ============================== DATA / AGGREGATED / TRANSACTION ===================================#
# 1

path_1 = "C:/Amrutha/Phonepe Pulse data/data/aggregated/transaction/country/india/state/"
Agg_tran_state_list = os.listdir(path_1)

Agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_tran_state_list:
    p_i = path_1 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            with open(p_k, 'r') as Data:
                A = json.load(Data)

            for l in A['data']['transactionData']:
                Name = l['name']
                count = l['paymentInstruments'][0]['count']
                amount = l['paymentInstruments'][0]['amount']
                Agg_tra['State'].append(i)
                Agg_tra['Year'].append(j)
                Agg_tra['Quarter'].append(int(k.strip('.json')))
                Agg_tra['Transaction_type'].append(Name)
                Agg_tra['Transaction_count'].append(count)
                Agg_tra['Transaction_amount'].append(amount)

df_aggregated_transaction = pd.DataFrame(Agg_tra)

# ============================== DATA / AGGREGATED / USER ===================================#
# 2

path_2 = "C:/Amrutha/Phonepe Pulse data/data/aggregated/user/country/india/state/"
Agg_user_state_list = os.listdir(path_2)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in Agg_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            with open(p_k, 'r') as Data:
                B = json.load(Data)

            try:
                for l in B["data"]["usersByDevice"]:
                    brand_name = l["brand"]
                    count_ = l["count"]
                    ALL_percentage = l["percentage"]
                    Agg_user["State"].append(i)
                    Agg_user["Year"].append(j)
                    Agg_user["Quarter"].append(int(k.strip('.json')))
                    Agg_user["Brands"].append(brand_name)
                    Agg_user["User_Count"].append(count_)
                    Agg_user["User_Percentage"].append(ALL_percentage * 100)
            except:
                pass

df_aggregated_user = pd.DataFrame(Agg_user)

# ============================== DATA / MAP / TRANSACTION =========================================#
# 3

path_3 = "C:/Amrutha/Phonepe Pulse data/data/map/transaction/hover/country/india/state/"
map_tra_state_list = os.listdir(path_3)

map_tra = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Transaction_Count': [], 'Transaction_Amount': []}

for i in map_tra_state_list:
    p_i = path_3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            with open(p_k, 'r') as Data:
                C = json.load(Data)

            for l in C["data"]["hoverDataList"]:
                District = l["name"]
                count = l["metric"][0]["count"]
                amount = l["metric"][0]["amount"]
                map_tra['State'].append(i)
                map_tra['Year'].append(j)
                map_tra['Quarter'].append(int(k.strip('.json')))
                map_tra["District"].append(District)
                map_tra["Transaction_Count"].append(count)
                map_tra["Transaction_Amount"].append(amount)

df_map_transaction = pd.DataFrame(map_tra)

# ============================== DATA / MAP / USER ============================================#
# 4

path_4 = "C:/Amrutha/Phonepe Pulse data/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(path_4)

map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

for i in map_user_state_list:
    p_i = path_4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            with open(p_k, 'r') as Data:
                D = json.load(Data)

            for l in D["data"]["hoverData"].items():
                district = l[0]
                registereduser = l[1]["registeredUsers"]
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
                map_user["District"].append(district)
                map_user["Registered_User"].append(registereduser)

df_map_user = pd.DataFrame(map_user)

# 5

path_5 = "C:/Amrutha/Phonepe Pulse data/data/top/transaction/country/india/state/"
top_tra_state_list = os.listdir(path_5)

top_tra = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for
