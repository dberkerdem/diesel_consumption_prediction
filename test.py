import psycopg2
from db_utils.db_conn import PosgreConnector
from src.utils.config import load_config
import pandas as pd
# config = load_config("config.yaml")
# username = config['dataload']['posgresql']['username']
# password = config['dataload']['posgresql']['password']
# contact_points = config['dataload']['posgresql']['contact_points']
# database = config['dataload']['posgresql']['database']
# port = config['dataload']['posgresql']['port']
# conn = psycopg2.connect(
#     host=contact_points,
#     database=database,
#     user=username,
#     password=password)
config = load_config("config.yaml")
pgconnector = PosgreConnector(config=config)
print("Success")
# cursor = conn.cursor()
# query =  """select * from daas.epdk_petrol_province limit 100;"""
# cursor.execute(query)
# data = cursor.fetchall()
# print(pd.DataFrame(data))
# conn.close()