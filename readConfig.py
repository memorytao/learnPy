import pandas as pd 
from pathlib import Path
import os


df = pd.read_excel('C:/dev/pelatro/processing/[Alignment] May Action plan 2023/Postpaid_NBA_State_configure_1.2_paletro_20230508 - Copy.xlsx',sheet_name='NBAstates_priorityPoP.map')
# print(df['Export configure'].to_string(index=False))