import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from sqlalchemy import create_engine

sku_data_query = """
SELECT 
    sk.code_wares, 
    name_wares,
    l2,
    l3,
    l4,
    l5,
    property_value,
    name_group_level5,
    expir_date_days--
    --weight,
    --(CASE WHEN expir_date_days = 0 OR expir_date_days IS NULL OR expir_date_days > 32 OR l3 IN ('F10', 'F16', 'F18', 'F19', 'F20', 'F22') OR l2 IN ('N','S') THEN 32 ELSE expir_date_days END) AS exp_days,
    --(CASE WHEN weight IS NULL OR weight = 0 OR weight > 20 THEN 1 ELSE weight END) AS weight
FROM dwh_com.dict_sku sk
left join ods.wares_property_link tm on tm.code_wares = sk.code_wares and code_property = 1150
ORDER BY tm.code_wares 
"""

def update_sku_params(conn:str):
    bottom_quantile = 0.05
    top_quantile = 0.95

    sku_data = pd.read_sql(sku_data_query, create_engine(conn))
    sku_data = sku_data.replace(0, np.nan)

    for lvl in tqdm(range(2, 6)):
        sku_data[f'l{lvl}_q{int(bottom_quantile*100)}'] = sku_data.groupby(f'l{lvl}')['expir_date_days'].transform(lambda x: x.quantile(bottom_quantile))#.map('{0:.1F}'.format)
        sku_data[f'l{lvl}_q{int(top_quantile*100)}'] = sku_data.groupby(f'l{lvl}')['expir_date_days'].transform(lambda x: x.quantile(top_quantile))#.map('{0:.1F}'.format)

    sku_data['exp_L5'] = np.where((sku_data['expir_date_days']>=sku_data[f'l5_q{int(bottom_quantile*100)}'])
                                    &(sku_data['expir_date_days']<=sku_data[f'l5_q{int(top_quantile*100)}']), 
                                        sku_data['expir_date_days'], 0)

    sku_data['exp_L5_bttm_trim'] = np.where((sku_data['exp_L5']==0)
                                                &(sku_data['expir_date_days']<=sku_data[f'l5_q{int(bottom_quantile*100)}']),
                                                    sku_data[f'l5_q{int(bottom_quantile*100)}'], 
                                                        sku_data['exp_L5'])

    sku_data['exp_L5_top_trim'] = np.where((sku_data['exp_L5_bttm_trim']==0)
                                                &(sku_data['exp_L5']==0)
                                                    &(sku_data['expir_date_days']>=sku_data[f'l5_q{int(top_quantile*100)}']),
                                                        sku_data[f'l5_q{int(top_quantile*100)}'], 
                                                            sku_data['exp_L5_bttm_trim'])

    sku_data['expl5_l4'] = np.where(sku_data['exp_L5_top_trim'] == 0, 
                                        sku_data[[f'l5_q{int(bottom_quantile*100)}', f'l5_q{int(top_quantile*100)}']].max(axis=1), 
                                            sku_data['exp_L5_top_trim'])

    sku_data['expl5_l4_l3'] = np.where(sku_data['expl5_l4'] == 0, 
                                        sku_data[[f'l4_q{int(bottom_quantile*100)}', f'l4_q{int(top_quantile*100)}']].max(axis=1),
                                            sku_data['expl5_l4'])

    sku_data['expl5_l4_l3_l2'] = np.where(sku_data['expl5_l4_l3'] == 0, 
                                        sku_data[[f'l3_q{int(bottom_quantile*100)}', f'l3_q{int(top_quantile*100)}']].max(axis=1), 
                                            sku_data['expl5_l4_l3'])

    sku_data['exp_D'] = np.where(sku_data['expl5_l4_l3_l2'] == 0, 
                                    sku_data[[f'l2_q{int(bottom_quantile*100)}', 
                                        f'l2_q{int(top_quantile*100)}']].max(axis=1), 
                                            sku_data['expl5_l4_l3_l2'])

    sku_data['exp_D'] = np.ceil(sku_data['exp_D'])
    sku_data["low_temp"] = sku_data["property_value"].str.extract("(-?[0-9]+)").astype(float)
    sku_data["high_temp"] = sku_data["property_value"].str.extract("(-?[0-9]+)$").astype(float)
    sku_data["low_temp"] = round(sku_data.groupby("l5")["low_temp"].transform(lambda x: x.fillna(x.mean())), 0)
    sku_data["high_temp"] = round(sku_data.groupby("l5")["high_temp"].transform(lambda x: x.fillna(x.mean())), 0)
    sku_data["check"] = sku_data.apply(lambda x: x["high_temp"] < x["low_temp"], axis=1)
    sku_data.loc[sku_data["check"] == True, "high_temp"] *= -1
    sku_data["low_temp"] = round(sku_data.groupby("l4")["low_temp"].transform(lambda x: x.clip(*x.quantile(q=[0.10, 0.90]))), 0)
    sku_data["high_temp"] = round(sku_data.groupby("l4")["high_temp"].transform(lambda x: x.clip(*x.quantile(q=[0.10, 0.90]))), 0)
    sku_data["is_fresh"] = np.where((sku_data["low_temp"] >= 0) & (sku_data["high_temp"] <= 10), True, False)
    sku_data = sku_data[['code_wares', 'name_wares', 'l2', 'l3', 'l4', 'l5','name_group_level5', 'expir_date_days','exp_D', 'property_value', 'low_temp', 'high_temp', 'is_fresh']]
    sku_data.to_sql('arb_sku_params', schema = 'dwh_com', con = create_engine(conn), if_exists = 'replace', index = False)
    print('arb_sku_params was updated')
