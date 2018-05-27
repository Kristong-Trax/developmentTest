import pandas as pd


__author__ = 'yoava'

scene_template_head = {"template_name":{"0":"ADDITIONAL AMBIENT PLACEMENT","1":"ADDITIONAL AMBIENT PLACEMENT","2":"CHILLED CASHIER COOLER PLACEMENT","3":"MAIN PLACEMENT","4":"MAIN PLACEMENT"},"scene_fk":{"0":182,"1":182,"2":183,"3":181,"4":181}}

scene_template_complete = {"template_name":{"0":"ADDITIONAL AMBIENT PLACEMENT","1":"ADDITIONAL AMBIENT PLACEMENT","2":"CHILLED CASHIER COOLER PLACEMENT","3":"MAIN PLACEMENT","4":"MAIN PLACEMENT","5":"MAIN PLACEMENT","6":"MAIN PLACEMENT","7":"MAIN PLACEMENT","8":"MAIN PLACEMENT","9":"MAIN PLACEMENT","10":"MAIN PLACEMENT","11":"MAIN PLACEMENT","12":"MAIN PLACEMENT","13":"MAIN PLACEMENT","14":"MAIN PLACEMENT","15":"MAIN PLACEMENT","16":"MAIN PLACEMENT"},"scene_fk":{"0":182,"1":182,"2":183,"3":181,"4":181,"5":181,"6":181,"7":181,"8":181,"9":181,"10":181,"11":181,"12":181,"13":181,"14":181,"15":181,"16":181}}

atomic_kpi_fk = {"atomic_kpi_name":{"4":"K001","5":"K002","6":"K003","7":"K004","8":"K005"},"atomic_kpi_fk":
    {"4":9,"5":10,"6":11,"7":12,"8":13},"kpi_name":{"4":"Shelf occupation","5":"Shelf occupation","6":"Shelf occupation","7":"Shelf occupation","8":"Shelf occupation"},"kpi_fk":{"4":3,"5":3,"6":3,"7":3,"8":3},"kpi_set_name":{"4":"Shelf occupation","5":"Shelf occupation","6":"Shelf occupation","7":"Shelf occupation","8":"Shelf occupation"},"kpi_set_fk":{"4":3,"5":3,"6":3,"7":3,"8":3}}


def get_scene_data_head():
        return pd.DataFrame(scene_template_head)


def get_scene_data_complete():
        return pd.DataFrame(scene_template_complete)


def get_atomic_fk():
    return pd.DataFrame(atomic_kpi_fk)
