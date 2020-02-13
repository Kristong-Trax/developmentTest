import os
import datetime
import pandas as pd

from mock import MagicMock
from Trax.Algo.Calculations.Core.DataProvider import Data


DATA_FILE = 'test_case_data.xlsx'
POS_FILE = 'test_case_pos_kpi_template.xlsx'


class DataTestUnitCCRU(object):

    pos_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), POS_FILE)
    pos_data = pd.read_excel(pos_file, sheet_name='test')

    data_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), DATA_FILE)
    products = pd.read_excel(data_file, sheet_name='products')
    templates = pd.read_excel(data_file, sheet_name='templates')
    scenes_info = pd.read_excel(data_file, sheet_name='scenes_info')
    store_areas = pd.read_excel(data_file, sheet_name='store_areas')
    scif = pd.read_excel(data_file, sheet_name='scif')

    project_name = 'Test_Project_1'
    session_uid = 'SESSION_1'
    external_session_id = 'EXT_SESSION_1'
    store_number = '10000000001'
    test_store = None
    attr15_store = 1.0
    pos_kpi_set_name = 'POS Test'
    session_user = {'user_position': 'MD', 'user_name': 'USER', 'user_role': 'Sales Rep'}
    planned_visit_flag = 1

    data_provider_data = dict()
    data_provider_data[Data.SESSION_INFO] = pd.DataFrame([{'pk': 1, 'visit_type_fk': 1, 's_sales_rep_fk': 1}])
    data_provider_data[Data.VISIT_DATE] = datetime.date(2020, 1, 1)
    data_provider_data[Data.STORE_FK] = 1
    data_provider_data[Data.OWN_MANUFACTURER] = pd.DataFrame([{'param_name': 'manufacturer_id', 'param_value': 1}])
    data_provider_data[Data.SURVEY_RESPONSES] = MagicMock()
    data_provider_data[Data.ALL_PRODUCTS] = products
    data_provider_data[Data.ALL_TEMPLATES] = templates
    data_provider_data[Data.SCENES_INFO] = scenes_info
    data_provider_data[Data.SCENE_ITEM_FACTS] = scif
    data_provider_data[Data.MATCHES] = MagicMock()





