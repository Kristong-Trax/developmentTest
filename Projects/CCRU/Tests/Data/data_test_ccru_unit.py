import os
import datetime
import pandas as pd

from mock import MagicMock
from Trax.Algo.Calculations.Core.DataProvider import Data


DATA_FILE = 'test_case_data.xlsx'
POS_FILE = 'test_case_pos_kpi_template.xlsx'

PROJECT_NAME = 'Test_Project_1'

VISIT_DATE = datetime.date(2020, 1, 1)

SESSION_UID = 'SESSION_1'
EXTERNAL_SESSION_ID = 'EXT_SESSION_1'

STORE_FK = 1
STORE_NUMBER = '10000000001'
TEST_STORE = None
ATTR15_STORE = 1.5

SESSION_USER = {'user_position': 'MD', 'user_name': 'USER', 'user_role': 'Sales Rep'}
SESSION_INFO = pd.DataFrame([{'pk': 1, 'visit_type_fk': 1, 's_sales_rep_fk': 1}])
OWN_MANUFACTURER = pd.DataFrame([{'param_name': 'manufacturer_id', 'param_value': 1}])

POS_KPI_SET_NAME = 'POS Test'
POS_KPI_SET_TYPE = 'POS'

PLANNED_VISIT_FLAG = 1


class DataTestUnitCCRU(object):

    project_name = PROJECT_NAME
    session_uid = SESSION_UID
    external_session_id = EXTERNAL_SESSION_ID
    store_number = STORE_NUMBER
    test_store = TEST_STORE
    attr15_store = ATTR15_STORE
    pos_kpi_set_name = POS_KPI_SET_NAME
    pos_kpi_set_type = POS_KPI_SET_TYPE
    session_user = SESSION_USER
    planned_visit_flag = PLANNED_VISIT_FLAG

    def __init__(self):

        pos_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), POS_FILE)
        self._pos_data = pd.read_excel(pos_file, sheet_name='test')

        data_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), DATA_FILE)
        self._products = pd.read_excel(data_file, sheet_name='products')
        self._templates = pd.read_excel(data_file, sheet_name='templates')
        self._scenes_info = pd.read_excel(data_file, sheet_name='scenes_info')
        self._store_areas = pd.read_excel(data_file, sheet_name='store_areas')
        self._scif = pd.read_excel(data_file, sheet_name='scif')

    @property
    def pos_data(self):
        return self._pos_data

    @property
    def store_areas(self):
        return self._store_areas

    @property
    def data_provider_data(self):

        _data_provider_data = dict()

        _data_provider_data[Data.SURVEY_RESPONSES] = MagicMock()
        _data_provider_data[Data.MATCHES] = MagicMock()

        _data_provider_data[Data.SESSION_INFO] = SESSION_INFO
        _data_provider_data[Data.VISIT_DATE] = VISIT_DATE
        _data_provider_data[Data.STORE_FK] = STORE_FK
        _data_provider_data[Data.OWN_MANUFACTURER] = OWN_MANUFACTURER

        _data_provider_data[Data.ALL_PRODUCTS] = self._products.copy()
        _data_provider_data[Data.ALL_TEMPLATES] = self._templates.copy()
        _data_provider_data[Data.SCENES_INFO] = self._scenes_info.copy()
        _data_provider_data[Data.SCENE_ITEM_FACTS] = self._scif.copy()

        return _data_provider_data
