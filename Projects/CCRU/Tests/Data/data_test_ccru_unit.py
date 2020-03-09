import os
import datetime
import pandas as pd

from mock import MagicMock
from Trax.Algo.Calculations.Core.DataProvider import Data


DATA_FILE = 'test_case_data.xlsx'
POS_FILE = 'test_case_pos_kpi_template.xlsx'

PROJECT_NAME = 'Test_Project_1'

VISIT_DATE = datetime.date(2020, 1, 1)

SESSION_ID = 1
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

TOP_SKU_KPI_SET_NAME = 'Top SKU'
TOP_SKU_KPI_SET_TYPE = 'TOPSKU'
TOP_SKUS = {'product_fks': {1: '1', 2: '2,4', 16: '16,6', 7: '7,17'},
               'min_facings': {1: 6, 2: 1, 16: 1, 7: 1}}

TOP_SKU_QUERIES = \
    ["INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('16', '0', '0', '1', '1', '0', '1', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('1', '0', '0', '1', '1', '0', '1', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('2', '0', '0', '1', '1', '0', '1', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('7', '0', '0', '1', '1', '0', '1', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('16', '0', '0', '1', '1', '0', '2', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('1', '0', '0', '1', '1', '0', '2', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('2', '0', '0', '1', '1', '0', '2', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('7', '0', '0', '1', '1', '0', '2', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('16', '0', '0', '1', '1', '0', '3', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('1', '0', '0', '1', '1', '0', '3', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('2', '0', '0', '1', '1', '0', '3', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('7', '0', '0', '1', '1', '0', '3', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('16', '0', '0', '1', '1', '0', '4', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('1', '0', '0', '1', '1', '0', '4', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('2', '0', '0', '1', '1', '0', '4', '0')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('7', '0', '0', '1', '1', '0', '4', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('16', '0', '0', '1', '1', '0', '5', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('1', '0', '0', '1', '1', '0', '5', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('2', '0', '0', '1', '1', '0', '5', '1')",
     "INSERT INTO pservice.custom_scene_item_facts (product_fk, mha_in_assortment, mha_oos, session_fk, in_assortment_osa, length_mm_custom, scene_fk, oos_osa)\n           VALUES ('7', '0', '0', '1', '1', '0', '5', '1')"]

KPI_NAME_TO_ID = {'kpi_1': 1,
                  'kpi_2': 2,
                  'kpi_3': 3,
                  'kpi_4': 4,
                  'kpi_5': 5}

KPI_SCORES_AND_RESULTS = {1: {'score': 0},
                          2: {'score': 100},
                          3: {'score': 30},
                          4: {'score': 100},
                          5: {'score': 100}}


class DataTestUnitCCRU(object):

    project_name = PROJECT_NAME
    session_id = SESSION_ID
    session_uid = SESSION_UID
    external_session_id = EXTERNAL_SESSION_ID
    store_number = STORE_NUMBER
    test_store = TEST_STORE
    attr15_store = ATTR15_STORE
    session_user = SESSION_USER
    planned_visit_flag = PLANNED_VISIT_FLAG

    pos_kpi_set_name = POS_KPI_SET_NAME
    pos_kpi_set_type = POS_KPI_SET_TYPE

    kpi_name_to_id = KPI_NAME_TO_ID
    kpi_scores_and_results = KPI_SCORES_AND_RESULTS

    top_sku_kpi_set_name = TOP_SKU_KPI_SET_NAME
    top_sku_kpi_set_type = TOP_SKU_KPI_SET_TYPE
    top_skus = TOP_SKUS
    top_sku_queries = TOP_SKU_QUERIES

    def __init__(self):

        pos_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), POS_FILE)
        self._pos_data = pd.read_excel(pos_file, sheet_name='test')

        data_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), DATA_FILE)
        self._products = pd.read_excel(data_file, sheet_name='products')
        self._templates = pd.read_excel(data_file, sheet_name='templates')
        self._scenes_info = pd.read_excel(data_file, sheet_name='scenes_info')
        self._store_areas = pd.read_excel(data_file, sheet_name='store_areas')
        self._scif = pd.read_excel(data_file, sheet_name='scif')
        self._kpi_level_2 = pd.read_excel(data_file, sheet_name='kpi_level_2')
        self._kpi_result_values = pd.read_excel(data_file, sheet_name='kpi_result_values')
        self._kpi_entity_types = pd.read_excel(data_file, sheet_name='kpi_entity_types')
        self._group_names = pd.read_excel(data_file, sheet_name='group_names')
        self._osa_kpi_results = pd.read_excel(data_file, sheet_name='osa_kpi_results')

    @property
    def pos_data(self):
        return self._pos_data

    @property
    def store_areas(self):
        return self._store_areas

    @property
    def kpi_level_2(self):
        return self._kpi_level_2

    @property
    def kpi_result_values(self):
        return self._kpi_result_values

    @property
    def kpi_entity_types(self):
        return self._kpi_entity_types

    @property
    def group_names(self):
        return self._group_names

    @property
    def products(self):
        return self._products

    @property
    def osa_kpi_results(self):
        return self._osa_kpi_results

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
