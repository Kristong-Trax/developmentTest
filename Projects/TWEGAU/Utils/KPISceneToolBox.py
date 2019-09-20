import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Utils.Logging.Logger import Log

__author__ = 'nidhinb'
# The KPIs
GSK_DISPLAY_PRESENCE = 'GSK_DISPLAY_PRESENCE'
GSK_DISPLAY_PRESENCE_SKU = 'GSK_DISPLAY_PRESENCE_SKU'
GSK_DISPLAY_SKU_COMPLIANCE = 'GSK_DISPLAY_SKU_COMPLIANCE'
GSK_DISPLAY_PRICE_COMPLIANCE = 'GSK_DISPLAY_PRICE_COMPLIANCE'
GSK_DISPLAY_BAY_PURITY = 'GSK_DISPLAY_BAY_PURITY'
# Other Constants
POS_TYPE = 'POS'
KPI_TYPE_COL = 'type'
POSM_KEY = 'posm_pk'
# Constant IDs
GSK_MANUFACTURER_ID = 2
EMPTY_PRODUCT_ID = 0
# the keys are named as per the config file
# ..ExternalTargetsTemplateLoader/ProjectsDetails/gskau.pyxternalTargetsTemplateLoader/ProjectsDetails/gskau.py
STORE_IDENTIFIERS = [
    'additional_attribute_1', 'additional_attribute_2',
    'region_fk', 'store_type', 'store_number_1', 'address_city',
    'retailer_fk',
]
SCENE_IDENTIFIERS = [
    'template_fk',
]
POSM_PK_KEY = 'posm_pk'
# ALLOWED_POSM_EAN_KEY = 'allowed_posm_eans'
OPTIONAL_EAN_KEY = 'optional_eans'
MANDATORY_EANS_KEY = 'mandatory_eans'
POSM_IDENTIFIERS = [
    POSM_PK_KEY,
    # ALLOWED_POSM_EAN_KEY,
    OPTIONAL_EAN_KEY,
    MANDATORY_EANS_KEY,
]


def _sanitize_csv(data):
    if type(data) == list:
        # not handling ["val1", "val2,val3", "val4"]
        return [x.strip() for x in data if x.strip()]
    else:
        return [each.strip() for each in data.split(',') if each.strip()]


class TWEGAUSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()

        self.kpi_results_queries = []
        self.missing_products = []

    def calculate_zone_kpis(self):
        pass
