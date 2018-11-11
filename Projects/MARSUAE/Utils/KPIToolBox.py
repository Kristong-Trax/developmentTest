import os

import pandas as pd
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DB.Common import Common as CommonV1
from KPIUtils_v2.Utils.Parsers import ParseTemplates
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.SOSCalculations import SOS

from Projects.MARSUAE.Utils.Runner import Results

__author__ = 'israels'

SHEETS_NAME = ['KPI', 'SOS', 'Distribution', 'Availability']
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class ToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = CommonV2(self.data_provider)
        self.data_provider.common = self.common
        self.commonV1 = CommonV1(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.channel = self.get_store_channel(self.store_id)
        self.kpi_static_data = self.common.get_kpi_static_data()
        # self.kpi_results_queries = []
        self.kpi_sheets = {}
        # self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        # self.scene_results = self.ps_data_provider.get_scene_results(self.scene_info['scene_fk'].drop_duplicates().values)
        self.old_kpi_static_data = self.common.get_kpi_static_data()
        for name in SHEETS_NAME:
            parsed_template = ParseTemplates.parse_template(TEMPLATE_PATH, sheet_name=name)
            self.kpi_sheets[name] = parsed_template[parsed_template['Channel'] == self.channel]
        self.data_provider.sos = SOS(self.data_provider, output=None)
        self.data_provider.assortment = Assortment(self.data_provider, output=None)

    def main_function(self):
        """
        This function calculates the KPI results.
        """
        Results(self.data_provider).calculate(self.kpi_sheets['KPI'])
        return

    def get_store_channel(self, store_fk):
        query = self.get_store_attribute(1, store_fk)
        att15 = pd.read_sql_query(query, self.rds_conn.db)
        return att15.values[0][0]

    @staticmethod
    def get_store_attribute(attribute, store_fk):
        return """
                    select additional_attribute_{} from static.stores
                    where pk = {}
                    """.format(attribute, store_fk)