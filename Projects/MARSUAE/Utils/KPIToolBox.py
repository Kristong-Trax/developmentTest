import os

import pandas as pd
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DB.Common import Common as CommonV1
from KPIUtils_v2.Utils.Parsers import ParseTemplates

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from Projects.MARSUAE.Utils.Runner import Results

__author__ = 'israels'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

SHEETS_NAME = ['KPI', 'SOS', 'Distribution', 'Availability']
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class MARSUAEToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    NON_KPI = 0
    ATOMIC_KPI_NAME = 'atomic_kpi_name'
    KPI_NAME = 'kpi_name'
    KPI_SET_NAME = 'kpi_set_name'

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
        # self.channel = self.get_store_channel(self.store_id)
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.data_provider.kpi_sheets = {}
        self.kpi_sheets = self.data_provider.kpi_sheets
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.scene_results = self.ps_data_provider.get_scene_results(self.scene_info['scene_fk'].drop_duplicates().values)
        self.old_kpi_static_data = self.common.get_kpi_static_data()
        for name in SHEETS_NAME:
            parsed_template = ParseTemplates.parse_template(TEMPLATE_PATH, sheet_name=name)
            self.kpi_sheets[name] = parsed_template[parsed_template['Channel'] == 'Impulse']

    def insert_results_to_old_tables(self):
        kpi_lvls = pd.DataFrame(columns=['level_by_num', 'level_by_name', 'kpis'])
        kpi_lvls['level_by_num'] = [self.LEVEL3, self.LEVEL2, self.LEVEL1]
        kpi_lvls['level_by_name'] = [self.ATOMIC_KPI_NAME, self.KPI_NAME, self.KPI_SET_NAME]
        for kpi in kpi_lvls['level_by_name'].values:
            kpis = self.old_kpi_static_data[kpi].drop_duplicates().values
            kpis = ",".join(kpis)
            kpi_lvls.loc[kpi_lvls['level_by_name'] == kpi, 'kpis'] = kpis
        for row in kpi_lvls.itertuples():
            for kpi in row.kpis.split(','):
                old_kpi_fk = self.commonV1.get_kpi_fk_by_kpi_name(kpi, row.level_by_num)
                if row.level_by_num != self.LEVEL1:
                    new_kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi)
                else:
                    new_kpi_fk = self.common.get_kpi_fk_by_kpi_type('scene_score')
                score = self.scene_results[self.scene_results['kpi_level_2_fk'] == new_kpi_fk]['score'].sum()
                self.commonV1.write_to_db_result(old_kpi_fk, row.level_by_num, score)

    def main_function(self):
        """
        This function calculates the KPI results.
        """
        assortment_result = Assortment(data_provider=self.data_provider).calculate_lvl3_assortment()
        Results(self.data_provider, assortment_result).calculate(self.kpi_sheets['KPI'])
        relevant_kpi_res = self.common.get_kpi_fk_by_kpi_type('scene_score')
        scene_kpi_fks = self.scene_results[self.scene_results['kpi_level_2_fk'] == relevant_kpi_res]['pk'].values
        origin_res = self.scene_results[self.scene_results['kpi_level_2_fk'] == relevant_kpi_res]['result'].sum()
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('store_score')
        identifier_result = self.common.get_dictionary(kpi_fk=kpi_fk)
        identifier_result['session_fk'] = self.session_info['pk'].values[0]
        identifier_result['store_fk'] = self.store_id
        # self.common.write_to_db_result(fk=kpi_fk, numerator_id=manu_fk, numerator_result=origin_res,
        #                                denominator_id=self.store_id, result=origin_res, score=origin_res,
        #                                should_enter=False, identifier_result=identifier_result)
        for scene in scene_kpi_fks:
            self.common.write_to_db_result(fk=self.NON_KPI, should_enter=True, scene_result_fk=scene,
                                           identifier_parent=identifier_result)
        self.insert_results_to_old_tables()
        return

    def get_store_channel(self, store_fk):
        query = self.get_store_attribute(15, store_fk)
        att15 = pd.read_sql_query(query, self.rds_conn.db)
        return att15.values[0][0]

    @staticmethod
    def get_store_attribute(attribute, store_fk):
        return """
                    select additional_attribute_{} from static.stores
                    where pk = {}
                    """.format(attribute, store_fk)