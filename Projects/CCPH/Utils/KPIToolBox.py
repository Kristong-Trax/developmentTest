
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'satya'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


class CCPHConsts(object):
    KPI_GROUP = 'kpi_group'
    KPI_NAME = 'kpi_name'
    FACINGS_SOS = 'FSOS'
    SHELF_PURITY = "SHELF_PURITY"
    EXCLUDE_EMPTY = "exclude_empty"
    EXCLUDE_IRRELEVANT = "exclude_irrelevant"
    GENERAL_EMPTY = 0
    SCENE_TYPE = 'scene_type'
    PRODUCT_FK = 'product_fk'
    EMPTY = 1251
    IRRELEVANT = 1252
    CUSTOM_KPIS = 19
    OWN_MANUFACTURER_FK = 2 # TCCC


class CCPHToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
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
        self.template_data = pd.read_excel(TEMPLATE_PATH, 'KPIs').fillna('')
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_static_data = self.kpi_static_data[self.kpi_static_data['kpi_family_fk'] == CCPHConsts.CUSTOM_KPIS]
        self.kpi_results_queries = []
        self.mapping = {'manufacturer': 'manufacturer_name'}

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        score = 0
        for group in self.template_data[CCPHConsts.KPI_GROUP].unique():
            kpis = self.template_data[self.template_data[CCPHConsts.KPI_GROUP] == group]

            if kpis.empty:
                print("KPI Group:{} is not valid".format(group))
                continue

            for row_num, kpi in kpis.iterrows():
                if kpi[CCPHConsts.KPI_GROUP] == CCPHConsts.SHELF_PURITY:
                    score = self.calculate_self_purity(kpi)

            else:
                continue

        self.common.commit_results_data_to_new_tables()

        return score

    def calculate_self_purity(self, kpi):
        result = 0
        df_scene_data = self.scif

        if df_scene_data.empty:
            return

        scene_types = [x.strip() for x in kpi[CCPHConsts.SCENE_TYPE].split(',')]

        filter_param_name_1 = kpi['filter_param_name_1'].strip()
        filter_param_value_1 = ""

        if len(filter_param_name_1) != 0:
            filter_param_value_1 = kpi['filter_param_value_1'].strip()

        if kpi[CCPHConsts.EXCLUDE_EMPTY] in ['Y', 'y']:
            df_scene_data = df_scene_data[df_scene_data[CCPHConsts.PRODUCT_FK] != CCPHConsts.EMPTY]
            df_scene_data = df_scene_data[df_scene_data[CCPHConsts.PRODUCT_FK] != CCPHConsts.GENERAL_EMPTY]

        if kpi[CCPHConsts.EXCLUDE_IRRELEVANT] in ['Y', 'y']:
            df_scene_data = df_scene_data[df_scene_data[CCPHConsts.IRRELEVANT] != CCPHConsts.IRRELEVANT]

        df_kpi_level_2_fk = self.kpi_static_data[self.kpi_static_data['type'] == kpi['kpi_name']]

        if df_kpi_level_2_fk.empty:
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = df_kpi_level_2_fk.iloc[0]['pk']

        df_scene_data = df_scene_data[df_scene_data['template_name'].isin(scene_types)]

        total_facings = df_scene_data['facings'].sum()
        denominator = float(total_facings)

        if len(filter_param_value_1) != 0:
            df_scene_data = df_scene_data[df_scene_data[self.mapping[filter_param_name_1]] == filter_param_value_1]

        df_scene_data = df_scene_data[['manufacturer_fk', 'facings']]
        df_purity = pd.DataFrame(df_scene_data.groupby('manufacturer_fk').sum().reset_index())

        for row_num, row_data in df_purity.iterrows():
            numerator = row_data['facings']
            try:
                result = round(float(numerator) / float(denominator), 4)
            except ZeroDivisionError:
                print("Error: {}".format(ZeroDivisionError.message))
                continue

            if kpi_level_2_fk != 0:
                self.common.write_to_db_result_new_tables(fk=kpi_level_2_fk,
                                                          numerator_id=row_data['manufacturer_fk'],
                                                          denominator_id=self.store_id,
                                                          numerator_result=numerator,
                                                          denominator_result=denominator,
                                                          result=result,
                                                          score=result)

        return result
