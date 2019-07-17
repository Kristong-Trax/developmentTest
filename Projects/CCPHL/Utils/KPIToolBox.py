
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

__author__ = 'sathiyanarayanan'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

class CCPHLConsts(object):
    KPI_GROUP = 'kpi_group'
    KPI_NAME = 'kpi_name'
    FACINGS_SOS = 'FSOS'
    SHELF_PURITY = "SHELF_PURITY"
    SHELF_CUTIL = "SHELF_CUTIL"
    EXCLUDE_EMPTY = "exclude_empty"
    EXCLUDE_IRRELEVANT = "exclude_irrelevant"
    GENERAL_EMPTY = 0
    SCENE_TYPE = 'scene_type'
    PRODUCT_FK = 'product_fk'
    EMPTY = 1251
    IRRELEVANT = 1252
    CUSTOM_KPIS = 19
    OWN_MANUFACTURER_FK = 2  # TCCC


class CCPHLToolBox:
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
        self.kpi_static_data = self.kpi_static_data[self.kpi_static_data['kpi_family_fk'] == CCPHLConsts.CUSTOM_KPIS]
        self.kpi_results_queries = []
        self.mapping_param = {"manufacturer": "manufacturer_name"}
        self.mapping_entity = {"manufacturer": "manufacturer_fk", "store": "store_id",
                               "scene_type": "template_fk", "brand": "brand_fk", "product": "product_fk"}

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        for group in self.template_data[CCPHLConsts.KPI_GROUP].unique():
            kpis = self.template_data[self.template_data[CCPHLConsts.KPI_GROUP] == group]

            if kpis.empty:
                print("KPI Group:{} is not valid".format(group))
                continue

            for row_num, kpi in kpis.iterrows():
                if kpi[CCPHLConsts.KPI_GROUP] == CCPHLConsts.SHELF_PURITY:
                    self.calculate_self_purity_util(kpi)
                elif kpi[CCPHLConsts.KPI_GROUP] == CCPHLConsts.SHELF_CUTIL:
                    self.calculate_self_purity_util(kpi)
            else:
                continue

        self.common.commit_results_data_to_new_tables()
    
    def calculate_self_purity_util(self, kpi):
        df_scene_data = self.scif

        if df_scene_data.empty:
            return

        scene_types = [x.strip() for x in kpi[CCPHLConsts.SCENE_TYPE].split(',')]

        filter_param_name_1 = kpi['filter_param_name_1'].strip()

        if len(filter_param_name_1) != 0:
            filter_param_value_1 = kpi['filter_param_value_1'].strip()
        else:
            filter_param_value_1 = ""

        if str(kpi[CCPHLConsts.EXCLUDE_EMPTY]).upper() == 'Y':
            df_scene_data = df_scene_data[df_scene_data[CCPHLConsts.PRODUCT_FK] != CCPHLConsts.EMPTY]
            df_scene_data = df_scene_data[df_scene_data[CCPHLConsts.PRODUCT_FK] != CCPHLConsts.GENERAL_EMPTY]

        if str(kpi[CCPHLConsts.EXCLUDE_IRRELEVANT]).upper() == 'Y':
            df_scene_data = df_scene_data[df_scene_data[CCPHLConsts.IRRELEVANT] != CCPHLConsts.IRRELEVANT]

        df_kpi_level_2_fk = self.kpi_static_data[self.kpi_static_data['type'] == kpi['kpi_name']]

        if df_kpi_level_2_fk.empty:
            kpi_level_2_fk = 0
        else:
            kpi_level_2_fk = df_kpi_level_2_fk.iloc[0]['pk']

        df_scene_data = df_scene_data[df_scene_data['template_name'].isin(scene_types)]

        group_list = []
        for idx in range(1, 5):
            entity = kpi['entity' + str(idx)].strip()
            if entity == 'N/A' or len(entity) == 0:
                continue
            else:
                entity = self.mapping_entity[kpi['entity' + str(idx)].strip()]
                group_list.append(entity)

        denominator = 0
        if group_list[0] == 'store_id':
            total_facings = df_scene_data['facings'].sum()
            denominator = float(total_facings)

        if len(filter_param_value_1) != 0:
            df_scene_data2 = df_scene_data[df_scene_data[self.mapping_param[filter_param_name_1]]==filter_param_value_1]
        else:
            df_scene_data2 = df_scene_data

        filter_columns = list(group_list)
        filter_columns.append('facings')

        df_scene_data2 = df_scene_data2[filter_columns]

        df_purity = pd.DataFrame(df_scene_data2.groupby(group_list).sum().reset_index())

        for row_num, row_data in df_purity.iterrows():
            numerator = row_data['facings']
            if group_list[0] == 'template_fk':
                df_scene_count = df_scene_data[df_scene_data['template_fk'] == row_data['template_fk']]
                if df_scene_count.empty:
                    total_facings = 0
                else:
                    total_facings = df_scene_count['facings'].sum()
                denominator = float(total_facings)
            try:
                result = round(float(numerator) / float(denominator), 4)
            except ZeroDivisionError:
                print("Error: {}".format(ZeroDivisionError.message))
                continue

            if kpi_level_2_fk != 0:
                self.common.write_to_db_result_new_tables(fk=kpi_level_2_fk,
                                                          numerator_id=row_data[group_list[len(group_list)-1]],
                                                          denominator_id=row_data[group_list[0]],
                                                          numerator_result=numerator,
                                                          denominator_result=denominator,
                                                          result=result,
                                                          score=result)
