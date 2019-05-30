from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.Programs.Utils.Fetcher import NEW_OBBOQueries
from Projects.CCUS.Programs.Utils.GeneralToolBox import NEW_OBBOGENERALToolBox
from Projects.CCUS.Programs.Utils.ParseTemplates import parse_template
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS as SOS_calc
from KPIUtils_v2.Calculations.SurveyCalculations import Survey as Survey_calc

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
STORE_TYPE_LIST = ['LS', 'CR', 'Drug', 'Value']
Availability = 'Availability'
SOS = 'SOS'
Survey = 'Survey'
Sheets = [Availability, SOS]
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Coke_FSOP_v1.xlsx')
CCNA = 'CCNA'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class FSOPToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, commonv2):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output

        self.data_provider = data_provider
        self.common = commonv2
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = NEW_OBBOGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        # self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        # self.rules = pd.read_excel(TEMPLATE_PATH).set_index('store_type').to_dict('index')
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self._position_graphs = PositionGraphs(self.data_provider)
        self.match_product_in_scene = self._position_graphs.match_product_in_scene
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.manufacturer_fk = self.all_products['manufacturer_fk'][self.all_products['manufacturer_name'] == 'CCNA'].iloc[0]
        # self.scene_data = self.load_scene_data()
        # self.kpi_set_fk = kpi_set_fk
        self.templates = {}
        self.parse_template()
        self.toolbox = GENERALToolBox(self.data_provider)
        self.SOS = SOS_calc(self.data_provider)
        self.survey = Survey_calc(self.data_provider)



    def parse_template(self):
        for sheet in Sheets:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        self.calculate_availability()
        self.calculate_sos()

    def calculate_availability(self):
        for i, row in  self.templates[Availability].iterrows():
            kpi_name = row['KPI Name']
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
            manufacturers =  self.sanitize_values(row['manufacturer'])
            brands = self.sanitize_values(row['Brand'])
            container = self.sanitize_values(row['Container'])
            attributte_4 = self.sanitize_values(row['att4'])
            required_brands = row['number_required_brands']
            required_sparkling = row['number_required_Sparkling']
            required_still = row['number_required_Still']
            filters = {'manufacturer_name': manufacturers, 'brand_name': brands, 'Container': container, 'att4': attributte_4 }

            filters = self.delete_filter_nan(filters)

            available_df = self.calculate_availability_df(**filters)

            if pd.notna(required_brands):
                brands_available = len(available_df['brand_name'].unique())
                if brands_available >= int(required_brands):
                    score = 1
                else:
                    score = 0
                # self.common.write_to_db_result(fk=kpi_fk, numerator_id=0, numerator_result=0, denominator_id=0,
                #                                  denominator_result=0, score=score )

            if pd.notna(required_sparkling and required_still):
                if required_sparkling <= len(available_df[available_df['att4'] == 'SSD']):
                    if required_still <= len(available_df[available_df['att4'] == 'Still']):
                        score = 1
                else:
                    score =0
            elif pd.notna(required_sparkling):
                if required_sparkling <= len(available_df[available_df['att4'] == 'SSD']):
                    score = 1
                else:
                    score = 0

            elif pd.notna(required_still):
                if required_still <= len(available_df[available_df['att4'] == 'Still']):
                    score = 1
                else:
                    score = 0


            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0, denominator_id=self.store_id,
                                                         denominator_result=0, score=score )


    def calculate_sos(self):
        for i, row in self.templates[SOS].iterrows():
            kpi_name = row['KPI Name']
            kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
            manufacturers = self.sanitize_values(row['manufacturer'])
            attributte_4 = self.sanitize_values(row['att4'])
            scene_types = self.sanitize_values(row['scene Type'])
            target = int(row['% SOS'])


            filters = {'manufacturer_name': manufacturers, 'att4': attributte_4, 'template_name': scene_types}
            filters = self.delete_filter_nan(filters)
            # general_filters = {}

            ratio = self.SOS.calculate_share_of_shelf(filters)
            if (100 * ratio) >= target:
                score = 1
            else:
                score = 0

            self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                           denominator_id=self.store_id,
                                           denominator_result=0, result=ratio, score=score)


    def sanitize_values(self, item):
        if pd.isna(item):
            return item
        else:
            item = item.split(', ')
            return item


    def delete_filter_nan(self, filters):
        for key in filters.keys():
            if type(filters[key]) is not list:
                if pd.isna(filters[key]):
                    del filters[key]
        return filters

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        self.calculate_availability()
        self.calculate_sos()



    def calculate_availability_df(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            scif_mpis_diff = self.match_product_in_scene[['scene_fk', 'product_fk'] +
                                             list(self.match_product_in_scene.keys().difference(self.scif.keys()))]
            merged_df = pd.merge(self.scif[self.scif.facings != 0], scif_mpis_diff, how='outer',
                                 left_on=['scene_id', 'item_id'], right_on=['scene_fk', 'product_fk'])
            filtered_df = \
                merged_df[self.toolbox.get_filter_condition(merged_df, **filters)]
            # filtered_df = \
            #     self.match_product_in_scene[self.toolbox.get_filter_condition(self.match_product_in_scene, **filters)]
        else:
            filtered_df = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
        if self.facings_field in filtered_df.columns:
            availability_df = filtered_df
        else:
            availability_df = filtered_df
        return availability_df

