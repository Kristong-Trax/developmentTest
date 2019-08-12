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
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'ccus_jeff_demo_1.xlsx')
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


class JEFFToolBox:
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
        self.templates['SOS'] = pd.read_excel(TEMPLATE_PATH)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.calculate_sos()

    def calculate_sos(self):
        for i, row in self.templates[SOS].iterrows():
            for scene in (self.scif['scene_fk'].unique()).tolist():
                parent_kpi_fk = 0

                kpi_name = row['KPI Name'].strip()
                kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)

                num_param1 = row['numerator_param1']
                num_values1 = self.sanitize_values(row['numerator_value1'])

                num_param2 = row['numerator_param2']
                num_values2 = self.sanitize_values(row['numerator_value2'])

                den_param1 = row['numerator_param1']
                den_values1 = self.sanitize_values(row['numerator_value1'])

                den_param2 = row['denominator_param2']
                den_values2 = self.sanitize_values(row['denominator_value2'])

                num_exclude_param1 = row['numerator_exclude_param1']
                num_exclude_value1 = self.sanitize_values(row['numerator_exclude_value1'])

                num_exclude_param2 = row['numerator_exclude_param2']
                num_exclude_value2 = self.sanitize_values(row['numerator_exclude_value2'])

                den_exclude_param = row['denominator_exclude_param']
                den_exclude_value = self.sanitize_values(row['denominator_exclude_value'])

                parent_kpi_name = row['Parent']
                if not pd.isna(parent_kpi_name):
                    parent_kpi_fk = self.common.get_kpi_fk_by_kpi_name(parent_kpi_name.strip())

                filters = {num_param1: num_values1, num_param2: num_values2,  'product_type': ['POS', 'SKU', 'OTHER'],
                           'scene_fk': scene}

                filters = self.delete_filter_nan(filters)
                general_filters = {den_param1: den_values1,
                                   den_param2: den_values2,
                                   'product_type': ['SKU', 'OTHER'],
                                   'scene_fk': scene}
                general_filters = self.delete_filter_nan(general_filters)

                if not pd.isna(num_exclude_param1):
                    if 'ALL' in num_values1:
                        if not pd.isna(num_exclude_param1):
                            all_unique_values = (self.all_products[num_exclude_param1].unique()).tolist()
                            excluded_list = list(set(all_unique_values) - set(num_exclude_value1))
                            filters[num_exclude_param1] = excluded_list

                    if 'ALL' in num_values2:
                        if not pd.isna(num_exclude_param2):
                            all_unique_values = (self.all_products[num_exclude_param2].unique()).tolist()
                            excluded_list = list(set(all_unique_values) - set(num_exclude_value2))
                            filters[num_exclude_param2] = excluded_list

                if not pd.isna(den_exclude_param):
                    if 'ALL' in den_values2:
                        all_unique_values = (self.all_products[den_exclude_param].unique()).tolist()
                        excluded_list = list(set(all_unique_values) - set(den_exclude_value))
                        general_filters[den_exclude_param] = excluded_list

                ratio = self.SOS.calculate_share_of_shelf(filters, **general_filters)

                shelf_count = max(
                    (self.match_product_in_scene['shelf_number'][self.match_product_in_scene['scene_fk'] == scene].
                        unique()).tolist())

                result = ratio
                score = (ratio * shelf_count)

                if parent_kpi_fk == 0:
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                                   denominator_id=scene, denominator_result=0, result=result,
                                                   score=score)
                else:
                    self.common.write_to_db_result(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                                                   denominator_id=scene,denominator_result=0, result=result,
                                                   score=score, identifier_parent=parent_kpi_fk)
    @staticmethod
    def sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            items = [x.strip() for x in item.split(',')]
            return items

    @staticmethod
    def delete_filter_nan(filters):
        for key in filters.keys():
            if type(filters[key]) is not list:
                if pd.isna(filters[key]):
                    del filters[key]
        return filters

    def calculate_availability_df(self, **filters):
        """
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared in the filtered Scene Item Facts data frame.
        """
        if set(filters.keys()).difference(self.scif.keys()):
            scif_mpis_diff = self.match_product_in_scene[['scene_fk', 'product_fk'] +
                                                         list(self.match_product_in_scene.keys().difference(
                                                             self.scif.keys()))]

            # a patch for the item_id field which became item_id_x since it was added to product table as attribute.
            item_id = 'item_id' if 'item_id' in self.scif.columns else 'item_id_x'
            merged_df = pd.merge(self.scif[self.scif.facings != 0], scif_mpis_diff, how='outer',
                                 left_on=['scene_id', item_id], right_on=['scene_fk', 'product_fk'])
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

