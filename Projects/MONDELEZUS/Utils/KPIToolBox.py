import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import math
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import os
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.MONDELEZUS.Utils.ParseTemplates import parse_template
import datetime

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
CATEGORIES = ['Sweets', 'Chewing Gum & Mints', 'Gum', 'Chocolate', 'Cookies & Crackers', 'Cough']

MM_TO_FEET_CONVERSION = 0.0032808399
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KENGINE_MONDELEZUS_V1.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class MONDELEZUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, common):
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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        self.kpi_results_queries = []
        self.potential_products = {}
        self.shelf_square_boundaries = {}
        self.average_shelf_values = {}
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.all_template_data = parse_template(TEMPLATE_PATH, "KPI")
        self.template_info = self.data_provider.all_templates
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.INCLUDE_FILTER = 1
        self.MM_TO_FEET_CONVERSION = MM_TO_FEET_CONVERSION
        self.kpi_new_static_data = self.common.kpi_static_data
        self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.template_info, on='template_fk', suffixes=['', '_t'])

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """

        template_data = self.all_template_data.loc[self.all_template_data['KPI Type'] == 'SET_SIZE']
        for i, row in template_data.iterrows():
            try:
                kpi_type = row['KPI Type']

                if kpi_type == 'SET_SIZE':
                    scene_type = row['Scene_Type'].split(',')
                    if row['Param1'] == 'category':
                        category = row['Value1']

                        kpi_set_fk = \
                            self.kpi_new_static_data.loc[self.kpi_new_static_data['type'] == kpi_type]['pk'].values[0]
                        if kwargs['kpi_set_fk'] == kpi_set_fk:
                            self.calculate_category_space(kpi_set_fk, kpi_type, category, scene_types=scene_type)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_type.encode('utf-8'), e))
                continue
        return

    def calculate_category_space(self, kpi_set_fk, kpi_type, category, scene_types=None):
        kpi_template = self.all_template_data.loc[(self.all_template_data['KPI Type'] == kpi_type) &
                                                  (self.all_template_data['Value1'] == category)]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []

        #Work around for mondelez until scene gets updated
        if scene_types == 'Chewing Gum Primary Location':
            scene_types = 'Chewing Gum Primary Location '


        filters = {'template_name': scene_types, 'category': kpi_template['Value1']}
        category_att = ''
        if kpi_template['Value1'] in CATEGORIES:
            category_att = 'category'

        if kpi_template['Value1']:
            values_to_check = self.all_products.loc[self.all_products[category_att] == kpi_template['Value1']][
                category_att].unique().tolist()

        for primary_filter in values_to_check:
            filters[kpi_template['Param1']] = primary_filter


            new_kpi_name = self.kpi_name_builder(kpi_type, **filters)

            result = self.calculate_category_space_length(new_kpi_name, **filters)

            score = result
            numerator_id = self.products['category_fk'][self.products['category'] == kpi_template['Value1']].iloc[0]
            self.common.write_to_db_result_new_tables(kpi_set_fk, numerator_id, 999, score, score=score)

    def calculate_category_space_length(self, kpi_name, threshold=0.5, retailer=None, exclude_pl=False, **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            space_length = 0
            shelf_values = []

            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.mpis[self.mpis['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for shelf in scene_matches['shelf_number'].unique().tolist():
                    scene_filters['shelf_number'] = shelf

                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear.loc[(tested_group_linear['stacking_layer'] == 1)][
                        'width_mm_advance'].sum()

                    shelf_values.append(tested_group_linear_value)
                max_shelf_mm = max(shelf_values)
                space_length = (3 * math.ceil((max_shelf_mm * self.MM_TO_FEET_CONVERSION) / 3.))


        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length

    def get_category(self):
        pass

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUPNGROGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition

    def kpi_name_builder(self, kpi_name, **filters):
        """
        This function builds kpi name according to naming convention
        """
        for filter in filters.keys():
            if filter == 'template_name':
                continue
            kpi_name = kpi_name.replace('{' + filter + '}', str(filters[filter]))
            kpi_name = kpi_name.replace("'", "\'")
        return kpi_name
