# -*- coding: utf-8 -*-
import os

import datetime
import json
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Orm.OrmCore import OrmSession
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from Projects.KCUS_SAND.KCUS_SANDFetcher import KCUS_SANDFetcher
from Projects.KCUS_SAND.Utils.KCUS_SAND_GENERAL_TOOLBOX import KCUS_SANDGENERALToolBox
from Projects.KCUS_SAND.Utils.ParseTemplates import parse_template

__author__ = 'nicolaskeeton'

CATEGORIES = ['ADULT INCONTINENCE PROTECTION','FEM CARE']
SUB_CATEGORIES = ['Feminine Hygiene','Feminine Needs']
KPI_LEVEL_2_cat_space = ['Category Space - Adult Incontinence','Category Space - Feminine Needs','Category Space - Feminine Hygiene']
UNLIMITED_DISTANCE = 'General'
TOP_DISTANCE = 'Above distance (shelves)'
BOTTOM_DISTANCE = 'Below distance (shelves)'
LEFT_DISTANCE = 'Left distance (facings)'
RIGHT_DISTANCE = 'Right distance (facings)'
PACKAGE_COLOR = 'Package color'
SUB_BRAND = 'sub_brand'
SUB_CATEGORY = 'sub_category'
SUB = {'sub_brand': 'sub_brand_x', 'sub_category': 'sub_category_x'}
QUALITY_TIER = 'quality_tier'
TARGET_GROUP = 'target_group'
PACKAGE_SIZE = 'package_size'
TITLE = {'Sub Category': 'sub_category', 'Package color': 'package_color',
         'Sub Brand': 'sub_brand', 'Package Size': 'package_size'}
TRAINING_YOUTH_SWIM_PANTS = 'TRAINING, YOUTH & SWIM PANTS'
YES = 'Yes'
NO = 'No'
BINARY = 'BINARY'
PROPORTIONAL = 'PROPORTIONAL'
CONDITIONAL_PROPORTIONAL = 'CONDITIONAL PROPORTIONAL'
MARS = 'Mars'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
EMPTY = 'Empty'
ALLOWED_EMPTIES_RATIO = 0.2
ALLOWED_DEVIATION = 2
NEGATIVE_ADJACENCY_RANGE = (2, 1000)
POSITIVE_ADJACENCY_RANGE = (0, 1)
MM_TO_FEET_CONVERSION = 0.0032808399
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'KEngine_KC_US_V6.xlsx')

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


class KCUS_SAND_KPIToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output, set_name=None):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.output = output
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.scenes_info = self.data_provider[Data.SCENES_INFO]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_data = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.data_provider[Data.STORE_INFO], how='left', left_on='store_id',
                                    right_on='store_fk')
        for sub in SUB:
            if sub in (SUB.keys()):
                self.scif = self.scif.rename(columns={sub: SUB.get(sub)})

        for title in TITLE:
            if title in (self.scif.columns.unique().tolist()):
                self.scif = self.scif.rename(columns={title: TITLE.get(title)})
        # self.generaltoolbox = KCUS_SANDGENERALToolBox(data_provider, output, geometric_kpi_flag=True)
        # self.scif = self.scif.replace(' ', '', regex=True)
        self.set_name = set_name
        self.kpi_fetcher = KCUS_SANDFetcher(self.project_name, self.scif, self.match_product_in_scene,
                                            self.set_name, self.products, self.session_uid)
        self.all_template_data = parse_template(TEMPLATE_PATH, "Simple KPI's")
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.region = self.data_provider[Data.STORE_INFO]['region_name'].iloc[0]
        self.thresholds_and_results = {}
        self.result_df = []
        self.writing_to_db_time = datetime.timedelta(0)
        self.kpi_results_queries = []
        self.potential_products = {}
        self.shelf_square_boundaries = {}
        self.average_shelf_values = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.max_shelf_of_bay = []
        self.INCLUDE_FILTER = 1
        self.MM_TO_FEET_CONVERSION = MM_TO_FEET_CONVERSION


    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """

        kpi_set_fk = 1
        set_name = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
        template_data = self.all_template_data.loc[self.all_template_data['KPI Level 1 Name'] == set_name]

        try:
            if set_name and not set(template_data['Scene Types to Include'].values[0].encode().split(', ')) & set(
                    self.scif['template_name'].unique().tolist()):
                Log.info('Category {} was not captured'.format(template_data['category'].values[0]))
                return
        except Exception as e:
            Log.info('KPI Set {} is not defined in the template'.format(set_name))
        # for kpi_name in kpi_list:
        for i, row in template_data.iterrows():
            try:
                kpi_name = row['KPI Level 2 Name']
                if kpi_name in KPI_LEVEL_2_cat_space:
                    scene_type = [s for s in row['Scene_Type'].encode().split(', ')]
                    kpi_type = row['KPI_Type']


                    if row['Param1'] == 'Category' or  'sub_category':
                        category = row['Value1']

                        if kpi_type == 'category space':
                            self.calculate_category_space(kpi_set_fk, kpi_name, scene_type, category)

            except Exception as e:
                Log.info('KPI {} calculation failed due to {}'.format(kpi_name.encode('utf-8'), e))
                continue
        return

    def calculate_category_space(self, kpi_set_fk, kpi_name, scene_types, category):
        template = self.all_template_data.loc[(self.all_template_data['KPI Level 2 Name'] == kpi_name) &
                                                (self.all_template_data['Value1'] == category)]
        kpi_template = template.loc[template['KPI Level 2 Name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        values_to_check = []
        secondary_values_to_check = []

        filters = {'template_name': scene_types, 'category': kpi_template['Value1']}


        if kpi_template['Value1']:
            values_to_check = self.all_products.loc[self.all_products['category'] == kpi_template['Value1']]['category'].unique().tolist()

        if kpi_template['Value2']:
            if kpi_template['Value2'] == 'Feminine Needs':
                sub_category_att = 'FEM NEEDS'
                secondary_values_to_check = self.all_products.loc[self.all_products['category'] == kpi_template['Value1']][
                sub_category_att].unique().tolist()

            elif kpi_template['Value2'] == 'Feminine Hygiene':
                sub_category_att = 'FEM HYGINE'
                secondary_values_to_check = self.all_products.loc[self.all_products['category'] == kpi_template['Value1']][
                sub_category_att].unique().tolist()
                secondary_values_to_check[secondary_values_to_check.index('Feminine Hygine')] = 'Feminine Hygiene'


        for primary_filter in values_to_check:
            filters[kpi_template['Param1']] = primary_filter #value1?
            if secondary_values_to_check:
                for secondary_filter in secondary_values_to_check:
                    if secondary_filter == None:
                        continue

                    filters['template_name'] = secondary_filter
                    new_kpi_name = self.kpi_name_builder(kpi_name, **filters)

                    result = self.calculate_category_space_length(new_kpi_name,
                                                                 **filters)
                    filters['category'] = kpi_template['KPI Level 2 Name']
                    score = result * self.MM_TO_FEET_CONVERSION
                    # score = result
                    self.write_to_db_result(kpi_set_fk, score, self.LEVEL3, kpi_name=new_kpi_name, score=score)
            else:
                new_kpi_name = self.kpi_name_builder(kpi_name, **filters)


                result = self.calculate_category_space_length(new_kpi_name,
                                                             **filters)
                filters['Category'] = kpi_template['KPI Level 2 Name']
                score = result * self.MM_TO_FEET_CONVERSION

                self.write_to_db_result(kpi_set_fk, score, self.LEVEL3, kpi_name=new_kpi_name, score=score)




    def calculate_category_space_length(self, kpi_name, threshold=0.5,  **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """

        try:
            filtered_scif = self.scif[
                self.get_filter_condition(self.scif, **filters)]
            space_length = 0
            bay_values = []
            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.get_filter_condition(scene_matches, **scene_filters)]

                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()


                    if tested_group_linear_value:
                        bay_ratio = bay_total_linear / float(tested_group_linear_value)
                    else:
                        bay_ratio = 0
                    if bay_ratio >= threshold:
                        bay_num_of_shelves = len(scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                                   (scene_matches['stacking_layer'] == 1)][
                                                     'shelf_number'].unique().tolist())
                        if kpi_name not in self.average_shelf_values.keys():
                            self.average_shelf_values[kpi_name] = {'num_of_shelves': bay_num_of_shelves,
                                                                   'num_of_bays': 1}
                        else:
                            self.average_shelf_values[kpi_name]['num_of_shelves'] += bay_num_of_shelves
                            self.average_shelf_values[kpi_name]['num_of_bays'] += 1
                        if bay_num_of_shelves:
                            bay_final_linear_value = tested_group_linear_value / float(bay_num_of_shelves)
                        else:
                            bay_final_linear_value = 0
                        bay_values.append(bay_final_linear_value)
                        space_length += bay_final_linear_value
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



    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = KCUS_SANDFetcher.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data




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







    def write_to_db_result(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None, kpi_fk=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(kpi_set_fk, result=result, level=level, score=score,
                                                 threshold=threshold,
                                                 kpi_name=kpi_name, kpi_fk=kpi_fk)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None, kpi_fk=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]

            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, kpi_set_fk,)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == kpi_fk]['kpi_name'].values[0].replace("'",
                                                                                                                    "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            kpi_set_name = \
                self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            try:
                atomic_kpi_fk = \
                    self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
                kpi_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == atomic_kpi_fk]['kpi_fk'].values[
                    0]
            except Exception as e:
                atomic_kpi_fk = None
                kpi_fk = None
            kpi_name = kpi_name.replace("'", "\\'")
            attributes = pd.DataFrame([(kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                        result, kpi_fk, atomic_kpi_fk, threshold, score)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                               'visit_date',
                                               'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                               'threshold',
                                               'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = KCUS_SANDFetcher.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        # for query in self.kpi_results_queries:
        #     try:
        #         cur.execute(query)
        #     except Exception as e:
        #         Log.info('Query {} failed due to {}'.format(query, e))
        #         continue
        queries = self.merge_insert_queries(self.kpi_results_queries)
        for query in queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def merge_insert_queries(self, insert_queries):
        # other_queries = []
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
            else:
                static_data, inserted_data = query.split('VALUES ')
                if static_data not in query_groups:
                    query_groups[static_data] = []
                query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))

        return merged_queries


