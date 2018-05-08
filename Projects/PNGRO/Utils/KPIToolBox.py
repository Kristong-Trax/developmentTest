import os
from datetime import datetime

import pandas as pd
import numpy as np

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector

from Projects.PNGRO.Utils.Fetcher import PNGRO_PRODQueries
from Projects.PNGRO.Utils.GeneralToolBox import PNGRO_PRODGENERALToolBox
from Projects.PNGRO.Utils.ParseTemplates import parse_template

__author__ = 'Israel'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')
SBD_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'SBD_Template.xlsx')


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


class PNGRO_PRODToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    KPI_NAME = 'KPI name'
    SBD_KPI_NAME = 'KPI Name'
    KPI_TYPE = 'KPI Type'
    BRAND = 'Brand'
    FORM = 'Form'
    MANUFACTURER = 'Manufacturer'
    CATEGORY = 'Product Category'
    DISPLAY_NAME = 'display name'
    WEIGHTS = 'weight'
    DISPLAYS = 'display name'
    DISPLAYS_COUNT = 'display count'
    DISPLAYS_COUNT_BY_TYPE = 'display count by display type'
    SOD_BY_BRAND = 'share of display by brand'
    SOD_BY_MANUFACTURER = 'share of display by manufacturer'
    WEIGHT = 'weight'
    KPI_FAMILY = 'KPI Family'
    BLOCKED_TOGETHER = 'Blocked Together'
    SOS = 'SOS'
    RELATIVE_POSITION = 'Relative Position'
    AVAILABILITY = 'Availability'
    SHELF_POSITION = 'Shelf Position'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = PNGRO_PRODGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []

        self.kpis_data = parse_template(TEMPLATE_PATH, 'KPI display')
        self.display_data = parse_template(TEMPLATE_PATH, 'display weight')
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        self.sbd_kpis_data = parse_template(SBD_TEMPLATE_PATH, 'Sheet1', lower_headers_row_index=1)

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGRO_PRODQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = PNGRO_PRODQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_match_stores_by_retailer(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_match_stores_by_retailer()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_template_fk_by_category_fk(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from static.stores.
        """
        query = PNGRO_PRODQueries.get_template_fk_by_category_fk()
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_display(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_display(session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_status_session_by_category(self, session_uid):
        query = PNGRO_PRODQueries.get_status_session_by_category(session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        match_display = self.get_status_session_by_display(self.session_uid)
        if kwargs['kpi_set_fk'] == 1:
            if not match_display.empty:
                if match_display['exclude_status_fk'][0] in (1, 4):
                    secondary_shelfs = self.scif.loc[self.scif['template_name'] == 'Secondary shelf'][
                        'scene_id'].unique().tolist()
                    display_filter_from_scif = self.match_display_in_scene.loc[self.match_display_in_scene['scene_fk']
                        .isin(secondary_shelfs)]
                    display_filter_from_scif['count'] = 0
                    display_agg = display_filter_from_scif.groupby(['scene_fk', 'display_name'], as_index=False).agg(
                        {'count': np.size})

                    for x, params in self.kpis_data.iterrows():
                        score, result, result_2 = 0, 0.0, 0.0
                        kpi_type = params[self.KPI_TYPE]

                        if kpi_type == self.DISPLAYS_COUNT:
                            result = self.display_count(params, display_agg)
                        elif kpi_type == self.DISPLAYS_COUNT_BY_TYPE:
                            score, result, result_2 = self.display_count_by_display_type(params, display_agg)
                        elif kpi_type == self.SOD_BY_BRAND:
                            result = self.sod_by_brand(params, display_agg)
                        elif kpi_type == self.SOD_BY_MANUFACTURER:
                            result = self.sod_by_manufacturer(params, display_agg)

                        if result:

                            kpi_fk = self.get_kpi_fk_by_kpi_name(params[self.KPI_NAME])
                            try:
                                self.write_to_db_result(score=score, result=result, result_2=result_2, level=self.LEVEL3,
                                                        fk=kpi_fk)
                            except IndexError:
                                pass
        elif kwargs['kpi_set_fk'] == 2:
            category_status_ok = self.get_status_session_by_category(self.session_uid)['category_fk'].tolist()
            for x, params in self.sbd_kpis_data.iterrows():
                if self.check_if_blade_ok(params, match_display, category_status_ok):
                    general_filters = self.get_general_filters(params)
                    if general_filters:
                        score = 0
                        kpi_type = params[self.KPI_FAMILY]

                        if kpi_type == self.BLOCKED_TOGETHER:
                            score = self.block_together(params, **general_filters)
                        elif kpi_type == self.SOS:
                            score = self.sos(params, **general_filters)
                        elif kpi_type == self.RELATIVE_POSITION:
                            score = self.relative_position(params, **general_filters)
                        elif kpi_type == self.AVAILABILITY:
                            score = self.availability(params, **general_filters)
                        elif kpi_type == self.SHELF_POSITION:
                            score = self.shelf_position(params, **general_filters)

                        atomic_kpi_fk = self.get_kpi_fk_by_kpi_name(params[self.SBD_KPI_NAME])
                        if atomic_kpi_fk is not None:
                            self.write_to_db_result(score=int(score), result=int(score), level=self.LEVEL3,
                                                    fk=atomic_kpi_fk)

    def check_if_blade_ok(self, params, match_display, category_status_ok):
        if not params['Scene Category'].strip():
            if not match_display.empty:
                if match_display['exclude_status_fk'][0] in (1, 4):
                    return True
            else:
                return False
        elif int(float(params['Scene Category'])) in category_status_ok:
            return True
        else:
            return False

    def get_general_filters(self, params):
        template_name = params['Template Name']
        category = params['Scene Category']
        location_type = params['Location Type']
        retailer = params['by Retailer']

        if template_name.strip():
            relative_scenes = self.scif[
                (self.scif['template_name'] == template_name) & (self.scif['location_type'] == location_type)]
        elif category.strip():
            template_fk = self.match_template_fk_by_category_fk['pk'][
                self.match_template_fk_by_category_fk['product_category_fk'] == int(float(category))].unique().tolist()
            relative_scenes = self.scif[
                (self.scif['template_fk'].isin(template_fk)) & (self.scif['location_type'] == location_type)]
        else:
            relative_scenes = self.scif[(self.scif['location_type'] == location_type)]

        if retailer.strip():
            stores = self.match_stores_by_retailer['pk'][
                self.match_stores_by_retailer['name'] == retailer].unique().tolist()
            relative_scenes = relative_scenes[(relative_scenes['store_id'].isin(stores))]

        general_filters = {}
        if not relative_scenes.empty:
            general_filters['scene_id'] = relative_scenes['scene_id'].unique().tolist()

        return general_filters

    def block_together(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        score_pass = True

        for value in map(unicode.strip, params['Param (1) Values'].split(',')):
            if type3.strip():
                filters = {type1: value, type2: value2, type3: value3}
            else:
                filters = {type1: value, type2: value2}

            if score_pass:
                for scene in general_filters['scene_id']:
                    if score_pass:
                        score_pass = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.9,
                                                                         allowed_products_filters={
                                                                             'product_type': 'Other'},
                                                                         **dict(filters, **{'scene_id': scene}))
                    else:
                        return False
            else:
                return False
        return score_pass

    def sos(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = map(unicode.strip, params['Param (1) Values'].split(','))
        type2 = params['Param Type (2)/ Denominator']
        value2 = map(unicode.strip, params['Param (2) Values'].split(','))
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        target = params['Target Policy']

        numerator_filters = {type1: value1, type3: value3}
        denominator_filters = {type2: value2, type3: value3}

        numerator_width = self.tools.calculate_linear_share_of_display(numerator_filters,
                                                                       include_empty=True,
                                                                       **general_filters)
        denominator_width = self.tools.calculate_linear_share_of_display(denominator_filters,
                                                                         include_empty=True,
                                                                         **general_filters)

        if denominator_width == 0:
            ratio = 0
        else:
            ratio = numerator_width / float(denominator_width)
        if (ratio * 100) >= int(target):
            return True
        else:
            return False

    def relative_position(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = params['Param (1) Values']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']

        block_products1 = {type1: value1, type3: value3}
        block_products2 = {type2: value2, type3: value3}
        if type1 == type2:
            filters = {type1: [value1, value2], type3: value3}
        else:
            filters = {type1: value1, type2: value2, type3: value3}
        score = self.tools.calculate_block_together(include_empty=False, minimum_block_ratio=0.9,
                                                    allowed_products_filters={'product_type': 'Other'},
                                                    block_of_blocks=True, block_products1=block_products1,
                                                    block_products2=block_products2,
                                                    **dict(filters, **general_filters))
        return score

    def availability(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']

        for value in map(unicode.strip, params['Param (1) Values'].split(',')):
            filters = {type1: value, type2: value2, type3: value3}
            if self.tools.calculate_availability(**dict(filters, **general_filters)) > 0: return True
        return False

    def shelf_position(self, params, **general_filters):
        type1 = params['Param Type (1)/ Numerator']
        value1 = params['Param (1) Values']
        type2 = params['Param Type (2)/ Denominator']
        value2 = params['Param (2) Values']
        type3 = params['Param (3)']
        value3 = params['Param (3) Values']
        if type3.strip():
            filters = {type1: value1, type2: value2, type3: value3}
        else:
            filters = {type1: value1, type2: value2}
        target = params['Target Policy']
        target = map(int, target.split(','))
        product_fk_codes = self.scif[self.tools.get_filter_condition(self.scif,
                                                                     **dict(filters, **general_filters))][
            'product_fk'].unique().tolist()
        shelf_list = self.match_product_in_scene[self.tools.get_filter_condition(self.match_product_in_scene,
                                                                                 **dict({'product_fk': product_fk_codes,
                                                                                         'scene_fk': general_filters[
                                                                                             'scene_id']}))][
            'shelf_number'].unique()
        score = len(set(shelf_list) - set(target))
        if score > 0:
            return False
        else:
            return True

    def display_count(self, params, display_agg):
        score = 0.0
        if params[self.BRAND].strip() or params[self.CATEGORY].strip():
            if params[self.BRAND].strip():
                sos_filters = {'brand_name': params[self.BRAND]}
            else:
                sos_filters = {'category': params[self.CATEGORY]}
            if params[self.FORM].strip(): sos_filters['Form'] = params[self.FORM]

            for y, display in display_agg.iterrows():
                display_weight = self.get_display_weight_by_display_name(display['display_name'])
                display_count = display['count']
                general_filters = {'scene_id': display['scene_fk']}
                score += self.tools.calculate_linear_share_of_display(sos_filters,
                                                                      **general_filters) * display_weight * display_count
        return score

    def display_count_by_display_type(self, params, display_agg):
        score, display_weight, display_count = 0.0, 0, 0

        if params[self.BRAND].strip() or params[self.CATEGORY].strip():
            if params[self.BRAND].strip():
                sos_filters = {'brand_name': params[self.BRAND]}
            else:
                sos_filters = {'category': params[self.CATEGORY]}
            if params[self.FORM].strip(): sos_filters['Form'] = params[self.FORM]

            display = display_agg[display_agg['display_name'] == params[self.DISPLAY_NAME]]
            if not display.empty:
                for y, dis in display.iterrows():
                    display_weight = self.get_display_weight_by_display_name(dis['display_name'])
                    display_count += dis['count']
                    general_filters = {'scene_id': dis['scene_fk']}
                    score += self.tools.calculate_linear_share_of_display(sos_filters,
                                                                          **general_filters) * dis['count']
        return display_count, score, display_weight

    def sod_by_brand(self, params, display_agg):
        score = 0.0
        if params[self.BRAND].strip():
            numerator_filters = {'brand_name': params[self.BRAND]}
            denominator_filters = {}
            if params[self.FORM].strip():
                numerator_filters['Form'] = params[self.FORM]
                denominator_filters['Form'] = params[self.FORM]
                score = self.calculate_sod_by_filters(display_agg, numerator_filters, denominator_filters)
            elif params[self.CATEGORY].strip():
                denominator_filters['category'] = params[self.CATEGORY]
                numerator_filters['category'] = params[self.CATEGORY]
                score = self.calculate_sod_by_filters(display_agg, numerator_filters, denominator_filters)
        return score

    def sod_by_manufacturer(self, params, display_agg):
        score = 0.0
        if params[self.MANUFACTURER].strip() and params[self.CATEGORY].strip():
            numerator_filters = {'manufacturer_name': params[self.MANUFACTURER], 'category': params[self.CATEGORY]}
            denominator_filters = {'category': params[self.CATEGORY]}
            score = self.calculate_sod_by_filters(display_agg, numerator_filters, denominator_filters)
        return score

    def get_display_weight_by_display_name(self, display_name):
        assert isinstance(display_name, unicode), "name is not a string: %r" % display_name
        return float(
            self.display_data[self.display_data[self.DISPLAYS] == display_name][self.WEIGHT].values[0])

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        assert isinstance(kpi_name, unicode), "name is not a string: %r" % kpi_name
        try:
            return self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].values[0]
        except IndexError:
            Log.info('Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def calculate_sod_by_filters(self, displays, numerator_filters, denominator_filters):
        """
        :return: The Linear SOS ratio.
        """
        numerator_score = denominator_score = 0

        for y, display in displays.iterrows():
            display_weight = self.get_display_weight_by_display_name(display['display_name'])
            display_count = display['count']
            general_filters = {'scene_id': display['scene_fk']}

            numerator_width = self.tools.calculate_linear_share_of_display(numerator_filters, **general_filters)
            numerator_width *= (display_weight * display_count)
            numerator_score += numerator_width

            denominator_width = self.tools.calculate_linear_share_of_display(denominator_filters, **general_filters)
            denominator_width *= (display_weight * display_count)
            denominator_score += denominator_width

        if denominator_score == 0:
            ratio = 0
        else:
            ratio = numerator_score / float(denominator_score)
        return ratio

    def write_to_db_result(self, fk, level, score=None, result=None, result_2=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        # assert isinstance(fk, int), "fk is not a int: %r" % fk
        # assert isinstance(score, float), "score is not a float: %r" % score
        attributes = self.create_attributes_dict(fk, score, result, result_2, level)
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

    def create_attributes_dict(self, fk, score=None, result=None, result_2=None, level=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, result, result_2, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'result_2', 'kpi_fk',
                                               'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGRO_PRODQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
