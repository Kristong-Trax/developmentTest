
import pandas as pd
import os
import numpy as np
from datetime import datetime
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Projects.PNGCN_PROD.Fetcher import PNGQueries
from Projects.PNGCN_PROD.ShareOfDisplay.Calculation import calculate_share_of_display

__author__ = 'nimrodp'


PRIMARY_SHELF = 'Primary Shelf'
RELEVANT_CATEGORIES = [4, 5, 6, 7, 8, 9, 10, 11, 13]
PNG_MANUFACTURER_FK = 4
SUB_CATEGORY_SETS_SUFFIX = '_SCat'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
DISPLAY_COUNT_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'display_to_count.xlsx')
TOTAL_DISPLAY_COUNT = 'Total_display_count'
KPS_WITH_SUB_CATEGORY = 'Fem'

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


class PNGToolBox:

    IRRELEVANT = 'Irrelevant'
    EMPTY = 'Empty'

    LEFT = -1
    RIGHT = 1

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.matches = self.get_match_product_in_scene()
        self.kpi_static_data = self.get_kpi_static_data()
        self.session_categories_data = self.get_session_categories_data()
        self.kpi_results_queries = []
        self.empty_spaces = {self.get_category_name(category): {} for category in RELEVANT_CATEGORIES
                             if self.check_validation_of_category(category)}
        self.irrelevant_empties = 0
        self.common = Common(self.data_provider)
        # all_products = self.data_provider._static_data_provider.all_products.where(
        #     (pd.notnull(self.data_provider._static_data_provider.all_products)), None)
        # self.data_provider._set_all_products(all_products)
        # self.data_provider._init_session_data(None, True)
        # self.data_provider._init_report_data(self.data_provider.session_uid)
        # self.data_provider._init_reporting_data(self.data_provider.session_id)

    def get_category_name(self, category_fk):
        return self.all_products[self.all_products['category_fk'] == category_fk]['category_local_name'].values[0]

    def get_match_product_in_scene(self):
        query = """
                select ms.pk as scene_match_fk, ms.n_shelf_items
                from probedata.match_product_in_scene ms
                join probedata.scene s on s.pk = ms.scene_fk
                where s.session_uid = '{}'""".format(self.session_uid)
        matches = pd.read_sql_query(query, self.rds_conn.db)
        matches = matches.merge(self.data_provider[Data.MATCHES], how='left', on='scene_match_fk', suffixes=['', '_1'])
        matches = matches.merge(self.all_products, how='left', on='product_fk', suffixes=['', '_2'])
        matches = matches.merge(self.scif[['product_fk', 'scene_fk', 'rlv_sos_sc', 'location_type']],
                                how='left', on=['product_fk', 'scene_fk'], suffixes=['', '_3'])
        matches = matches[(matches['stacking_layer'] == 1) & (matches['location_type'] == PRIMARY_SHELF)]
        return matches

    def check_validation_of_session(self):
        relevant_scenes = self.scif[self.scif['location_type'] == PRIMARY_SHELF]
        if len(relevant_scenes):
            return True
        else:
            return False

    def check_validation_of_category(self, category):
        # checking facings
        relevant_facings = self.scif[(self.scif['category_fk'] == category) &
                                     (~self.scif['product_type'].isin(['Irrelevant'])) &
                                     (self.scif['rlv_sos_sc'] == 1) &
                                     (self.scif['location_type'] == PRIMARY_SHELF)]
        if relevant_facings['facings'].sum() > 0:
            return True
            # if self.session_categories_data.empty:
            #     return True
            # # checking if category is not excluded in this session
            # statuses_to_include = [1, 4]
            # filtered_df = self.session_categories_data[(self.session_categories_data['category'] == category) &
            #                                            (self.session_categories_data['status'].isin(statuses_to_include))]
            # if not filtered_df.empty:
            #     return True
        return False

    def get_kpi_static_data(self):
        query = PNGQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_session_categories_data(self):
        query = PNGQueries.get_session_categories_query().format(self.session_uid)
        session_categories_data = pd.read_sql_query(query, self.rds_conn.db)
        return session_categories_data

    def calculate_share_of_display(self):
        calculate_share_of_display(self.rds_conn, self.session_uid, self.data_provider)

    def handle_db_total_number_of_display(self, display_counter):
        """
        This function handles writing to DB for KPI_SET TOTAL_DISPLAY_COUNT.
        """
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == TOTAL_DISPLAY_COUNT]['kpi_set_fk'].iloc[0]
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == TOTAL_DISPLAY_COUNT]['kpi_fk'].iloc[0]
        atomic_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == TOTAL_DISPLAY_COUNT]['atomic_kpi_fk'].iloc[0]
        self.write_to_db_result(set_fk, display_counter, self.LEVEL1)
        self.write_to_db_result(kpi_fk, display_counter, self.LEVEL2)
        self.write_to_db_result(atomic_fk, display_counter, self.LEVEL3)

    def calculate_total_number_of_display(self):
        """
        This function calculates the total number of display per session.
        It checks Data/display_to_count.xlsx and counts according to it each display that defined 'isPhysical'.
        """
        display_counter = 0
        query = PNGQueries.get_display_count_per_name().format(self.session_uid)
        total_display_count = pd.read_sql_query(query, self.rds_conn.db)
        display_template = pd.read_excel(DISPLAY_COUNT_PATH)
        for i in xrange(len(total_display_count)):
            row = total_display_count.iloc[i]
            try:
                count = display_template.loc[display_template['name'] == row['display_name']]['count'].values[0]
                display_counter += count*row['display_count']
            except IndexError:
                Log.warning('The display:{} does not exist in the template.'.format(row['display_name']))
        self.handle_db_total_number_of_display(display_counter)


    def main_calculation(self):
        self.calculate_total_number_of_display()
        self.calculate_empty_spaces()
        relevant_facings = self.scif[(~self.scif['product_type'].isin([self.IRRELEVANT, self.EMPTY])) &
                                     (self.scif['rlv_sos_sc'] == 1) &
                                     (self.scif['location_type'] == PRIMARY_SHELF) &
                                     (self.scif['facings'] > 0)]
        relevant_facings = relevant_facings.fillna("")
        sets_to_save = set()
        for category in self.empty_spaces.keys():
            # if np.nan in self.empty_spaces[category]:
            #     self.empty_spaces[category][None] = self.empty_spaces[category][np.nan]
            #     del self.empty_spaces[category][np.nan]
            for sub_category in self.empty_spaces[category].keys():
                kpi_dict = {}
                empty_spaces = self.empty_spaces[category][sub_category]
                main_category = True
                category_facings = relevant_facings[relevant_facings['category_local_name'].str.encode("utf8") == category.encode("utf8")]
                if sub_category != category:
                    main_category = False
                    if sub_category == '{} Other'.format(category.split('_')[0]):
                        category_facings = category_facings[~category_facings['sub_category'].apply(bool)]
                    else:
                        category_facings = category_facings[category_facings['sub_category'].str.encode("utf8") == sub_category.encode("utf8")]
                kpi_dict['PNG_Empty'] = empty_spaces['png']
                kpi_dict['Total_Empty'] = empty_spaces['png'] + empty_spaces['other']
                png_facings = category_facings[category_facings['manufacturer_fk'] == PNG_MANUFACTURER_FK]
                kpi_dict['PG_non-Empty_Facing'] = png_facings['facings'].sum()
                kpi_dict['Total_non-Empty_Facing'] = category_facings['facings'].sum()
                png_facings_with_empty = kpi_dict['PG_non-Empty_Facing'] + empty_spaces['png']
                if png_facings_with_empty > 0:
                    kpi_dict['PNG_Empty_Rate%'] = (empty_spaces['png'] / float(png_facings_with_empty)) * 100
                else:
                    kpi_dict['PNG_Empty_Rate%'] = 0
                total_facings_with_empty = kpi_dict['Total_non-Empty_Facing'] + kpi_dict['Total_Empty']
                if total_facings_with_empty > 0:
                    kpi_dict['Category_Empty_Rate%'] = (kpi_dict['Total_Empty'] / float(total_facings_with_empty)) * 100
                else:
                    kpi_dict['Category_Empty_Rate%'] = 0

                if not main_category:
                    sub_category_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'].str.encode("utf8")
                                                              == (category + SUB_CATEGORY_SETS_SUFFIX).encode("utf8")) &
                                                             (self.kpi_static_data['kpi_name'].str.encode("utf8")
                                                              == sub_category.encode("utf8"))]
                    if sub_category_data.empty:
                        self.insert_sub_category_kpi(category, sub_category)
                        sub_category_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'].str.encode("utf8") ==
                                                            (category + SUB_CATEGORY_SETS_SUFFIX).encode("utf8")) &
                                                            (self.kpi_static_data['kpi_name'].str.encode("utf8")
                                                             == sub_category.encode("utf8"))]
                    sets_to_save.add(sub_category_data['kpi_set_fk'].values[0])
                    kpi_fk = sub_category_data['kpi_fk'].values[0]
                    self.write_to_db_result(kpi_fk, 100, self.LEVEL2)
                    for kpi in kpi_dict.keys():
                        atomic_kpi_fk = sub_category_data[sub_category_data['atomic_kpi_name'] ==
                                                          kpi]['atomic_kpi_fk'].values[0]
                        # if KPS_WITH_SUB_CATEGORY in str(sub_category_data[sub_category_data['atomic_kpi_name'] ==
                        #                                   kpi]['kpi_set_name']):
                        self.write_to_db_result(atomic_kpi_fk, kpi_dict[kpi], self.LEVEL3, (sub_category).encode('utf-8').strip())
                        # else:
                        #     self.write_to_db_result(atomic_kpi_fk, kpi_dict[kpi], self.LEVEL3)
                else:
                    category_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'].str.encode("utf8")
                                                         == category.encode("utf8")]
                    sets_to_save.add(category_data['kpi_set_fk'].values[0])
                    for kpi in kpi_dict:
                        kpi_fk = category_data[category_data['kpi_name'] == kpi]['kpi_fk'].values[0]
                        atomic_kpi_fk = category_data[category_data['atomic_kpi_name'] == kpi]['atomic_kpi_fk'].values[0]
                        self.write_to_db_result(kpi_fk, kpi_dict[kpi], self.LEVEL2)
                        # if KPS_WITH_SUB_CATEGORY in str(category_data[category_data['kpi_name'] == kpi]['kpi_set_name']):
                        self.write_to_db_result(atomic_kpi_fk, kpi_dict[kpi], self.LEVEL3, (sub_category).encode('utf-8').strip())
                        # else :
                        #     self.write_to_db_result(atomic_kpi_fk, kpi_dict[kpi], self.LEVEL3)
        for set_fk in sets_to_save:
            self.write_to_db_result(set_fk, 100, self.LEVEL1)

    def calculate_empty_spaces(self):
        empty_facings = self.matches[self.matches['product_type'] == 'Empty']
        if empty_facings.empty:
            Log.info('No empty spaces appeared in this visit')
        else:
            empty_sequences = self.get_empty_sequences(empty_facings)
            for sequence in empty_sequences:
                start_facing, end_facing, sequence = sequence
                anchor_for_all_empties = None
                if start_facing is None and end_facing is None:
                    continue
                elif start_facing is None:
                    anchor_for_all_empties = end_facing
                elif end_facing is None:
                    anchor_for_all_empties = start_facing
                if anchor_for_all_empties is not None:
                    self.update_empty_spaces(anchor_for_all_empties, len(sequence))
                else:
                    if len(sequence) % 2 == 0:
                        self.update_empty_spaces(start_facing, number_of_empties=(len(sequence) / 2))
                        self.update_empty_spaces(end_facing, number_of_empties=(len(sequence) / 2))
                    else:
                        self.update_empty_spaces(start_facing, number_of_empties=(len(sequence) / 2 + 1))
                        self.update_empty_spaces(end_facing, number_of_empties=(len(sequence) / 2))

    def update_empty_spaces(self, anchor_facing, number_of_empties):
        if anchor_facing['product_type'] != self.IRRELEVANT and anchor_facing['rlv_sos_sc'] == 1:
            anchor_category = anchor_facing['category_local_name']
            anchor_sub_category = anchor_facing['sub_category']
            if not anchor_sub_category or np.nan in [anchor_sub_category]:
                anchor_sub_category = '{} Other'.format(anchor_category.split('_')[0])
            anchor_manufacturer = anchor_facing['manufacturer_fk']
            if anchor_category in self.empty_spaces.keys():
                if anchor_category not in self.empty_spaces[anchor_category].keys():
                    self.empty_spaces[anchor_category][anchor_category] = {'png': 0, 'other': 0}
                if anchor_sub_category not in self.empty_spaces[anchor_category].keys():
                    self.empty_spaces[anchor_category][anchor_sub_category] = {'png': 0, 'other': 0}
                if anchor_manufacturer == PNG_MANUFACTURER_FK:
                    self.empty_spaces[anchor_category][anchor_category]['png'] += number_of_empties
                    if anchor_sub_category:
                        self.empty_spaces[anchor_category][anchor_sub_category]['png'] += number_of_empties
                else:
                    self.empty_spaces[anchor_category][anchor_category]['other'] += number_of_empties
                    if anchor_sub_category:
                        self.empty_spaces[anchor_category][anchor_sub_category]['other'] += number_of_empties
        else:
            self.irrelevant_empties += number_of_empties

    def get_empty_sequences(self, empty_facings):
        empty_sequences = []
        for scene in empty_facings['scene_fk'].unique():
            matches_for_scene = self.matches[self.matches['scene_fk'] == scene]
            matches_for_scene.merge(self.all_products[['product_fk', 'category_local_name']], how='inner', on=['product_fk'])
            empties_for_scene = empty_facings[empty_facings['scene_fk'] == scene]
            calculate_per_bay = self.get_scene_bay_status(matches_for_scene)
            empty_facings_already_calculated = []
            for f in xrange(len(empties_for_scene)):
                facing = empties_for_scene.iloc[f]
                if facing['scene_match_fk'] not in empty_facings_already_calculated:
                    start_facing = end_facing = None
                    empty_sequence = [facing]
                    left_facing = self.get_neighbour_facing(facing, matches_for_scene, self.LEFT, calculate_per_bay)
                    if left_facing is not None:
                        if left_facing['product_type'] == self.EMPTY:
                            continue
                        start_facing = left_facing
                    right_facing = self.get_neighbour_facing(facing, matches_for_scene, self.RIGHT, calculate_per_bay)
                    while right_facing is not None and right_facing['product_type'] == self.EMPTY:
                        empty_sequence.append(right_facing)
                        empty_facings_already_calculated.append(right_facing['scene_match_fk'])
                        right_facing = self.get_neighbour_facing(right_facing, matches_for_scene, self.RIGHT, calculate_per_bay)
                    if right_facing is not None:
                        end_facing = right_facing
                    empty_sequences.append((start_facing, end_facing, empty_sequence))
        return empty_sequences

    @staticmethod
    def get_neighbour_facing(anchor, matches, left_or_right=LEFT, calculate_per_bay=False):
        anchor_bay = anchor['bay_number']
        anchor_shelf = anchor['shelf_number']
        anchor_facing = anchor['facing_sequence_number']
        if 0 < (anchor_facing + left_or_right) <= anchor['n_shelf_items']:
            neighbour_facing = matches[(matches['bay_number'] == anchor_bay) &
                                       (matches['shelf_number'] == anchor_shelf) &
                                       (matches['facing_sequence_number'] == anchor_facing + left_or_right)]
        else:
            if calculate_per_bay:
                neighbour_facing = None
            else:
                if anchor_facing + left_or_right == 0:
                    if anchor_bay == 1:
                        neighbour_facing = None
                    else:
                        right_bay = matches[(matches['bay_number'] == anchor_bay - 1) &
                                            (matches['shelf_number'] == anchor_shelf)]
                        if not right_bay.empty:
                            max_facing = right_bay['n_shelf_items'].values[0]
                            neighbour_facing = right_bay[right_bay['facing_sequence_number'] == max_facing]
                        else:
                            neighbour_facing = None
                else:
                    if anchor_bay == matches['bay_number'].max():
                        neighbour_facing = None
                    else:
                        neighbour_facing = matches[(matches['bay_number'] == anchor_bay + 1) &
                                                   (matches['shelf_number'] == anchor_shelf) &
                                                   (matches['facing_sequence_number'] == 1)]

        if neighbour_facing is not None and not neighbour_facing.empty:
            neighbour_facing = neighbour_facing.iloc[0]
        else:
            neighbour_facing = None
        return neighbour_facing

    @staticmethod
    def get_scene_bay_status(matches_for_scene):
        number_of_shelves = None
        for bay in matches_for_scene['bay_number'].unique():
            max_shelf = matches_for_scene[matches_for_scene['bay_number'] == bay]['shelf_number'].max()
            if number_of_shelves is not None and max_shelf != number_of_shelves:
                return True
            number_of_shelves = max_shelf
        return False

    def write_to_db_result(self, fk, score, level ,sub_cat=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        if sub_cat:
            attributes = self.create_attributes_dict(fk, score, level,sub_cat)
        else:
            attributes = self.create_attributes_dict(fk, score, level)

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

    def create_attributes_dict(self, fk, score, level,sub_cat=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        score = round(score, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            if sub_cat:
                atomic_kpi_name = (atomic_kpi_name +" " +sub_cat.decode('utf-8')).encode('utf-8')
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, None,score)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time','score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def insert_sub_category_kpi(self, category, sub_category):
        set_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'].str.encode("utf8)") == (category + SUB_CATEGORY_SETS_SUFFIX).encode("utf8")]
        if sub_category not in set_data['kpi_name'].tolist():
            cur = self.rds_conn.db.cursor()
            kpi_query = PNGQueries.get_insert_kpi_query(set_data['kpi_set_fk'].values[0], sub_category)
            cur.execute(kpi_query)
            atomic_queries = PNGQueries.get_insert_atomic_query(cur.lastrowid)
            for query in atomic_queries:
                cur.execute(query)
            self.rds_conn.db.commit()
            Log.info(u"Sub category '{}' KPIs were inserted to the DB under '{}'".format(sub_category, category))
            self.kpi_static_data = self.get_kpi_static_data()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
