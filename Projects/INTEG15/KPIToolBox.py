# coding=utf-8
import numpy
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.INTEG15.Fetcher import INTEG15Queries

__author__ = 'ortalk'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

RELEVANT_TEMPLATES = [38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                      50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
                      62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73,
                      74, 75, 76, 77, 89, 90, 91, 92, 93, 94, 95, 96,
                      97, 98, 99, 100, 101, 102, 103, 104]


class INTEG15ToolBox:
    def __init__(self, data_provider, output, project_name):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.project_name = project_name
        self.data_provider = data_provider
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()

    def check_validation_of_session(self):
        relevant_scenes = self.scene_info[self.scene_info['template_fk'].isin(RELEVANT_TEMPLATES)]
        if len(relevant_scenes):
            return True
        else:
            return False

    def get_kpi_static_data(self):
        query = INTEG15Queries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def delete_session_results(self):
        cur = self.rds_conn.db.cursor()
        queries = INTEG15Queries.get_delete_session_results_query(self.session_uid)
        for query in queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    def check_empty_spaces(self, params):
        # Log.info('Start calc')
        # query = Queries.get_session_facings().format(self.session_uid, params)
        # all_facings = pd.read_sql_query(query, self.rds_conn.db)
        all_facings = self.scif[(self.scif['category_fk'] == params) &
                                (~self.scif['product_type'].isin(['Irrelevant'])) &
                                (self.scif['template_fk'].isin(RELEVANT_TEMPLATES))]
        matches_1 = self.match_product_in_scene.sort_values(by=['bay_number', 'shelf_number', 'facing_sequence_number'])
        matches_2 = matches_1.merge(self.products, how='left', on='product_fk')
        matches_3 = matches_2.loc[matches_2['status'] == 1]
        matches = matches_3.loc[matches_3['stacking_layer'] == 1]
        self.scores = pd.DataFrame([(0, 0, 0, 0, 0, 0, 0, 0)], columns=['PNG_Empty', 'irrelevant_Empty', 'Total_Empty',
                                                                        'PNG_Total_Facing', 'Total_Facing',
                                                                        'PNG_Empty_Rate%', 'Category_Empty_Rate%',
                                                                        'other_Empty'])

        # for scene in matches['scene_fk'].unique().tolist():
        for scene in list(all_facings['scene_id'].unique()):
            temp = matches.loc[matches['scene_fk'] == scene]
            is_empty = temp['product_type'].values.tolist()
            if 'Empty' in is_empty:
                bays = temp['bay_number'].unique().tolist()
                counts = set()
                per_bay = False
                for bay in bays:
                    shelf_for_bays = temp.loc[temp['bay_number'] == bay]
                    counts.add(shelf_for_bays['shelf_number'].max())
                    if len(counts) > 1:
                        per_bay = True
                        break
                if per_bay:
                    for bay in bays:
                        bay_data = temp.loc[temp['bay_number'] == bay]
                        is_empty_bay = bay_data['product_type'].values.tolist()
                        if 'Empty' in is_empty_bay:
                            shelf_for_bays = temp.loc[temp['bay_number'] == bay]
                            self.check_shelf(shelf_for_bays, params)
                else:
                    bay_data = temp.loc[temp['bay_number'].isin(bays)]
                    is_empty_bays = bay_data['product_type'].values.tolist()
                    if 'Empty' in is_empty_bays:
                        self.check_shelf(temp, params)

        self.scores['Total_Empty'] = self.scores['other_Empty'] + self.scores['PNG_Empty']
        self.scores['PNG_Total_Facing'] = all_facings[all_facings['manufacturer_fk'] == 4]['facings'].sum()
        self.scores['Total_Facing'] = all_facings['facings'].sum()
        png_facings_with_empty = float(self.scores['PNG_Total_Facing'] + self.scores['PNG_Empty'])
        if png_facings_with_empty:
            self.scores['PNG_Empty_Rate%'] = (self.scores['PNG_Empty'] / png_facings_with_empty) * 100
        else:
            self.scores['PNG_Empty_Rate%'] = 0
        total_facings_with_empty = float(self.scores['Total_Facing'] + self.scores['Total_Empty'])
        if total_facings_with_empty:
            self.scores['Category_Empty_Rate%'] = (self.scores['Total_Empty'] / total_facings_with_empty) * 100
        else:
            self.scores['Category_Empty_Rate%'] = 0
        if self.scores['Category_Empty_Rate%'][0] == 1:
            self.scores['Category_Empty_Rate%'] = 100
        self.scores = self.scores.drop('irrelevant_Empty', 1)
        self.scores = self.scores.drop('other_Empty', 1)

    def check_shelf(self, temp, params):
        shelves = temp['shelf_number'].unique().tolist()
        for shelf_number in shelves:
            self.place_count = 0
            temp2 = temp.loc[temp['shelf_number'] == shelf_number]
            is_empty = temp2['product_type'].values.tolist()
            if 'Empty' in is_empty:
                temp3 = temp2.reset_index()
                for i in temp3.index:
                    if (i == 0) or (i > self.place_count):
                        match = temp3.loc[[i]]
                        if match['product_type'][i] == 'Empty':
                            self.place_count = i
                            self.empty_count = 1
                            self.check_next_empty(temp3, i)
                            if self.empty_count > 1:
                                self.add_empties(temp3, i, params)
                            else:
                                self.add_empties(temp3, i, params, one_empty=True)

    def check_next_empty(self, temp3, i):
        for j in xrange(i + 1, len(temp3.index)):
            match = temp3.loc[[j]]
            if type(match['product_type'][j]) == unicode:
                if match['product_type'][j].encode() == 'Empty':
                    self.empty_count += 1
                    self.place_count += 1
                else:
                    break
            else:
                break

    def add_empties(self, temp3, i, cat, one_empty=False):
        if i == 0 and not one_empty:
            if temp3.index.max() == self.place_count:
                pass
            else:
                row_right_cat = int(numpy.nan_to_num(temp3[self.place_count + 1:self.place_count + 2]['category_fk'].values))
                manufacturer_row_right_cat = \
                    int(numpy.nan_to_num(temp3[self.place_count + 1:self.place_count + 2]['manufacturer_fk'].values))
                if row_right_cat == cat:
                    if manufacturer_row_right_cat == 4:
                        self.scores['PNG_Empty'] += self.empty_count
                    else:
                        self.scores['other_Empty'] += self.empty_count
                elif row_right_cat == 'irrelevant':
                    self.scores['irrelevant_Empty'] += self.empty_count
        elif (i == temp3.index.max() or self.place_count == temp3.index.max() or
                          self.place_count + 1 > temp3.index.max()) and not one_empty:
            left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['category_fk'].values))
            manufacturer_left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['manufacturer_fk'].values))
            if left_cat == cat:
                if manufacturer_left_cat == 4:
                    self.scores['PNG_Empty'] += self.empty_count
                else:
                    self.scores['other_Empty'] += self.empty_count
            elif left_cat == 'irrelevant':
                self.scores['irrelevant_Empty'] += self.empty_count

        elif self.empty_count % 2 == 0 and not one_empty:
            left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['category_fk'].values))
            manufacturer_left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['manufacturer_fk'].values))
            if left_cat == cat:
                if manufacturer_left_cat == 4:
                    self.scores['PNG_Empty'] += (self.empty_count / 2)
                else:
                    self.scores['other_Empty'] += self.empty_count / 2
            elif left_cat == 'irrelevant':
                self.scores['irrelevant_Empty'] += self.empty_count / 2

            row_right_cat = int(numpy.nan_to_num(temp3[self.place_count + 1:self.place_count + 2]['category_fk'].values))
            manufacturer_row_right_cat = int(numpy.nan_to_num(
                temp3[self.place_count + 1:self.place_count + 2]['manufacturer_fk'].values))
            if row_right_cat == cat:
                if manufacturer_row_right_cat == 4:
                    self.scores['PNG_Empty'] += self.empty_count / 2
                else:
                    self.scores['other_Empty'] += self.empty_count / 2
            elif row_right_cat == 'irrelevant':
                self.scores['irrelevant_Empty'] += self.empty_count / 2

        elif self.empty_count % 2 != 0 and not one_empty:
            left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['category_fk'].values))
            manufacturer_left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['manufacturer_fk'].values))
            if left_cat == cat:
                if manufacturer_left_cat == 4:
                    self.scores['PNG_Empty'] += (self.empty_count + 1) / 2
                else:
                    self.scores['other_Empty'] += (self.empty_count + 1) / 2
            elif left_cat == 'irrelevant':
                self.scores['irrelevant_Empty'] += (self.empty_count + 1) / 2
            row_right_cat = int(numpy.nan_to_num(temp3[self.place_count + 1:self.place_count + 2]['category_fk'].values))
            manufacturer_row_right_cat = int(numpy.nan_to_num(
                temp3[self.place_count + 1:self.place_count + 2]['manufacturer_fk'].values))
            if row_right_cat == cat:
                if manufacturer_row_right_cat == 4:
                    self.scores['PNG_Empty'] += ((self.empty_count + 1) / 2) - 1
                else:
                    self.scores['other_Empty'] += ((self.empty_count + 1) / 2) - 1
            elif row_right_cat == 'irrelevant':
                self.scores['irrelevant_Empty'] += ((self.empty_count + 1) / 2) - 1
        elif one_empty:
            if i > 0:
                left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['category_fk'].values))
                manufacturer_left_cat = int(numpy.nan_to_num(temp3[i - 1:i]['manufacturer_fk'].values))
                if left_cat == cat:
                    if manufacturer_left_cat == 4:
                        self.scores['PNG_Empty'] += self.empty_count
                    else:
                        self.scores['other_Empty'] += self.empty_count
                elif left_cat == 'irrelevant':
                    self.scores['irrelevant_Empty'] += self.empty_count

            elif i == 0:
                if temp3.index.max() == i:
                    pass
                else:
                    manufacturer_right_cat = int(numpy.nan_to_num(temp3[i + 1:i + 2]['manufacturer_fk'].values))
                    right_cat = int(numpy.nan_to_num(temp3[i + 1:i + 2]['category_fk'].values))
                    if right_cat == cat:
                        if manufacturer_right_cat == 4:
                            self.scores['PNG_Empty'] += self.empty_count
                        else:
                            self.scores['other_Empty'] += self.empty_count
                    elif right_cat == 'irrelevant':
                        self.scores['irrelevant_Empty'] += self.empty_count

    def write_to_db_result(self, df=None, level=None, kps_name_temp=None):
        # temp = kps_name_temp.encode('utf-8')
        # kps_name = str(temp)
        if level == 'level3':
            # query = Queries.get_kpi_results_data().format(df['kpi_fk'][0])
            # level3 = pd.read_sql_query(query, self.rds_conn.db)
            atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_fk'] ==
                                                 df['kpi_fk'][0]]['atomic_kpi_fk'].values[0]
            df['atomic_kpi_fk'] = atomic_kpi_fk
            df['kpi_fk'] = df['kpi_fk'][0]
            df_dict = df.to_dict()
            query = insert(df_dict, KPI_RESULT)
            return query
        elif level == 'level2':
            # temp = df['kpk_name'][0].encode('utf-8')
            # kpi_name = str(temp)
            # query = Queries.get_kpk_results_data().format(kpi_name, kps_name)
            # level2 = pd.read_sql_query(query, self.rds_conn.db)
            kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == kps_name_temp) &
                                          (self.kpi_static_data['kpi_name'] == df['kpk_name'][0])]['kpi_fk'].values[0]
            df['kpi_fk'] = kpi_fk
            df_dict = df.to_dict()
            query = insert(df_dict, KPK_RESULT)
            return query
        elif level == 'level1':
            # query = Queries.get_kps_results_data().format(kps_name)
            # level1 = pd.read_sql_query(query, self.rds_conn.db)
            kpi_set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] ==
                                              kps_name_temp]['kpi_set_fk'].values[0]
            df['kpi_set_fk'] = kpi_set_fk
            df_dict = df.to_dict()
            query = insert(df_dict, KPS_RESULT)
            return query

    # def write_to_db_static(self, df=None, level=None, kps_name_temp=None):
    #     temp = kps_name_temp.encode('utf-8')
    #     kps_name = str(temp)
    #     if level == 'level3':
    #         query = Queries.get_kpi_results_data().format(df['kpi_fk'][0])
    #         level3 = pd.read_sql_query(query, self.rds_conn.db)
    #         df['atomic_kpi_fk'] = level3['atomic_kpi_fk']
    #         df['kpi_fk'] = df['kpi_fk'][0]
    #         df_dict = df.to_dict()
    #         query = insert(df_dict, KPI_RESULT)
    #         return query
    #     elif level == 'level2':
    #         temp = df['kpk_name'][0].encode('utf-8')
    #         kpi_name = str(temp)
    #         query = Queries.get_kpk_results_data().format(kpi_name, kps_name)
    #         level2 = pd.read_sql_query(query, self.rds_conn.db)
    #         df['kpi_fk'] = level2['kpi_fk']
    #         df_dict = df.to_dict()
    #         query = insert(df_dict, KPK_RESULT)
    #         return query
    #     elif level == 'level1':
    #         query = Queries.get_kps_results_data().format(kps_name)
    #         level1 = pd.read_sql_query(query, self.rds_conn.db)
    #         Log.info('start_updating_level1_result')
    #         df['kpi_set_fk'] = level1['pk']
    #         df_dict = df.to_dict()
    #         query = insert(df_dict, KPS_RESULT)
    #         return query

