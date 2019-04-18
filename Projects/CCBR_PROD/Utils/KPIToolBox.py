#!/usr/bin/env python
# -*- coding: utf-8 -*

import pandas as pd
import os

from datetime import datetime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.Calculations.Survey import Survey
from Projects.CCBR_PROD.Utils.Fetcher import CCBRQueries
from Projects.CCBR_PROD.Utils.GeneralToolBox import CCBRGENERALToolBox
from Projects.CCBR_PROD.Data.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider as PsDataProvider
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider as PsDataProvider_v2
from KPIUtils.DB.Common import Common

__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_NEW_TABLE = 'report.kpi_level_2_results'
PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Femsa template 2019 - KENGINE_DCH v8.0.xlsx')

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


class CCBRToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = CCBRGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.kpi_results_queries = []
        self.survey = Survey(self.data_provider, self.output)
        self.kpi_results_new_tables_queries = []
        self.New_kpi_static_data = self.get_new_kpi_static_data()
        self.session_id = self.data_provider.session_id
        self.prices_per_session = PsDataProvider(self.data_provider, self.output).get_price_union(self.session_id)
        self.survey_answers = PsDataProvider_v2(self.data_provider, self.output).get_result_values()
        self.common_db = Common(self.data_provider)
        self.count_sheet = pd.read_excel(PATH, Const.COUNT).fillna("")
        self.group_count_sheet = pd.read_excel(PATH, Const.GROUP_COUNT).fillna("")
        self.survey_sheet = pd.read_excel(PATH, Const.SURVEY).fillna("")

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpis_sheet = pd.read_excel(PATH, Const.KPIS).fillna("")
        for index, row in kpis_sheet.iterrows():
            self.handle_atomic(row)
        self.handle_simon_kpis()
        self.commit_results_data()

    def handle_simon_kpis(self):
        """
        activate the availability and pricing functions
        """
        active_products = self.all_products.loc[self.all_products["is_active"] > 0]
        self.calculate_availability(active_products)
        self.calculate_pricing(self.all_products)

    def calculate_availability(self, active_products):
        """
        calculates the availability for all products per session, used is sovi and sovi vertical reports
        :param active_products: a df containing only active products
        """
        active_products_sku_and_other = active_products[(active_products['product_type'] == 'SKU')
                                                                    | (active_products['product_type'] == 'Other')]
        active_products_pks = active_products_sku_and_other['product_fk'].unique().tolist()
        filters = {'product_fk': active_products_pks}
        filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        facing_filtered = filtered_df.loc[filtered_df['facings'] > 0][['template_fk','product_fk', 'facings']]
        facing_filtered_pks = facing_filtered['product_fk'].unique().tolist()
        for product in facing_filtered_pks:
            product_df = facing_filtered.loc[facing_filtered['product_fk'] == product]
            product_template_fks = product_df['template_fk'].unique().tolist()
            for template_fk in product_template_fks:
                sum_facing = product_df.loc[product_df['template_fk'] == template_fk]['facings'].sum()
                self.write_to_db_result_new_tables(fk=Const.AVAILABILITY_PK, numerator_id=product, score='1',
                                                   denominator_id=template_fk, numerator_result='1', result=sum_facing)

    def calculate_pricing(self, all_products):
        """
        inserting the db the pricing of all active and inactive skus.
        used in preco and preco vertical reports
        :param all_products: df containing all products
        """
        only_sku_type_products = all_products.loc[all_products['product_type'] == 'SKU']
        all_products_fks_size = only_sku_type_products[['product_fk', 'size']].fillna("")
        product_fks_and_prices = self.prices_per_session
        merge_size_and_price = pd.merge(all_products_fks_size, product_fks_and_prices, how='left', on='product_fk')
        merge_size_and_price['value'] = merge_size_and_price['value'].fillna('0')
        for row in merge_size_and_price.itertuples():
            product = row[1] # row['product_fk']
            size = row[2] # row['size']
            price = row[3] # row['value']
            if size == '':
                size = 0
            if price > 0:
                self.write_to_db_result_new_tables(fk=Const.PRICING_PK, numerator_id=product,
                                               numerator_result=size, result=price)

    def handle_atomic(self, row):
        """
        run the correct kpi for a specific row in the template
        :param row: a row from the template
        """
        atomic_name = row[Const.ENGLISH_KPI_NAME].strip()
        kpi_type = row[Const.KPI_TYPE].strip()
        if kpi_type == Const.SURVEY:
            self.handle_survey_atomics(atomic_name)
        elif kpi_type == Const.COUNT:
            self.handle_count_atomics(atomic_name)
        elif kpi_type == Const.GROUP_COUNT:
            self.handle_group_count_atomics(atomic_name)

    def handle_survey_atomics(self, atomic_name):
        """
        handle survey questions
        :param atomic_name: the name of the kpi
        :return: only if the survey filters aren't satisfied
        """
        row = self.survey_sheet.loc[
            self.survey_sheet[Const.ENGLISH_KPI_NAME].str.encode('utf8') == atomic_name.encode('utf8')]
        if row.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return
        store_type_filter = self.store_info['store_type'].values[0].strip()
        store_type_template = row[Const.STORE_TYPE_TEMPLATE].values[0].strip()

        # if cell in template is not empty
        if store_type_template != "":
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if store_type_filter not in store_types:
                return

        # find the answer to the  in session
        question_id = row[Const.SURVEY_QUESTION_ID].values[0]
        question_answer_template = row[Const.TARGET_ANSWER].values[0]

        survey_result = self.survey.get_survey_answer(('question_fk', question_id))
        if not survey_result:
            return
        if question_answer_template == Const.NUMERIC:
            if not isinstance(survey_result, (int, long, float)):
                Log.warning("question id " + str(question_id) + " in template is not a number")
                return
        elif question_answer_template == Const.STRING:
            possible_answers = self.survey_answers[self.survey_answers['kpi_result_type_fk']==2]
            answer_df = possible_answers[possible_answers['value']==survey_result]
            if answer_df.empty:
                survey_result = -1
            else:
                survey_result = answer_df.iloc[0]['pk']
        else:
            answer = self.survey.check_survey_answer(('question_fk', question_id), question_answer_template)
            survey_result = 1 if answer else -1

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id, numerator_result=survey_result,
                                           result=survey_result)

    def handle_count_atomics(self, atomic_name):
        """
        handle count kpis, used in consolidada report
        :param atomic_name: the name of the kpi to calculate
        """
        sum_of_count = 0
        target = 0
        count_result = 0
        row = self.count_sheet.loc[
            self.count_sheet[Const.ENGLISH_KPI_NAME].str.encode('utf8') == atomic_name.encode('utf8')]
        if row.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return
        for index, row in row.iterrows():
            sum_of_count, target, count_result = self.handle_count_row(row)
        if not isinstance(sum_of_count,(int, float, long)):
            sum_of_count = count_result

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=sum_of_count,
                                           denominator_result=target, result=count_result)

    def handle_group_count_atomics(self, atomic_name):
        """
        handle group count kpis (different from count in or and and conditions), used in consolidada report
        :param atomic_name: the name of the kpi to calculate
        """
        rows = self.group_count_sheet.loc[
            self.group_count_sheet[Const.GROUP_KPI_NAME].str.encode('utf8') == atomic_name.encode('utf8')]
        group_weight = 0
        group_result = 0
        group_target = 0
        group_sum_of_count = 0
        sum_of_count_df = pd.DataFrame()
        target_operator = ""
        if rows.empty:
            Log.warning("Dataframe is empty, wrong kpi name: " + atomic_name)
            return

        try:
            atomic_pk = self.common_db.get_kpi_fk_by_kpi_name_new_tables(atomic_name)
        except IndexError:
            Log.warning("There is no matching Kpi fk for kpi name: " + atomic_name)
            return

        for index, row in rows.iterrows():
            target_operator = row[Const.TARGET_OPERATOR].strip()
            weight = row[Const.WEIGHT]
            sum_of_count, target, count_result = self.handle_count_row(row)
            if count_result >= 1:
                group_weight += weight
                if group_weight >= 100:
                    # use for getting numeric results instead of 1 and 0
                    if (target_operator == '+'):
                        sum_of_count_df = pd.concat([sum_of_count_df,sum_of_count])
                    else:
                        group_result = 1
                        break

            # conditional, if given -1000 kpi must fail
            elif count_result == -1000:
                group_result = 0
                break

        # use for getting numeric results instead of 1 and 0
        if (target_operator == '+'):
            if sum_of_count_df.empty:
                group_sum_of_count = 0
            else:
                group_sum_of_count = len(sum_of_count_df.groupby('scene_id'))
            group_result = group_sum_of_count

        self.write_to_db_result_new_tables(fk=atomic_pk, numerator_id=self.session_id,
                                           numerator_result=group_sum_of_count,
                                           denominator_result=group_target, result=group_result)

    def handle_count_row(self, row):
        """
        filters qall params in aspecific row and send it to the correct count calculation
        :param row:
        :return:
        """
        count_type = row[Const.COUNT_TYPE].strip()
        target = row[Const.TARGET]
        target_operator = row[Const.TARGET_OPERATOR].strip()
        product_template = row[Const.PRODUCT]
        store_type_filter = self.store_info['store_type'].values[0]
        store_type_template = row[Const.STORE_TYPE_TEMPLATE]
        product_size = row[Const.PRODUCT_SIZE]
        product_size_operator = row[Const.PRODUCT_SIZE_OPERATOR].strip()
        product_measurement_unit = row[Const.MEASUREMENT_UNIT].strip()
        consider_few = row[Const.CONSIDER_FEW]
        multipack_template = row[Const.MULTIPACK].strip()
        multipack_df = None

        # filter store type
        if store_type_template != "":
            store_types = store_type_template.split(",")
            store_types = [item.strip() for item in store_types]
            if store_type_filter not in store_types:
                return 0,0,0

        filtered_df = self.scif.copy()

        # filter product
        if product_template != "":
            products_to_check = product_template.split(",")
            products_to_check = [item.strip() for item in products_to_check]
            filtered_df = filtered_df[filtered_df['product_name'].isin(products_to_check)]
            if filtered_df.empty:
                return 0,0,0

        # filter product size
        if product_size != "":
            if product_measurement_unit == 'l':
                product_size *= 1000

            ml_df = filtered_df[filtered_df['size_unit'] == 'ml']
            l_df = filtered_df[filtered_df['size_unit'] == 'l']

            if multipack_template != "":
                multipack_df = filtered_df[filtered_df['MPACK'] == 'Y']
            temp_df = l_df.copy()
            temp_df['size'] = l_df['size'].apply((lambda x: x * 1000))
            filtered_df = pd.concat([temp_df,ml_df])

            if product_size_operator == '<': filtered_df = filtered_df[filtered_df['size'] < product_size]
            elif product_size_operator == '<=': filtered_df = filtered_df[filtered_df['size'] <= product_size]
            elif product_size_operator == '>': filtered_df = filtered_df[filtered_df['size'] > product_size]
            elif product_size_operator == '>=': filtered_df = filtered_df[filtered_df['size'] >= product_size]
            elif product_size_operator == '=': filtered_df = filtered_df[filtered_df['size'] == product_size]

            # multipack conditions is an or between product size and MPACK
            if multipack_template != "":
                filtered_df = pd.concat([filtered_df,multipack_df]).drop_duplicates()


        filters = self.get_filters_from_row(row)
        count_of_units = 0
        if count_type == Const.SCENE:
            count_of_units = self.count_of_scenes(filtered_df, filters, target_operator, target)
        elif count_type == Const.FACING:
            count_of_units = self.count_of_facings(filtered_df, filters, consider_few, target)
        elif count_type == Const.SCENE_SOS:
            count_of_units = self.count_of_sos(filtered_df, filters)
        else:
            Log.warning("Couldn't find a correct COUNT variable in template")

        if target_operator == '<=':
            count_result = 1 if (target <= count_of_units) else 0

        # use for getting numeric results instead of 1 and 0
        elif target_operator == '+':
            if isinstance(count_of_units,(int, float, long)):
                count_result = count_of_units
            else:
                count_result = len(count_of_units)
        else:
            count_result = 1 if (target >= count_of_units) else 0
        return count_of_units, target, count_result

    def get_filters_from_row(self, row):
        """
        handle filters appering in scif
        :param row: row containing all filters
        :return: a dictionary of the filters
        """
        filters = dict(row)

        # no need to be accounted for, fields that aren't in scif
        for field in Const.DELETE_FIELDS:
            if field in filters:
                del filters[field]

        if Const.WEIGHT in filters.keys():
            del filters[Const.WEIGHT]
        if Const.GROUP_KPI_NAME in filters.keys():
            del filters[Const.GROUP_KPI_NAME]

        exclude_manufacturer = filters[Const.EXCLUDE_MANUFACTURER].strip()
        if exclude_manufacturer != "":
            filters[Const.MANUFACTURER] = (exclude_manufacturer, Const.EXCLUDE_FILTER)
            del filters[Const.EXCLUDE_MANUFACTURER]

        exclude_category = filters[Const.EXCLUDE_CATEGORY].strip()
        if exclude_category != "":
            filters[Const.CATEGORY] = (exclude_category, Const.EXCLUDE_FILTER)
            del filters[Const.EXCLUDE_CATEGORY]

        exclude_product = filters[Const.EXCLUDE_PRODUCT].strip()
        if exclude_product != "":
            filters[Const.EXCLUDE_PRODUCT] = (exclude_product, Const.EXCLUDE_PRODUCT)
            del filters[Const.EXCLUDE_PRODUCT]

        # filter all the empty cells
        for key in filters.keys():
            if (filters[key] == ""):
                del filters[key]
            elif isinstance(filters[key], tuple):
                filters[key] = (filters[key][0].split(","), filters[key][1])
            else:
                filters[key] = filters[key].split(",")
                filters[key] = [item.strip() for item in filters[key]]

        return self.create_filters_according_to_scif(filters)

    def create_filters_according_to_scif(self, filters):
        """
        adjusting the template names to scif names
        :param filters: only the scif filters in the template shape
        :return: the filters dictionary
        """
        convert_from_scif =    {Const.TEMPLATE_GROUP: 'template_group',
                                Const.TEMPLATE_NAME: 'template_name',
                                Const.BRAND: 'brand_name',
                                Const.CATEGORY: 'category',
                                Const.MANUFACTURER: 'manufacturer_name',
                                Const.PRODUCT_TYPE: 'product_type',
                                Const.MULTIPACK: 'MPAK'}
        for key in filters.keys():
            filters[convert_from_scif[key]] = filters.pop(key)
        return filters

    def count_of_scenes(self, filtered_df, filters, target_operator, target):
        """
        calculate the count of scene types
        :param filtered_df: the first filtered (no scif filters) dataframe
        :param filters: the scif filters
        :param target_operator: the operation to do, + for returning a dataframe (used in group count)
        :param target: the target
        :return: dataframe for group counts +, number of scenes for all other functions
        """
        scene_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        if target_operator == '+':

            # filter by scene_id and by template_name (scene type)
            scene_types_groupby = scene_data.groupby(['template_name','scene_id'])['facings'].sum().reset_index()
            number_of_scenes = scene_types_groupby[scene_types_groupby['facings'] >= target]
        else:
            number_of_scenes = len(scene_data['scene_id'].unique())
        return number_of_scenes

    def count_of_sos(self, filtered_df, filters):
        """
        calculating the share of shelf
        :param filtered_df: the first filtered (no scif filters) dataframe
        :param filters: the scif filters
        :return: the number of different scenes answered the condition  (hard coded 50%)
        """
        filtered_df = filtered_df[~filtered_df['product_name'].isin(['Empty', 'Irrelevant'])]
        scene_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        scene_data = scene_data.rename(columns={"facings": "facings_nom"})

        # filter by scene_id and by template_name (scene type)
        scene_types_groupby = scene_data.groupby(['template_name','scene_id'])['facings_nom'].sum()
        all_products_groupby = self.scif.groupby(['template_name', 'scene_id'])['facings'].sum()
        merge_result = pd.concat((scene_types_groupby, all_products_groupby), axis=1, join='inner').reset_index()
        return len(merge_result[merge_result['facings_nom'] >= merge_result['facings'] * 0.5])

    def count_of_facings(self, filtered_df, filters, consider_few, target):
        '''
        calculate the count of facings
        :param filtered_df: the first filtered (no scif filters) dataframe
        :param filters: the scif filters
        :param consider_few: in case there is a need to consider more then one brand
        :param target: the target to pass
        :return:
        '''
        facing_data = filtered_df[self.tools.get_filter_condition(filtered_df, **filters)]
        if consider_few != "":
            facing_data_groupby = facing_data.groupby(['brand_name'])['facings'].sum()
            if len(facing_data_groupby[facing_data_groupby >= target]) >= consider_few:
                number_of_facings = facing_data['facings'].sum()
            else:
                number_of_facings = 0
        else:
            number_of_facings = facing_data['facings'].sum()
        return number_of_facings

    def get_new_kpi_static_data(self):
        """
        This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        The data is taken from static.kpi_level_2.
        """
        query = CCBRQueries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def write_to_db_result_new_tables(self, fk, numerator_id, numerator_result, result, denominator_id=None,
                                      denominator_result=None, score=None):
        """
        This function creates the result data frame of new rables KPI,
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        table = KPI_NEW_TABLE
        attributes = self.create_attributes_dict_new_tables(fk, numerator_id, numerator_result, denominator_id,
                                                            denominator_result, result, score)
        query = insert(attributes, table)
        self.kpi_results_new_tables_queries.append(query)

    def create_attributes_dict_new_tables(self, kpi_fk, numerator_id, numerator_result, denominator_id,
                                          denominator_result, result, score):
        """
        This function creates a data frame with all attributes needed for saving in KPI results new tables.
        """
        attributes = pd.DataFrame([(kpi_fk, self.session_id, numerator_id, numerator_result, denominator_id,
                                    denominator_result, result, score)], columns=['kpi_level_2_fk', 'session_fk',
                                                                                  'numerator_id', 'numerator_result',
                                                                                  'denominator_id',
                                                                                  'denominator_result', 'result',
                                                                                  'score'])
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_new_tables_queries)
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_query = CCBRQueries.get_delete_session_results_query(self.session_uid, self.session_id)
        cur.execute(delete_query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
        self.rds_conn.disconnect_rds()

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