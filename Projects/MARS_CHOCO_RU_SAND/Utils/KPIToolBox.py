from _mysql import result

import pandas as pd
import math
from datetime import datetime
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
# from serial.tools.list_ports_osx import location_to_string

from Projects.MARS_CHOCO_RU_SAND.Utils.ParseTemplates import parse_template
from Projects.MARS_CHOCO_RU_SAND.Utils.Fetcher import MARS_CHOCO_RU_SANDMARSQueries
from Projects.MARS_CHOCO_RU_SAND.Utils.GeneralToolBox import MARS_CHOCO_RU_SANDMARSGENERALToolBox

__author__ = 'Sanad'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


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


class MARS_CHOCO_RU_SANDMARSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = MARS_CHOCO_RU_SANDMARSGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.atomic_kpi_fk = 0
        self.kpi_fk = 0
        self.setName = ''

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MARS_CHOCO_RU_SANDMARSQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, kpi_set_fk):
        """
        This function calculates the KPI results.
        """
        score = 0
        self.setName = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].iloc[
            0]
        filter_kpi_data = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_name'] == self.setName]
        kpi_template = parse_template(TEMPLATE_PATH, self.setName)
        for i in xrange(len(kpi_template)):
            row = kpi_template.iloc[i]
            self.get_kpi_atomic_kpi_fk(filter_kpi_data, row)
            if row['KPI Type'] == 'Conditional Availability':
                score = self.calculate_availability_by_product_weight(row)
            elif row['KPI Type'] == 'linear SOS':
                score = self.calculate_linear_sos(row)
            elif row['KPI Type'] == 'Availability':
                score = self.calculate_availability(row)
            elif row['KPI Type'] == 'Share of Assortment':
                score = self.calculate_share_of_assortment(row)
            elif row['KPI Type'] == 'Scene Count':
                score = self.calculate_scene_count(row)
            elif row['KPI Type'] == 'Scene Double Facing Count':
                score = self.calculate_scene_double_facing_count(row)
            elif row['KPI Type'] == 'Binary Availability':
                score = self.calculate_binary_availability(row)
            elif row['KPI Type'] == 'Horizontal Block':
                score = self.calculate_horizontal_block(row)
            elif row['KPI Type'] == 'Distribution':
                score = self.calculate_top_sku(row)
            elif row['KPI Type'] == 'Height':
                score = self.calculate_height(row)
            elif row['KPI Type'] == 'Scene Recognition':
                score = self.calculate_hot_spot_display(row)
            elif row['KPI Type'] == 'Duplicated Cashiers':
                score = self.check_duplicated_cashiers(row)
            elif row['KPI Type'] == 'Scene Type count':
                score = self.calculate_hot_spot_scene_count(row)
            elif row['KPI Type'] == 'Scene Location count':
                score = self.calculate_scene_location(row)
            elif row['KPI Type'] == 'Share of Displays':
                score = self.calculate_share_of_boxes(row)
        return score

    def write_to_db_result(self, fk, score, level, threshold=None, result=None):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        pay attention added threshold, result in level 3
        """
        attributes = self.create_attributes_dict(fk, score, level, threshold, result)
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

    def create_attributes_dict(self, fk, score, level, threshold=None, result=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.
        pay attention added threshold, result in level 3
        and the all kpi result stored in result column not in the score
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
            if threshold is not None and result is not None:
                data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
                atomic_kpi_name = data['atomic_kpi_name'].values[0]
                kpi_fk = data['kpi_fk'].values[0]
                kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[
                    0]
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            score, kpi_fk, fk, threshold, result)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'score', 'kpi_fk',
                                                   'atomic_kpi_fk', 'threshold',
                                                   'result'])
            else:
                data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
                atomic_kpi_name = data['atomic_kpi_name'].values[0]
                kpi_fk = data['kpi_fk'].values[0]
                kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[
                    0]
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            score, kpi_fk, fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk',
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
        delete_queries = MARS_CHOCO_RU_SANDMARSQueries.get_delete_session_results_query(self.session_uid)
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
            merged_queries.append('{0} VALUES {1}'.format(group, ','.join(query_groups[group])))
        return merged_queries

    def get_kpi_atomic_kpi_fk(self, kpi_data, row):
        """
        :param kpi_data:
        :param row: These are the parameters which the data frame is filtered by.
        :param filter_kpi_data : thes are the datFrame that contains all Kpi static data
        :return: no return its store kpi_fk  and atomic_fk  in self
        """
        filter_kpi_data = kpi_data.loc[kpi_data['kpi_name'] == row['KPI Display Name']]
        filter_kpi_data = filter_kpi_data['kpi_fk']
        filter_kpi_data = filter_kpi_data.tolist()
        self.kpi_fk = filter_kpi_data[0]
        filter_kpi_data = kpi_data.loc[kpi_data['atomic_kpi_name'] == row['Atomic KPI']]
        filter_kpi_data = filter_kpi_data['atomic_kpi_fk']
        filter_kpi_data = filter_kpi_data.tolist()
        self.atomic_kpi_fk = filter_kpi_data[0]

    def calculate_availability_by_product_weight(self, row):
        """
        :param row:
        :param filters: These are the parameters which the data frame is filtered by.
        :return: Total number of SKUs facings appeared by product weight in the filtered Scene Item Facts data frame.
        """
        filters = {'manufacturer_name': row['manufacturer']}
        category = row['category']
        exclude_category = row['Exclude category']
        exclude_category = str(exclude_category).split(',')
        if category:
            filters['category'] = category
        else:
            filters['category'] = (exclude_category, 0)
        weight = row['max product_weight']
        filtered_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        if weight:
            weight = float(weight)
            filtered_df_weight = filtered_df[filtered_df['size'] <= weight]
            filtered_df_nulls = filtered_df[filtered_df['size'].isnull()]
            filtered_df = filtered_df_weight.append(filtered_df_nulls)
        availability = filtered_df.facings.sum()
        # self.write_to_db_result(self.kpi_fk, availability, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, availability, self.LEVEL3)
        return availability

    def calculate_share_of_assortment(self, row):
        numerator_filters = self.get_filter(row['numerator type'], row['numerator value'])
        numerator_filters['template_name'] = row['scene type']
        denominator_filters = self.get_filter(row['denominator type'], row['denominator value'])
        denominator_filters['template_name'] = row['scene type']
        numerator_uniques = self.tools.calculate_assortment(**numerator_filters)
        denominator_uniques = self.tools.calculate_assortment(**denominator_filters)
        _result = (float(numerator_uniques) / denominator_uniques) * 100 if denominator_uniques else 0
        # self.write_to_db_result(self.kpi_fk, int(_result), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, numerator_uniques, self.LEVEL3, denominator_uniques, _result)
        return _result

    def calculate_linear_sos(self, row):
        col_name = None
        col_scif_name = None
        if self.setName == 'Checkout':
            col_name = 'Scene location type'
            col_scif_name = 'location_type'
        elif self.setName == 'Main Shelf (Loose)':
            col_scif_name = 'template_name'
            col_name = 'scene type'
        numerator_filters = self.get_filter(row['numerator type'], row['numerator value'])
        numerator_filters[col_scif_name] = row[col_name]
        denominator_filters = self.get_filter(row['denominator type'], row['denominator value'])
        denominator_filters[col_scif_name] = row[col_name]
        _result = self.tools.calculate_linear_share_of_shelf(numerator_filters, True, **denominator_filters) * 100
        _result = round(_result)
        # self.write_to_db_result(self.kpi_fk, int(_result), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, int(_result), self.LEVEL3)
        return _result

    def get_filter(self, keys, value):
        """
        :param keys: These are the parameters which the keys as string in data frame.
        :param value: These are the parameters which the values as string in data frame.
        :return: dictionary of keys and value .
        :description : the function take keys and value as string as params and
        split them using regex(,) to start loop in the keys and get value using the index of keys
         and add them as related to a dictionary some times we have only 1 key and a lot of value
         so if there something like that the function take index 0 of key list and put value as list
        """
        isvaluelist = str(value).find('Not')
        keys = str(keys).split(',')
        value = str(value).split(',')
        filters = dict()
        if len(keys) == len(value):
            for i in xrange(len(keys)):
                filters[keys[i]] = value[i]
        else:
            filters[keys[0]] = value
        if isvaluelist > -1:
            for key in filters.keys():
                value = filters[key]
                if value.find('Not') > -1:
                    value = str(value)
                    value.replace('Not(', '')
                    value = value.replace('Not(', '')
                    value = value.replace(')', '')
                    value = value.split('/')
                    value = (value, 0)
                    filters[key] = value
        return filters

    def calculate_availability(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: return number of available product .
        :description : the function take row as params and send the the relevant data  to
        calculate_availability in GeneralToolBox and check if availability grater than threshold
        """
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        filters['product_ean_code'] = ean_codes
        filters['location_type'] = row['Scene location type']
        score = self.tools.calculate_availability(**filters)
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, score, self.LEVEL3)
        return score

    def calculate_binary_availability(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: return 1 if all product available in all cashes zone  else 0 .
        :description : the function take row as params and send the the relevant data  to
        calculate_number_of_facing_and_scene to get each product available scene count
        all get all cash zone scene count using calculate_number_of_scenes function
        and then check each product if have the same count for the cash zone count(all cash zone count)
        that will return 1 else will return 0(if one product have count less than cashes count)
        :example :1- there 8 cash zone in store and all ean code available in all cashes output 1
                  2-there 8 cash zone in store and 1 ean code available only in 7 cash  output 0
        """
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        minimum_threshold = row['minimum threshold']
        filters['location_type'] = row['Scene location type']
        score = self.calculate_minimum_threshold(ean_codes, float(minimum_threshold), filters)
        # self.write_to_db_result(self.kpi_fk, int(score), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, int(score), self.LEVEL3)
        return score

    def calculate_scene_count(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: number of SKUs available in cash zone  / number of  cash zone
        :description : the function take row as params and send filters to calculate_number_of_scenes
        to get the number of scene of cash zone type and sending the relevant data
        to calculate_number_of_facing_and_scene function ,because that function return multiple
        i got the data as list at count[0] .should be a dic (ean_code , number_of_scene)
        convert this dict to dataframe using panda how this help ? converting dict to panda give us ewach ean_code in
         how much number of scene it appear so because the function may take one ean code or multiple ean_codes
          so we need to find the min element and this will be the number of scene that contains the ean code or multiple
          ean_codes
        """
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        minimum_threshold = row['minimum threshold']
        filters['location_type'] = row['Scene location type']
        count_result, number_of_scene_pass, scene_count = self.calculate_facing_in_scenes(ean_codes,
                                                                                          float(minimum_threshold),
                                                                                          filters)
        # self.write_to_db_result(self.kpi_fk, int(count_result), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, number_of_scene_pass, self.LEVEL3, scene_count, count_result)
        return count_result

    def calculate_scene_double_facing_count(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: number of SKUs available in cash zone  / number of  cash zone
        :description : the function take row as params and send filters to calculate_number_of_scenes
        to get the number of scene of cash zone type and sending the relevant data
        to calculate_number_of_facing_and_scene function ,because that function return multiple
        i got the data as list at count[0] .should be a dic (ean_code , number_of_scene)
        convert this dict to dataframe using panda how this help ? converting dict to panda give us ewach ean_code in
         how much number of scene it appear so because the function may take one ean code or multiple ean_codes
          so we need to find the min element and this will be the number of scene that contains the ean code or multiple
          ean_codes
        """
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        ean_codes_fk = self.scif['product_fk'][self.scif['product_ean_code'].isin(ean_codes)]
        filters['location_type'] = row['Scene location type']
        number_of_scene_general = number_of_scene_pass = 0
        relevant_scenes = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        for scene in relevant_scenes['scene_id'].unique().tolist():
            check_pass = False
            relevant_scene_by_product = self.match_product_in_scene[
                ['scene_fk', 'product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number']][
                self.match_product_in_scene['product_fk'].isin(ean_codes_fk)]
            relevant_product_in_scene = relevant_scene_by_product[relevant_scene_by_product['scene_fk'] == scene]

            for bay in relevant_product_in_scene['bay_number'].unique().tolist():
                relevant_product_in_bay = relevant_product_in_scene[relevant_product_in_scene['bay_number'] == bay]
                for shelf in relevant_product_in_bay['shelf_number'].unique().tolist():
                    relevant_product_in_shelf = relevant_product_in_bay[
                        relevant_product_in_bay['shelf_number'] == shelf]
                    sort_sequence = sorted(relevant_product_in_shelf['facing_sequence_number'].tolist(), key=int)
                    if len(sort_sequence) > 1:
                        for i in xrange(len(sort_sequence) - 1):
                            if sort_sequence[i] + 1 == sort_sequence[i + 1]:
                                check_pass = True
            if check_pass:
                number_of_scene_pass += 1
            number_of_scene_general += 1
        score = (float(number_of_scene_pass) / number_of_scene_general) * 100 if number_of_scene_general else 0
        # self.write_to_db_result(self.kpi_fk, int(score), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, number_of_scene_pass, self.LEVEL3, number_of_scene_general, score)

    def calculate_height(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: 1 if SKUs available above shelf number else 0
        :description: the function take relevant data from row and start filter 'scif' dataFrame
        then take all sceneId that have type of cash zone and start loop in every scene id and check if
        product fk is above shelf_number that defined in row it will continue looping if all is
        ok it will return 1 else return 0 and stop loop and exit the function
        """
        score = 1
        failed_count = 0
        filters = dict()
        shelf_number = row['shelf number']
        ean_code = row['EAN codes']
        ean_code = str(ean_code).split(',')
        filters['location_type'] = row['Scene location type']
        filter_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        mask = filter_df['product_ean_code'].isin(ean_code)
        filter_df = filter_df.loc[mask]
        for ean in ean_code:
            if ean == '5000159455367':
                pass
            temp_df = filter_df.loc[filter_df.product_ean_code == ean]
            product_fk = temp_df['product_fk']
            product_fk = product_fk.tolist()
            if len(temp_df) == 0:
                pass
            scenes_id = temp_df['scene_id']
            scenes_id = scenes_id.unique().tolist()
            for i in xrange(len(scenes_id)):
                scene_id = scenes_id[i]
                match_product = self.match_product_in_scene.loc[self.match_product_in_scene.scene_fk == scene_id]
                match_product = match_product.loc[match_product.product_fk == product_fk[0]]
                match_product = match_product.loc[
                    match_product.shelf_number_from_bottom.astype(float) > float(shelf_number)]
                if len(match_product) == 0:
                    score = 0
                    failed_count += 1
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, score, self.LEVEL3)
        return score

    def calculate_top_sku(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: 1 if SKUs available by number of facing else 0
        :description : the function take row as params and send filters to calculate_number_of_facing_and_scene
        to get the number of facing of top 8 sku .
        there 2 situation if ean_code in excel have a list of ean code or have only one code
        so the code will check the tow situation if have only one it will get the dict and get facing value by ean code
        and check if it fit the minimum_threshold
        if there a list of ean code it will loop for each every ean code and check if there facing lees than the
         minimum_threshold it will return 0 otherwise return 1
          hint : if there a list of ean_code the kpi should failed(return 0) if one ot the ean code have facing lees
          than minimum_threshold
        """
        score = 1
        failed_count = 0
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        minimum_threshold = row['minimum threshold']
        minimum_threshold = float(minimum_threshold)
        filters['template_name'] = row['scene type']
        count = self.calculate_number_of_facing_and_scene(ean_codes, minimum_threshold, filters)
        if len(ean_codes) > 1:
            facing = count[1]
            for key in facing.keys():
                number_of_face = facing[key]
                if number_of_face < minimum_threshold:
                    score = 0
                    failed_count += 1
        else:
            score = 1 if count[1][ean_codes[0]] >= minimum_threshold else 0
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, score, self.LEVEL3)
        return score

    def calculate_number_of_facing_and_scene(self, ean_codes, minimum_threshold, filters):
        """
        :param ean_code: These are ean_code that read from row
        :param minimum_threshold the number of min sku faces that should to take it in calculation
        :return: dict contains number of SKUs facing in all session
                 dict contains number of scene that product appear in that above minimum_threshold param
                 number_of_scene_genearal return all scene numbers for all ean codes
        :description : the function take  params and start filter the dataFrame by product ean code
        to get all scenes id and product_fk then start looping for each scene and check if
        product is appear more than minimum threshold if is it add to number_of_scene 1
        and putt all facing for sku in other dict to get all facing in all scene
        :hint : product_fk.tolist() and in side the loop  product_fk[0]
        because  the dataFrame return duplicated product_fk ex(2336,2336)
        """
        number_of_scene_general = 0
        scene = dict()
        facing = dict()
        filter_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        for j in xrange(len(ean_codes)):
            ean_code = ean_codes[j]
            filter_df_ean_code = filter_df.loc[filter_df.product_ean_code == ean_code]
            filter_df_ean_code = filter_df_ean_code.loc[filter_df_ean_code.facings >= minimum_threshold]
            number_of_face = filter_df_ean_code.facings.sum()
            number_of_scene = len(filter_df_ean_code.scene_fk.unique().tolist())
            number_of_scene_general += number_of_scene
            scene[ean_code] = number_of_scene
            facing[ean_code] = number_of_face
        return scene, facing, number_of_scene_general

    def calculate_facing_in_scenes(self, ean_codes, minimum_threshold, filters):
        """
        :param filters:
        :param ean_codes:
        :param ean_code: These are ean_code that read from row
        :param minimum_threshold the number of min sku faces that should to take it in calculation
        :return: dict contains number of SKUs facing in all session
                 dict contains number of scene that product appear in that above minimum_threshold param
                 number_of_scene_genearal return all scene numbers for all ean codes
        :description : the function take  params and start filter the dataFrame by product ean code
        to get all scenes id and product_fk then start looping for each scene and check if
        product is appear more than minimum threshold if is it add to number_of_scene 1
        and putt all facing for sku in other dict to get all facing in all scene
        :hint : product_fk.tolist() and in side the loop  product_fk[0]
        because  the dataFrame return duplicated product_fk ex(2336,2336)
        """
        number_of_scene_general = number_of_scene_pass = 0
        relevant_scenes = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        for scene in relevant_scenes['scene_id'].unique().tolist():
            relevant_product_in_scene = relevant_scenes[['product_ean_code', 'facings']][
                relevant_scenes['scene_id'] == scene]
            relevant_product_by_facing = relevant_product_in_scene['product_ean_code'][
                relevant_product_in_scene['facings'] >= minimum_threshold]
            result_product_by_ean_code = relevant_product_by_facing[relevant_product_by_facing.isin(ean_codes)]
            if len(result_product_by_ean_code) == len(ean_codes):
                number_of_scene_pass += 1
            number_of_scene_general += 1
        result_scene = (float(number_of_scene_pass) / number_of_scene_general) * 100 if number_of_scene_general else 0
        return result_scene, number_of_scene_pass, number_of_scene_general

    def calculate_minimum_threshold(self, ean_codes, minimum_threshold, filters):
        """
        :param filters:
        :param ean_codes:
        :param ean_code: These are ean_code that read from row
        :param minimum_threshold the number of min sku faces that should to take it in calculation
        :return: dict contains number of SKUs facing in all session
                 dict contains number of scene that product appear in that above minimum_threshold param
                 number_of_scene_genearal return all scene numbers for all ean codes
        :description : the function take  params and start filter the dataFrame by product ean code
        to get all scenes id and product_fk then start looping for each scene and check if
        product is appear more than minimum threshold if is it add to number_of_scene 1
        and putt all facing for sku in other dict to get all facing in all scene
        :hint : product_fk.tolist() and in side the loop  product_fk[0]
        because  the dataFrame return duplicated product_fk ex(2336,2336)
        """
        relevant_scenes = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        relevant_product_by_facing = relevant_scenes['product_ean_code'][
            relevant_scenes['facings'] >= minimum_threshold]
        result_product_by_ean_code = relevant_product_by_facing[relevant_product_by_facing.isin(ean_codes)].unique()
        if len(result_product_by_ean_code) == len(ean_codes):
            return True
        else:
            return False

    def calculate_scene_location(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: number of hot spot base in location
        :description : the function take row as params and send relevant data to func get_location
        then will get dataFrame contains a scene Id that of specific type(hot spot) and in specific location
        and get sceneId unique why unique to make sure that no duplicated
        and find the length of the scene id list in this way we have the number of scene in
         location by specific type
        example :
        get number of scene type hot spot in entry location
        there 24 scene but only 12 of them of type hot spot and only 2 in the entry location
        return should be 2
        """
        location_name = row['locations']
        location_name = str(location_name).split(',')
        location_name = str(location_name).replace('[', '')
        location_name = location_name.replace(']', '')
        scene_type = row['scene type']
        scene_type = str(scene_type).split(',')
        scene_type = str(scene_type).replace('[', '')
        scene_type = scene_type.replace(']', '')
        number_of_scene_type = len(
            self.scif['scene_id'][self.scif['template_name'].isin(row['scene type'].split(','))].unique())
        scene_by_location = self.get_location(location_name, scene_type)
        scene_list = scene_by_location['Scene Id'].unique().tolist()
        location_scene_count = len(scene_list)
        score = (float(location_scene_count) / number_of_scene_type) * 100 if number_of_scene_type else 0
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, location_scene_count, self.LEVEL3, number_of_scene_type, score)
        return location_scene_count

    def calculate_hot_spot_scene_count(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: Count how many hot spots have at least minimum_threshold facings mars manufacturer or brand or comp
        :description : the function take row as params and send filters the scif base on scene type
        and by brand or manufacturer or by Competition and git all scene the take the scene as unique and start looping
        on every unique scene id and filter data frame again but this time with scene id and get leng of dataframe tell
        us how mane facing we have in the scene for brand or mars manufacturer or comp and if that above or
        equal the minimum_threshold add one else 0 in that we can get how much scene have brand or etc
        :example: 7 hot spot and only 3 hotspot have brand mars above 1 facing return 3
        """
        number_of_scene = 0
        filters = dict()
        minimum_threshold = row['minimum threshold']
        minimum_threshold = float(minimum_threshold)
        filters['location_type'] = row['Scene location type']
        number_of_location_type_general = len(
            self.scif['scene_id'][self.scif['location_type'] == row['Scene location type']].unique())
        brand = row['brand']
        manufacturer = row['manufacturer']
        exclude_manufacturer = row['Exclude manufacturer']
        if brand:
            filters['brand_name'] = brand
        elif manufacturer:
            filters['manufacturer_name'] = manufacturer
        else:
            filters['manufacturer_name'] = (exclude_manufacturer, 0)
        filter_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        scenes_id = filter_df['scene_id']
        scenes_id = scenes_id.unique().tolist()
        for scene_id in scenes_id:
            scene_fact = filter_df.loc[filter_df['scene_id'] == scene_id]
            number_of_scene += 1 if len(scene_fact) >= minimum_threshold else 0
        score = float(number_of_scene) / number_of_location_type_general * 100 if number_of_location_type_general else 0
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, number_of_scene, self.LEVEL3, number_of_location_type_general,
                                score)

        return number_of_scene

    def calculate_hot_spot_display(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: Count how many hotspots have at least 1 display with display mars manufacturer or brand or comp
        :description : the function take row as params and send filters the scif base on scene type
        and by brand or manufacturer or by Competition and git all scene , get match_display_in_scene and then take the
        scene as unique and start looping and filter match_display on scene id and again filter the match display as
        unique  by display name (the match display return scene duplicated) and if scene id have display that equal
        or above minimum_threshold add one else zero in this way we can count how many scene have display
        :example: 7 hot spot and only 3 hotspot have brand mars and have display
         return 3

         :Note: ***- each scene must have only one display relation between them 1 to 1 -***
        """
        number_of_display = 0
        filters = dict()
        minimum_threshold = row['minimum threshold']
        minimum_threshold = float(minimum_threshold)
        filters['location_type'] = row['Scene location type']
        brand = row['brand']
        manufacturer = row['manufacturer']
        exclude_manufacturer = row['Exclude manufacturer']
        if brand:
            filters['brand_name'] = brand
        elif manufacturer:
            filters['manufacturer_name'] = manufacturer
        else:
            filters['manufacturer_name'] = (exclude_manufacturer, 0)
        filter_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        match_display = self.get_match_display()
        scenes_id = filter_df['scene_id']
        scenes_id = scenes_id.unique().tolist()
        for scene_id in scenes_id:
            filter_display = match_display.loc[match_display['scene_fk'] == scene_id]
            length = len(filter_display['display_name'].unique())
            number_of_display += 1 if length >= minimum_threshold else 0
        count_of_displays_total = len(match_display['scene_fk'].unique())
        score = float(number_of_display) / count_of_displays_total * 100 if count_of_displays_total else 0
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, number_of_display, self.LEVEL3, count_of_displays_total, score)
        return number_of_display

    def get_match_display(self, flag=None):
        """
        :params condition : parameter to get data from sql by manufacturers
        manufacturers should be the name of type (column name in DB)
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        if flag:
            query = MARS_CHOCO_RU_SANDMARSQueries.get_match_display_by_condition(self.session_uid)
        else:
            query = MARS_CHOCO_RU_SANDMARSQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_location(self, location_name, scene_type):
        """
        :params location_name : parameter to get data from sql by location name
        :params scene_type : parameter to get only the scene by type(template)
        location_name should be the name (column name in DB)
        scene_type should be the name of template (column name in DB in static.template)
        This function extracts the scene from specific type by the given location data and saves it into one global data frame.
        The data is taken from multiply tables look to the fetcher class.
        example :
        there 24 scene but only 12 of them of type hot spot and only 2 in the entry location
        the get location function take the location name and scene type and select the data from db
        and return dataframe so the dataframe will contains only 2 scene 0f type hot spot
        and that in entry location
        """

        query = MARS_CHOCO_RU_SANDMARSQueries.get_location(self.session_uid, location_name, scene_type)
        location_df = pd.read_sql_query(query, self.rds_conn.db)
        return location_df

    def get_assortment_of_products(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: return 1 if one  product available in any hotSpot else 0 / return percentage if ean codes avail in any hot spot.
        :description : the function take row as params and send the the relevant data  to
        calculate_number_of_facing_and_scene to get each product available facing count
       --code2 and then get length of ean_code in excel (input) and length of dict ean_code_facing
        then divide length_of_facing_ean_code / length_of_input_ean_code * 100
        example : 8 ean code in excel file and the dict have 7 ean code with facing above or equal
        minimum_threshold 7/8 *100
      --code1 and then get the ean_code (should be only 1 ean code ) in excel and the dict that contains
       with how much facing now get from the dict the value by ean and check if above or equal
       minimum_threshold return 1 else 0
        :testCase :1- there 1 ean code that avail in only one hotspot with 2 facing
                      return 1
                   2- there 1 ean code but not avail in hot spot return 0
                   3- there 1 ean code that avail in only one hotspot with 2 facing but the minimum_threshold for facing is 3
                      return 0
        """
        filters = dict()
        ean_codes = str(row['EAN codes']).split(',')
        minimum_threshold = row['minimum threshold']
        minimum_threshold = float(minimum_threshold)
        filters['template_name'] = row['scene type']
        score = self.calculate_number_of_facing_and_scene(ean_codes, minimum_threshold, filters)
        ean_code_facing_count = score[1]
        # belong--code 2
        length_of_input_ean_code = len(ean_codes)
        length_of_facing_ean_code = len(ean_code_facing_count)
        #  if customer decide true/false --code 1
        if length_of_input_ean_code == 1:
            return 1 if ean_code_facing_count[ean_codes[0]] >= minimum_threshold else 0
        # if customer decide percentage --code 2
        if length_of_facing_ean_code:
            return 0
        else:
            return float(length_of_facing_ean_code) / length_of_input_ean_code * 100 if length_of_input_ean_code else 0

    def calculate_share_of_boxes(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: count how many display of type mars manufacturer divide display mars manufacturer and comp * 100
        :description : the function take row as params and send twice to get_match_display
        the first one to get numerator value the second one to get denominator and divide numerator in the
         denominator (n/m *100) mul by 100 to get percentage
         :hint : numerator in this situation should be the display type for own
         manufacturer  denominator should be   manufacturer and the Competition
        :Note: ***- each scene may have more than one display relation between them 1 to m -***
        #denominator excluded  because the denominator mars and not mars means all the manufacturer
         already the query return that
        """

        match_display = self.get_match_display(flag=1)
        numerator_type = row['numerator type']
        numerator_type = str(numerator_type)
        numerator_value = row['numerator value']
        numerator_value = str(numerator_value).split(',')
        len_numerator_result = len(match_display[match_display[numerator_type].isin(numerator_value)])
        len_denominator_result = len(match_display)
        _result = float(len_numerator_result) / len_denominator_result * 100 if len_denominator_result else 0
        # self.write_to_db_result(self.kpi_fk, int(_result), self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, len_numerator_result, self.LEVEL3, len_denominator_result, _result)
        return _result

    def calculate_count_side_a(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: count how many display of type cash zone Side A
        """
        match_display = self.get_match_display(str(row['display name']))
        match_display = match_display['scene_id'].unique()
        _result = len(match_display)
        return _result

    def calculate_horizontal_block(self, row):
        filters = dict()
        brands = str(row['brand']).split(',')
        filters['brand_name'] = brands
        relevant_scenes = self.scif[self.scif['location_type'] == row['Scene location type']]['scene_fk'].unique()
        result = self.calculate_horizontal_sequence(relevant_scenes=relevant_scenes, **filters)
        result = 1 if result else 0
        self.write_to_db_result(self.atomic_kpi_fk, result, self.LEVEL3)
        return result

    def calculate_horizontal_sequence(self, relevant_scenes, **filters):
        result = False
        for scene in relevant_scenes:
            filters['scene_fk'] = scene
            matches_products_fk = self.scif[self.tools.get_filter_condition(self.scif, **filters)]['product_fk']
            matches_products = self.match_product_in_scene[
                (self.match_product_in_scene['product_fk'].isin(matches_products_fk)) & (
                    self.match_product_in_scene['scene_fk'] == scene)]
            relevant_bays = matches_products['bay_number'].unique().tolist()
            for bay in relevant_bays:
                relevant_prducts_in_bays = matches_products[
                    (matches_products['bay_number'] == bay) & (matches_products['stacking_layer'] == 1)]
                relevant_shelfs = relevant_prducts_in_bays.groupby('shelf_number')['shelf_number'].count().reset_index(
                    name='count')
                relevant_shelfs = relevant_shelfs[relevant_shelfs['count'] > 1]['shelf_number']
                for shelf in relevant_shelfs:
                    products = relevant_prducts_in_bays[relevant_prducts_in_bays['shelf_number'] == shelf]
                    product_sequence = products['sku_sequence_number'].unique().tolist()
                    product_sequence.sort()
                    if not self.check_sequence_list(product_sequence):
                        return False
                    # if (product_sequence[-1] - product_sequence[0] + 1) != len(products['product_fk'].unique()):
                    #     return False
                    else:
                        result = True
        if result:
            return True
        else:
            return False

    def check_sequence_list(self, list):
        for i in xrange(len(list) - 1):
            if list[i] + 1 != list[i + 1]:
                return False
        return True

    def check_duplicated_cashiers(self, row):
        """
        :param row: These are the parameters which the row in data frame.
        :return: 1 if there a duplicated scene else 0
        :description : the function take row in excel as a param and extract scene type to filter the scif
        then it will take all scenes id as a unique and make a copy of them to another list named duplicated_scenes_id
        and start looping through the first list and in second list and compare if there a two data frame that represent
        scene the same it will add 1 to the score in the end check if score above 0 it will return 1 else 0
        :example: in the store there 3 cash zone but sales person capture the the first cash zone twice and didn't
        capture the second but the third capture it in the end we have 3 cash zone but 2 of this cash zone duplicated
        the output should be 1
        """
        filters = dict()
        score = 0
        filters['location_type'] = row['Scene location type']
        filter_df = self.scif[self.tools.get_filter_condition(self.scif, **filters)]
        scenes_id = filter_df['scene_id'].unique().tolist()
        for i in xrange(len(scenes_id)):
            product_list_1 = filter_df['product_fk'][filter_df['scene_id'] == scenes_id[i]].unique().tolist()
            for j in xrange(len(scenes_id)):
                if i == j: continue
                product_list_2 = filter_df['product_fk'][filter_df['scene_id'] == scenes_id[j]].unique().tolist()
                intersection_list = list(set(product_list_1).intersection(product_list_2))
                if len(product_list_1) > len(product_list_2):
                    biggest_list = len(product_list_1)
                else:
                    biggest_list = len(product_list_2)
                if len(intersection_list) / float(biggest_list) > 0.85:
                    score = 1
        # self.write_to_db_result(self.kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(self.atomic_kpi_fk, score, self.LEVEL3)
        return score
