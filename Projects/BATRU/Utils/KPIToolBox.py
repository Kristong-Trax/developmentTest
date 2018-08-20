# coding=utf-8
# import numpy as np
import pandas as pd
from datetime import datetime
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.BATRU.Utils.ParseTemplates import parse_template
from Projects.BATRU.Utils.Fetcher import BATRUQueries
from Projects.BATRU.Utils.GeneralToolBox import BATRUGENERALToolBox
from Projects.BATRU.Utils.PositionGraph import BATRUPositionGraphs
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime, log_runtime


__author__ = 'uri'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
MAX_DAYS_BACK_FOR_HISTORY_BASED_ASSORTMENT = 60
MAX_CYCLES_FOR_HISTORY_BASED_ASSORTMENT = 3
EMPTY = 'Empty'
OTHER = 'Other'
IRRELEVANT = 'Irrelevant'
SKU = 'SKU'
POSM = 'POS'
NON_COMPETITOR_PRODUCTS = ("Cigarettes Empty", "Empty")
# P1_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'StoreAssortment.csv')
P2_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'P2_monitored_sku.xlsx')
P3_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'P3_template.xlsx')
P4_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'p4_template.xlsx')
P5_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'p5_template.xlsx')
POSM_AVAILABILITY = 'POSM Status'
SHARE_OF = 'Share of Shelf / Assortment'
PRICE_MONITORING = 'Price Monitoring'
MOBILE_PRICE_MONITORING = 'Mobile Price Monitoring'
MOBILE_PRICE_MONITORING_KPI_FK = 5891
P2_FULFILMENT = 'Price-Monitoring fulfillment (TMR)'
P2_MOBILE_FULFILMENT = 'Mobile Price Fulfillment'
P2_EFFICIENCY = 'Price-Monitoring efficiency (Trax)'
P2_RAW_DATA = 'Raw Data'
P2_SET_PRICE = 'Price Monitoring'
P2_SET_DATE = 'Date Monitoring'
P4_API_SET = 'POSM Status Raw Data'
BAT = 'BAT'
ENTRY_TEMPLATE_GROUP = u'1. На входе'
EXIT_TEMPLATE_GROUP = u'2. На выходе'
EXIT_TEMPLATE_NAME = u'Выход'
EXIT_STOCK_NAME = u'Сток на выходе (выдвинутая полка)'
SK = 'SK'
SK_RAW_DATA = 'SK Raw Data'
SAS = 'SAS'
SAS_RAW_DATA = 'SAS Raw Data'
ASSORTMENT_DISTRIBUTION = 'Assortment Distribution'
SAS_ZONE = 'SAS ZONE'
ASSORTMENT_DISTRIBUTION_EXIT = 'Assortment Distribution - Exit'
ASSORTMENT_DISTRIBUTION_ENTRY = 'Assortment Distribution - Entry'
ASSORTMENT_DISTRIBUTION_EXIT_FOR_API = 'Assortment Distribution Raw Data - Exit'
ASSORTMENT_DISTRIBUTION_ENTRY_FOR_API = 'Assortment Distribution Raw Data - Entry'
ASSORTMENT_DISTRIBUTION_AGGREGATION_ENTRY = 'Assortment Distribution Aggregations - Entry'
ASSORTMENT_DISTRIBUTION_AGGREGATION_EXIT = 'Assortment Distribution Aggregations - Exit'
EFFICIENCY_TEMPLATE_NAME = u'Дата производства'
ATTRIBUTE_3 = 'Filter stores by \'attribute 3\''
BUNDLE2LEAD = "bundle>lead"
LEAD2BUNDLE = "lead>bundle"
OUTLET_ID = 'Outlet ID'
EAN_CODE = 'product_ean_code'


class BATRUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    SKU_PRESENCE_KPI_NAME = 'SKU presence in SKU_List'
    SKU_SEQUENCE_KPI_NAME = 'SKU sequence'
    SKU_REPEATING_KPI_NAME = 'SKU repeating'
    API_SKU_PRESENCE_KPI_NAME = '{fixture};{section_name};SKU presence in SKU_List'
    API_SKU_REPEATING_KPI_NAME = '{fixture};{section_name};SKU repeating'
    API_SKU_SEQUENCE_KPI_NAME = '{fixture};{section_name};SKU sequence'
    API_SECTION_KPI_NAME = '{fixture};{section_name}'
    API_EQUIPMENT_KPI_NAME = 'Equipment:{fixture}'
    API_DISPLAY_KPI_NAME = 'Equipment:{fixture} – Display:{display}'
    NO_COMPETITORS_IN_SAS_ZONE = 'No competitors in SAS Zone'
    API_NO_COMPETITORS_IN_SAS_ZONE = 'Equipment:{fixture} – No competitors in SAS Zone'
    CONTRACTED_OOS = 'Contracted - OOS'
    CONTRACTED_AVAILABILITY = 'Contracted - Availability'
    CONTRACTED_DISTRIBUTION = 'Contracted - Distribution'
    CONTRACTED = 'Contracted'
    CONTRACTED_FOR_ENTRY = 'Contracted For Entry'
    CONTRACTED_FOR_EXIT = 'Contracted For Exit'
    ASSORTMENT_DISTRIBUTION_AVAILABILITY = 'Assortment Distribution - Availability'
    ASSORTMENT_DISTRIBUTION_DISTRIBUTION = 'Assortment Distribution - Distribution'
    ASSORTMENT_DISTRIBUTION_OOS = 'Assortment Distribution - OOS'

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid

        self.products = self.data_provider[Data.PRODUCTS]
        self.products = self.products\
            .merge(self.products[['product_fk', 'product_ean_code', 'product_name']],
                   how='left', left_on='substitution_product_fk', right_on='product_fk', suffixes=['', '_lead'])
        self.products.loc[self.products['product_fk_lead'].isnull(), 'product_ean_code_lead'] = self.products['product_ean_code']
        self.products.loc[self.products['product_fk_lead'].isnull(), 'product_name_lead'] = self.products['product_name']
        self.products.loc[self.products['product_fk_lead'].isnull(), 'product_fk_lead'] = self.products['product_fk']

        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_products = self.all_products\
            .merge(self.all_products[['product_fk', 'product_ean_code', 'product_name']],
                   how='left', left_on='substitution_product_fk', right_on='product_fk', suffixes=['', '_lead'])
        self.all_products.loc[self.all_products['product_fk_lead'].isnull(), 'product_ean_code_lead'] = self.all_products['product_ean_code']
        self.all_products.loc[self.all_products['product_fk_lead'].isnull(), 'product_name_lead'] = self.all_products['product_name']
        self.all_products.loc[self.all_products['product_fk_lead'].isnull(), 'product_fk_lead'] = self.all_products['product_fk']

        self.templates = self.data_provider[Data.ALL_TEMPLATES]

        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]\
            .merge(self.templates[['template_fk', 'template_name']], how='left', on='template_fk')
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.merged_additional_data = self.get_additional_product_data()
        self.tools = BATRUGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.match_display_in_scene = self.tools.get_match_display()
        # self.tools.upload_store_assortment_file(P1_PATH)
        self.kpi_static_data = self.get_kpi_static_data()
        new_products = False
        try:
            set_names = [ASSORTMENT_DISTRIBUTION, ASSORTMENT_DISTRIBUTION_ENTRY]
            set_fks = self.kpi_static_data[
                self.kpi_static_data['kpi_set_name'].isin(set_names)]['kpi_set_fk'].unique().tolist()
            for set_fk in set_fks:
                if self.get_missing_products_to_api_set(set_fk):
                    new_products = True
        except Exception as e:
            Log.info('Updating API sets failed')
        if new_products:
            self.kpi_static_data = self.get_kpi_static_data()
        self.custom_templates = {}
        self.sas_zone_statuses_dict = {}
        self.fixtures_statuses_dict = {}
        if not self.scif.empty:
            self.get_store_number_1()
        self.position_graph = BATRUPositionGraphs(self.data_provider)
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.kpi_results_queries = []
        self.section_products = pd.DataFrame([], columns=['section', 'product'])  # todo change this to get_template
        self.filters_params = {'Template Group': 'template_group', 'Product Name': 'display_name',
                               'KPI Display Name': 'additional_attribute_1'}
        self.posm_in_session = None
        self.encode_data_frames()
        self.fixtures_presence_histogram = {}
        self.sas_zones_scores_dict = {}
        self.state = self.get_state()
        self.p4_display_count = {}

#init functions

    def encode_data_frames(self):
        self.kpi_static_data['kpi_name'] = self.encode_column_in_df(self.kpi_static_data, 'kpi_name')
        self.kpi_static_data['atomic_kpi_name'] = self.encode_column_in_df(self.kpi_static_data, 'atomic_kpi_name')
        self.scif['additional_attribute_1'] = self.encode_column_in_df(self.scif, 'additional_attribute_1')
        self.templates['additional_attribute_1'] = self.encode_column_in_df(self.templates, 'additional_attribute_1')

    def get_store_number_1(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = BATRUQueries.get_store_data(self.scif['store_id'][0])
        store_data = pd.read_sql_query(query, self.rds_conn.db)
        self.scif = self.scif.merge(store_data, how='left', left_on='store_id',
                                    right_on='store_fk', suffixes=['', '_1'])
        return

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = BATRUQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_state(self):
        query = BATRUQueries.get_state(self.store_id)
        match_state = pd.read_sql_query(query, self.rds_conn.db)
        return 'No State' if match_state.empty else match_state['state'].values[0]

    def upload_store_assortment_file_for_p1(self, file_path):
        """
        This function validates the template and uploads store assortments to pservice.custom_osa
        It printS the stores and products that don't exist in the DB
        :param file_path: the assortment file (suppose to have 2 columns: Outlet ID and product_ean_code
        """
        if self.tools.p1_assortment_validator(file_path):
            self.tools.upload_store_assortment_file(file_path)
        else:
            Log.warning("Error in P1 store assortment template")
            print "Please fix the issues and try again"

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        self.handle_priority_1()
        self.handle_priority_2()
        self.handle_priority_3()
        self.handle_priority_4()
        self.handle_priority_5()

# general functions:

    def get_custom_template(self, template_path, name):
        if name not in self.custom_templates.keys():
            template = parse_template(template_path, sheet_name=name)
            if template.empty:
                template = parse_template(template_path, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def get_bundles_by_definitions(self, input_products, convert=BUNDLE2LEAD, input_type='product_fk',
                                   output_type='product_fk', get_only_one=False):
        """
        the function takes eans/fks and returns their lead/bundle products
        :param input_products: str/float, or a list of strings/floats
        :param convert: BUNDLE2LEAD/LEAD2BUNDLE
        :param input_type: product_fk/product_ean_code
        :param output_type: product_fk/product_ean_code
        :return: a list of fks/eans
        """
        if type(input_products) is not list:
            input_products = [input_products]
        input_products = self.all_products[self.all_products[input_type].isin(input_products)][
            'product_fk'].unique().tolist()
        if convert == LEAD2BUNDLE:
            output_fks = self.all_products.loc[self.all_products['substitution_product_fk'].isin(input_products)][
                'product_fk'].unique().tolist()
        elif convert == BUNDLE2LEAD:
            output_fks = self.all_products.loc[self.all_products['product_fk'].isin(input_products)][
                'substitution_product_fk'].unique().tolist()
        else:
            Log.debug("Wrong input, get_bundles_by_definitions can't convert '{}'".format(convert))
            return None
        output = self.all_products[self.all_products['product_fk'].isin(output_fks)][output_type].unique().tolist()
        if get_only_one:
            if output:
                return output[0]
            else:
                return None
        return self.all_products[self.all_products['product_fk'].isin(output_fks)][output_type].unique().tolist()

    @staticmethod
    def encode_column_in_df(df, column_name):
        return df[column_name].str.encode('utf-8')

    # P1 KPI
    @kpi_runtime()
    def handle_priority_1(self):
        set_templates_mapping = {EXIT_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION,
                                 ENTRY_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION_ENTRY}
        set_templates_api_mapping = {EXIT_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION_EXIT_FOR_API,
                                     ENTRY_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION_ENTRY_FOR_API}
        api_aggregation_templates_mapping = {EXIT_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION_AGGREGATION_EXIT,
                                             ENTRY_TEMPLATE_GROUP: ASSORTMENT_DISTRIBUTION_AGGREGATION_ENTRY}
        contracted_api_mapping = {EXIT_TEMPLATE_GROUP: self.CONTRACTED_FOR_EXIT,
                                  ENTRY_TEMPLATE_GROUP: self.CONTRACTED_FOR_ENTRY}
        results_mapping = {'OOS contracted': '1. OOS (contracted)',
                           'No Distribution': '2. No Distribution',
                           'AV': '3. AV',
                           'OOS rest': '4. OOS rest',
                           'NA': '5. NA'}
        last_cycles_products = self.calculate_history_based_assortment(self.session_fk)
        contracted_products = self.tools.get_store_assortment_for_store(self.store_id).values()
        for template_group in [EXIT_TEMPLATE_GROUP, ENTRY_TEMPLATE_GROUP]:
            session_product_fks = self.scif.loc[(self.scif['dist_sc'] == 1) &
                                                (self.scif['template_group'] == template_group)][
                'product_fk'].unique().tolist()
            session_products = self.all_products[self.all_products['product_fk'].isin(session_product_fks)][
                'product_ean_code'].unique().tolist()
            total_assortment_init = self.all_products[self.all_products['product_fk'].isin(session_product_fks) &
                                                      (self.all_products['manufacturer_name'] == BAT) &
                                                      (self.all_products['product_type'].isin([SKU, POSM]))][
                'product_ean_code'].unique().tolist()
            active_products = self.all_products.loc[(self.all_products['product_type'].isin([SKU, POSM])) &
                                                    (self.all_products['manufacturer_name'] == BAT) &
                                                    (self.all_products['is_active'] == 1)][
                'product_ean_code'].unique().tolist()
            total_assortment_init.extend(active_products)
            total_assortment = set(total_assortment_init)
            if None in total_assortment:
                total_assortment.remove(None)
            product_log = {}
            for assortment in [total_assortment, contracted_products]:
                availability_score_list = {}
                dist_score_list = {}
                oos_contracted_score_list = {}
                oos_rest_score_list = {}
                for product in assortment:
                    if product is None:
                        continue
                    active_status = 1
                    result = 'No Distribution'
                    oos_contracted_status = 0
                    oos_rest_status = 0
                    try:
                        product_fk = self.all_products.loc[(self.all_products['product_ean_code'] == product) &
                                                           (self.all_products['manufacturer_name'] == BAT)][
                            'product_fk'].values[0]
                    except Exception as e:
                        Log.warning('Product with the ean code {} is not defined in the data provider'.format(product))
                        continue
                    # bundled_products = self.bundle_fk_ean(product_fk)
                    bundled_products = self.get_bundles_by_definitions(
                        product_fk, convert=LEAD2BUNDLE, input_type='product_fk', output_type='product_ean_code')
                    if bundled_products:
                        bundled_products.append(product)
                        products_list = bundled_products
                        presence_condition = set(bundled_products) & set(session_products)
                    else:
                        bundle_lead = self.get_bundles_by_definitions(
                            product_fk, convert=BUNDLE2LEAD, input_type='product_fk', output_type='product_ean_code',
                            get_only_one=True)
                        if bundle_lead:
                            continue
                        else:
                            presence_condition = True if product in session_products else False
                            products_list = [product]
                    if presence_condition:
                        result = 'AV'
                        availability_status = 1
                        dist_status = 1
                    else:
                        availability_status = 0
                        if set(products_list) & set(last_cycles_products['product_ean_code'].tolist()):
                            if set(products_list) & set(contracted_products):
                                if active_status == 1:
                                    oos_contracted_status = 1
                                    result = 'OOS contracted'
                                    dist_status = 1
                                else:
                                    dist_status = 0
                            else:
                                if active_status == 1:
                                    oos_rest_status = 1
                                    dist_status = 1
                                    result = 'OOS rest'
                                else:
                                    dist_status = 0
                        else:
                            if set(products_list) & set(contracted_products):
                                if active_status == 1:
                                    dist_status = 0
                                else:
                                    dist_status = 0
                            else:
                                result = 'NA'
                                dist_status = 0
                    try:
                        product_name = self.all_products.loc[self.all_products['product_fk'] == product_fk][
                            'product_english_name'].values[0]
                    except Exception as e:
                        Log.warning('Product with the ean code {} is not defined in the data provider'.format(product))
                        continue
                    oos_contracted_score_list[product] = oos_contracted_status
                    oos_rest_score_list[product] = oos_rest_status
                    availability_score_list[product] = availability_status
                    dist_score_list[product] = dist_status

                    if product_fk not in product_log.keys():
                        product_log[product_fk] = results_mapping[result]
                        set_for_db = set_templates_mapping[template_group]
                        set_for_api_aggregations = api_aggregation_templates_mapping[template_group]
                        set_for_api = set_templates_api_mapping[template_group]
                        self.save_level2_and_level3(set_for_db,
                                                    product_name, results_mapping[result], level_3_only=True,
                                                    level2_name_for_atomic=self.CONTRACTED)
                        self.write_to_db_result_for_api(score=result, level=self.LEVEL3, level3_score=None,
                                                        kpi_set_name=set_for_api,
                                                        kpi_name=contracted_api_mapping[template_group],
                                                        atomic_kpi_name=product)
                if assortment == total_assortment:
                    oos = round((sum(oos_rest_score_list.values()) / float(len(total_assortment))) * 100, 1)
                    distribution = round((sum(dist_score_list.values()) / float(len(total_assortment))) * 100, 1)
                    availability = round((sum(availability_score_list.values()) / float(len(total_assortment))) * 100,
                                         1)
                    self.save_level1(set_for_db, score=distribution, score2=availability, score3=oos)
                    self.save_level1(set_for_api, score=distribution, score2=availability, score3=oos)
                    self.save_level1(set_for_api_aggregations, score=distribution, score2=availability, score3=oos)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=oos,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.ASSORTMENT_DISTRIBUTION_OOS)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=availability,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.ASSORTMENT_DISTRIBUTION_AVAILABILITY)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=distribution,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.ASSORTMENT_DISTRIBUTION_DISTRIBUTION)
                elif assortment == contracted_products:
                    if contracted_products:
                        total_contract = len(dist_score_list)
                        oos = round((sum(oos_contracted_score_list.values()) / float(total_contract)) * 100, 1)
                        distribution = round((sum(dist_score_list.values()) / float(total_contract)) * 100, 1)
                        availability = round((sum(availability_score_list.values()) / float(total_contract)) * 100, 1)
                    else:
                        oos = 0
                        distribution = 0
                        availability = 0
                        Log.info('No contracted products are defined for store {}'.format(self.scif['store_number_1']))
                    self.save_level2_and_level3(set_for_db, kpi_name=self.CONTRACTED, result=distribution,
                                                score_2=availability, score_3=oos, level_2_only=True)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=oos,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.CONTRACTED_OOS)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=availability,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.CONTRACTED_AVAILABILITY)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=distribution,
                                                    kpi_set_name=set_for_api_aggregations,
                                                    kpi_name=contracted_api_mapping[template_group],
                                                    atomic_kpi_name=self.CONTRACTED_DISTRIBUTION)

                else:
                    pass
        return

    def calculate_history_based_assortment(self, session_id, required_template_group=None):
        """
        :param int session_id:
        :return:
        """
        days_back = MAX_DAYS_BACK_FOR_HISTORY_BASED_ASSORTMENT
        previous_sessions = self.get_previous_sessions_in_the_same_store_by_session(session_id, days_back)
        if previous_sessions.empty:
            return self._get_empty_assortment()
        else:
            required_cycles = []
            current_cycle = self.get_current_cycle(self.visit_date)
            current_cycle = current_cycle['plan_fk'].values[0]
            required_cycles.append(current_cycle)
            last_2_cycles = previous_sessions.loc[previous_sessions['plan_fk'].between(current_cycle - 2,
                                                                                       current_cycle)][
                'plan_fk'].unique().tolist()
            required_cycles.extend(last_2_cycles)
            session_for_history_based_assortment = previous_sessions.loc[
                previous_sessions['plan_fk'].isin(required_cycles)]['session_id'].tolist()
            if session_for_history_based_assortment:
                if required_template_group:
                    return self.get_products_distributed_in_sessions(session_for_history_based_assortment,
                                                                     required_template_group)
                else:
                    return self.get_products_distributed_in_sessions(session_for_history_based_assortment)
            else:
                return self._get_empty_assortment()

    def get_current_cycle(self, visit_date):
        query = """ select pd.pk as plan_fk
                     from static.plan_details pd
                     where '{}' between pd.start_date and pd.end_date
                        """.format(visit_date)
        return pd.read_sql_query(query, self.rds_conn.db)

    def get_missing_products_to_api_set(self, set_fk):
        existing_skus = self.all_products[self.all_products['product_type'].isin([SKU, POSM])]
        set_data = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == set_fk][
            'atomic_kpi_name'].unique().tolist()
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == set_fk]['kpi_fk'].iloc[0]
        not_in_db_products = existing_skus[~existing_skus['product_short_name'].isin(set_data)]
        if not_in_db_products.empty:
            return False
        else:
            self.add_new_kpi_to_static_tables(kpi_fk, not_in_db_products)
            return True

    @staticmethod
    def _is_date_valid(date_field):
        try:
            separator = '.'
            for sign in ['-', ',', '/']:
                if sign in date_field:
                    separator = sign
            m, y = date_field.split(separator)
        except ValueError:
            return False
        try:
            m = int(m)
            y = int(y)
            if not 1 <= m <= 12:
                return False
            elif not 1 <= y <= 99 and not 2000 <= y <= 3000:
                return False
            else:
                return True
        except TypeError:
            return False
        except ValueError:
            return False

    def _get_formate_date(self, row):
        """
        This gets the date table and create a valid date string so we can use it later in agregation
        """
        if not pd.isnull(row['date_value']):
            try:
                f_date = pd.datetime.strftime(row['date_value'], '%Y.%m.%d')
            except ValueError:
                Log.error("The date {} is not possible".format(row['date_value']))
                return "2018.01.01"
            return datetime.strptime(f_date, '%Y.%m.%d')
        if row['original_value']:
            date_field = row['original_value']
            if self._is_date_valid(row['original_value']):
                separator = '.'
                for sign in ['-', ',', '/']:
                    if sign in date_field:
                        separator = sign
                m, y = date_field.split(separator)
                if len(str(m)) < 2:
                    m = '0{}'.format(m)
                date = '{}.{}.01'.format(y, m)
                try:
                    f_date = datetime.strptime(date, '%Y.%m.%d')
                except ValueError:
                    try:
                        f_date = datetime.strptime(date, '%y.%m.%d')
                    except:
                        return
                return f_date
        return

    def get_additional_product_data(self):
        #####
        # This queries are temporary until given in data provider.
        #####

        price_query = """SELECT   p.scene_fk as scene_fk, mpip.product_fk as product_fk, mpn.match_product_in_probe_fk as probe_match_fk,
                    mpn.value as price_value, mpas.state as number_attribute_state
            FROM
                    probedata.match_product_in_probe_number_attribute_value mpn
                    LEFT JOIN static.number_attribute_brands_scene_types mpna on mpn.number_attribute_fk = mpna.pk
                    LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpn.attribute_state_fk = mpas.pk
                    JOIN probedata.match_product_in_probe mpip ON mpn.match_product_in_probe_fk = mpip.pk
                    JOIN probedata.probe p ON p.pk = mpip.probe_fk
            WHERE
                    p.session_uid = "{0}" """.format(self.session_uid)

        date_query = """SELECT p.scene_fk as scene_fk, mpip.product_fk as product_fk, mpd.match_product_in_probe_fk as probe_match_fk, mpd.value as date_value, mpd.original_value,
                              mpas.state as date_attribute_state
                        FROM
                                probedata.match_product_in_probe_date_attribute_value mpd
                                LEFT JOIN static.date_attribute_brands_scene_types mpda ON mpd.date_attribute_fk = mpda.pk
                                LEFT JOIN static.match_product_in_probe_attributes_state mpas ON mpd.attribute_state_fk = mpas.pk
                                JOIN probedata.match_product_in_probe mpip ON mpd.match_product_in_probe_fk = mpip.pk
                                JOIN probedata.probe p ON p.pk = mpip.probe_fk
                        WHERE
                                p.session_uid = "{0}"
                                """.format(self.session_uid)
        price_attr = pd.read_sql_query(price_query, self.rds_conn.db)
        date_attr = pd.read_sql_query(date_query, self.rds_conn.db)
        matches = self.data_provider[Data.MATCHES]

        merged_pricing_data = price_attr.merge(matches[['scene_fk', 'product_fk', 'probe_match_fk']]
                                               , on=['probe_match_fk', 'product_fk', 'scene_fk'])
        merged_dates_data = date_attr.merge(matches[['scene_fk', 'product_fk', 'probe_match_fk']],
                                            on=['probe_match_fk', 'product_fk', 'scene_fk'])

        merged_pricing_data.dropna(subset=['price_value'], inplace=True)
        merged_dates_data.dropna(subset=['original_value'], inplace=True)

        if not merged_pricing_data.empty:
            try:
                merged_pricing_data = merged_pricing_data.groupby(['scene_fk', 'product_fk'], as_index=False)[['price_value']].median()
            except Exception as e:
                merged_pricing_data['price_value'] = 0
                merged_pricing_data = merged_pricing_data.groupby(['scene_fk', 'product_fk'], as_index=False)[['price_value']].median()
                Log.info('There are missing numeric values: {}'.format(e))

        if not merged_dates_data.empty:
            merged_dates_data['fixed_date'] = merged_dates_data.apply(lambda row: self._get_formate_date(row), axis=1)
            try:
                merged_dates_data = merged_dates_data.groupby(['scene_fk', 'product_fk'], as_index=False)[['fixed_date']].min()
            except Exception as e:
                merged_dates_data = merged_dates_data.groupby(['scene_fk', 'product_fk'], as_index=False)[['fixed_date']].min()
                Log.info('There is a dates integrity issue: {}'.format(e))
        else:
            merged_dates_data['fixed_date'] = None

        merged_additional_data = self.scif\
            .merge(merged_pricing_data, how='left', left_on=['scene_id', 'item_id'], right_on=['scene_fk', 'product_fk'])\
            .merge(merged_dates_data, how='left', left_on=['scene_id', 'item_id'], right_on=['scene_fk', 'product_fk'])\
            .merge(self.all_products, how='left', left_on='item_id', right_on='product_fk', suffixes=['', '_all_products'])
        #    .dropna(subset=['fixed_date', 'price_value'], how='all')

        return merged_additional_data

    def add_new_kpi_to_static_tables(self, kpi_fk, new_kpi_list):
        """
        :param set_fk: The relevant KPI set FK.
        :param new_kpi_list: a list of all new KPI's parameters.
        This function adds new KPIs to the DB ('Static' table) - both to level2 (KPI) and level3 (Atomic KPI).
        """
        cur = self.rds_conn.db.cursor()
        for i in xrange(len(new_kpi_list)):
            kpi = new_kpi_list.iloc[i]
            kpi_name = kpi['product_short_name'].encode('utf-8')
            product_fk = kpi['product_fk']
            level3_query = """
                   INSERT INTO static.atomic_kpi (kpi_fk, name, description, display_text, model_id)
                   VALUES ('{0}', '{1}', '{2}', '{3}', {4});""".format(kpi_fk, kpi_name, kpi_name,
                                                                       kpi_name, product_fk)
            cur.execute(level3_query)
        self.rds_conn.db.commit()

    def _get_empty_assortment(self):
        return pd.DataFrame(columns=['product_fk', 'product_ean_code'])

    def get_products_distributed_in_sessions(self, session_ids, template_group=None):
        in_p, params = self._get_sessions_list_as_params_and_query(session_ids, 'session_id')
        if not template_group:
            query = '''SELECT distinct p.ean_code as product_ean_code FROM reporting.scene_item_facts sif
                        join static_new.product p on p.pk = sif.item_id and p.type in('SKU','POS')
                        join probedata.scene sc on sc.pk=sif.scene_id
                        join static.template st on st.pk=sc.template_fk
                       where dist_sc = 1 and session_id in ({})'''.format(
                ",".join(str(session) for session in session_ids))
        else:
            query = '''SELECT distinct p.ean_code as product_ean_code FROM reporting.scene_item_facts sif
                        join static_new.product p on p.pk = sif.item_id and p.type in('SKU','POS')
                        join probedata.scene sc on sc.pk=sif.scene_id
                        join static.template st on st.pk=sc.template_fk
                       where sif.dist_sc = 1 and sif.session_id in ({}) and st.template_group = '{}'
                       '''.format(",".join(str(session) for session in session_ids), template_group.encode('utf-8'))
        return pd.read_sql_query(query, self.rds_conn.db)
        # return pd.read_sql_query(query, self.rds_conn.db, params=params)

    def _get_sessions_list_as_params_and_query(self, values, name):
        args = []
        params = {}
        for i in range(len(values)):
            args.append(name + '_{}'.format(i))
            params.update({name + '_{}'.format(i): values[i]})
        in_p = ', '.join(list(map(lambda x: ':{}'.format(x), args)))
        return in_p, params

    def get_previous_sessions_in_the_same_store_by_session(self, session_id, days_back):
        query = """ select pd2.pk as plan_fk, pd2.start_date as cycle_start_date, pd2.end_date as cycle_end_date,
                     s2.pk as session_id, s2.start_time, s2.visit_date from probedata.session s
                    join static.plan_details pd on s.visit_date between pd.start_date and pd.end_date
                    join static.plan_details pd2 on pd2.pk <= pd.pk
                    join probedata.session s2 on s2.visit_date between pd2.start_date
                     and pd2.end_date and s.store_fk = s2.store_fk
                    where s.pk = {}
                     and s2.visit_date between DATE_SUB(s.visit_date, INTERVAL {} DAY)
                     and DATE_SUB(s.visit_date, INTERVAL 0 DAY);
                       """.format(session_id, days_back)
        # params = {'session_id': session_id, 'daya_back': days_back}
        return pd.read_sql_query(query, self.rds_conn.db)

    # P2 KPI
    @kpi_runtime()
    def handle_priority_2(self):
        set_fk = self.kpi_static_data[
            self.kpi_static_data['kpi_set_name'] == PRICE_MONITORING]['kpi_set_fk'].iloc[0]
        mobile_set_fk = self.kpi_static_data[
            self.kpi_static_data['kpi_set_name'] == MOBILE_PRICE_MONITORING]['kpi_set_fk'].iloc[0]
        monitored_sku = self.get_sku_monitored(self.state)
        if not self.merged_additional_data.empty:
            self.merged_additional_data = self.merged_additional_data.loc[
                self.merged_additional_data['template_name'] == EFFICIENCY_TEMPLATE_NAME]
            score = self.calculate_fulfilment(monitored_sku['product_ean_code_lead'])
            efficiency_score = self.calculate_efficiency()
            self.get_raw_data()
            self.set_p2_sku_mobile_results(monitored_sku)
            if score or score == 0:
                self.write_to_db_result(set_fk, format(score, '.2f'), self.LEVEL1)
                self.write_to_db_result(fk=mobile_set_fk, result=format(score, '.2f'), level=self.LEVEL1,
                                        score_2=format(efficiency_score, '.2f'))

    def get_sku_monitored(self, state):
        monitored_skus_raw = self.get_custom_template(P2_PATH, 'SKUs')
        states = monitored_skus_raw['State'].tolist()
        if state in states:
            monitored_skus_raw = monitored_skus_raw.loc[monitored_skus['State'].apply(lambda x: pd.Series(x.split(', ')).isin([state]).any())]
        else:
            monitored_skus_raw = monitored_skus_raw.loc[monitored_skus['State'].str.upper() == 'ALL']
        monitored_skus = pd.DataFrame()
        for sku in monitored_skus_raw['ean_code'].unique().tolist():
            try:
                product_ean_code_lead = self.all_products[self.all_products['product_ean_code'] == sku]['product_ean_code_lead'].values[0]
            except Exception as e:
                Log.warning('Product ean {} is not defined in the DB'.format(sku))
                continue
            monitored_skus = monitored_skus.append({'product_ean_code_lead': product_ean_code_lead}, ignore_index=True)
        monitored_skus = monitored_skus.drop_duplicates(subset=['product_ean_code_lead'], keep='last')
        return monitored_skus

    def is_relevant_bundle(self, product_sku, bundle_sku):
        """
        This function checks if the bundle product needs to be added to the monitored_sku Data Frame.
        Logic: If the product doesn't exist in the store and the bundle product does, add it.
        :return: 1 in case of we need to add the bundle.
        """
        filtered_scif = self.scif[(self.scif['template_name'] == EFFICIENCY_TEMPLATE_NAME)]
        if filtered_scif.loc[filtered_scif['product_ean_code'] == product_sku].empty:
            if not filtered_scif.loc[filtered_scif['product_ean_code'] == bundle_sku].empty:
                return 1
        return 0

    def calculate_fulfilment(self, monitored_products):
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == P2_FULFILMENT]['kpi_fk'].iloc[0]
        mobile_kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == P2_MOBILE_FULFILMENT]['kpi_fk'].iloc[0]
        atomic_fk = \
            self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == P2_FULFILMENT]['atomic_kpi_fk'].iloc[0]
        score = self.get_fulfilment(monitored_products)
        if score == -1:
            Log.info('No Monitored products were found.')
            return None
        else:
            self.write_to_db_result(kpi_fk, round(score, 2), self.LEVEL2)
            self.write_to_db_result(mobile_kpi_fk, round(score, 2), self.LEVEL2)
            self.write_to_db_result(atomic_fk, round(score, 2), self.LEVEL3)
            return score

    def calculate_efficiency(self):
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == P2_EFFICIENCY]['kpi_fk'].iloc[0]
        atomic_fk = \
            self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == P2_EFFICIENCY]['atomic_kpi_fk'].iloc[0]
        score = self.get_efficiency()
        self.write_to_db_result(kpi_fk, round(score, 2), self.LEVEL2)
        self.write_to_db_result(atomic_fk, round(score, 2), self.LEVEL3)
        return score

    def get_fulfilment(self, monitored_skus):
        """
        gets all the products that are monitored and calculates the percentage of those recognized.
        """
        num_of_all_monitor = len(monitored_skus)
        scif = self.scif[self.scif['template_name'] == EFFICIENCY_TEMPLATE_NAME]\
            .merge(self.all_products, how='left', left_on='product_fk', right_on='product_fk', suffixes=['', '_all_products'])
        num_of_recognized_monitor = scif[scif['product_ean_code_lead'].isin(monitored_skus)]['product_ean_code_lead']\
            .drop_duplicates().count()
        if not (scif.empty or num_of_all_monitor == 0):
            return (float(num_of_recognized_monitor) / num_of_all_monitor) * 100
        else:
            return -1

    def get_efficiency(self):
        """
        calculates the percentage of recognized skus out of all skus by facings
        """
        facing_of_all = self.scif.loc[(self.scif['template_name'] == EFFICIENCY_TEMPLATE_NAME) &
                                      (self.scif['product_type'].isin([OTHER, SKU, POSM]))]['facings'].sum()
        products_eans_with_leads = \
            self.merged_additional_data.loc[
                (self.merged_additional_data['template_name'] == EFFICIENCY_TEMPLATE_NAME) &
                (~self.merged_additional_data['fixed_date'].isnull())][
                'product_ean_code'].unique().tolist() +\
            self.merged_additional_data.loc[
                (self.merged_additional_data['template_name'] == EFFICIENCY_TEMPLATE_NAME) &
                (~self.merged_additional_data['fixed_date'].isnull())][
                'product_ean_code_lead'].unique().tolist()
        facing_of_recognized = self.scif[(self.scif['template_name'] == EFFICIENCY_TEMPLATE_NAME) &
                                         (self.scif['product_ean_code'].isin(products_eans_with_leads))]['facings'].sum()
        return (facing_of_recognized / float(facing_of_all)) * 100 if facing_of_all else 0

    def get_raw_data(self):
        for index in xrange(len(self.merged_additional_data)):
            row = self.merged_additional_data.iloc[index]
            product_for_db = row['product_ean_code_lead']
            if product_for_db is None:
                continue

            if pd.isnull(row['price_value']):  # None
                price_value_for_db = 0
            else:
                price_value_for_db = format(row['price_value'], '.2f')

            if pd.isnull(row['fixed_date']):  # None
                formatted_date_for_db = '0'
            else:
                formatted_date_for_db = row['fixed_date'].strftime('%m.%Y')

            self.write_to_db_result_for_api(score=price_value_for_db, level=self.LEVEL3, level3_score=None,
                                            kpi_set_name=P2_SET_PRICE,
                                            kpi_name=P2_RAW_DATA,
                                            atomic_kpi_name=product_for_db)
            self.write_to_db_result_for_api(score=formatted_date_for_db, level=self.LEVEL3, level3_score=None,
                                            kpi_set_name=P2_SET_DATE,
                                            kpi_name=P2_RAW_DATA,
                                            atomic_kpi_name=product_for_db)

        # using P4 save function to save None without problems so will add set name to api.
        self.write_to_db_result_for_api(score=None, level=self.LEVEL1, level3_score=None, kpi_set_name=P2_SET_DATE)

    def set_p2_sku_mobile_results(self, monitored_sku):
        new_products = False
        set_name = MOBILE_PRICE_MONITORING
        kpi_fk = MOBILE_PRICE_MONITORING_KPI_FK
        try:
            existing_skus = self.all_products[(self.all_products['product_type'].isin([OTHER, SKU, POSM])) &
                                              (self.all_products['product_ean_code'].isin(monitored_sku['product_ean_code_lead'].values))]
            set_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['atomic_kpi_name'].unique().tolist()
            not_in_db_products = existing_skus[~existing_skus['product_short_name'].isin(set_data)]
            if not_in_db_products.any:
                self.add_new_kpi_to_static_tables(kpi_fk, not_in_db_products)
                new_products = True
        except Exception as e:
            Log.info('Updating Mobile price sets failed')
        if new_products:
            self.kpi_static_data = self.get_kpi_static_data()
            self.kpi_static_data['kpi_name'] = self.encode_column_in_df(self.kpi_static_data, 'kpi_name')
            self.kpi_static_data['atomic_kpi_name'] = self.encode_column_in_df(self.kpi_static_data, 'atomic_kpi_name')
        for index in xrange(len(monitored_sku)):
            row = monitored_sku.iloc[index]
            try:
                product_name = self.all_products[self.all_products['product_ean_code'] == row.product_ean_code_lead][
                    'product_short_name'].drop_duplicates().values[0]
            except Exception as e:
                Log.warning('Product ean {} is not defined in the DB'.format(row.leading))
                continue
            try:
                kpi_fk = self.kpi_static_data[(self.kpi_static_data['atomic_kpi_name'] == product_name) &
                                              (self.kpi_static_data['kpi_set_name'] == set_name)][
                    'atomic_kpi_fk'].drop_duplicates().values[0]

                score = 0 if self.merged_additional_data[self.merged_additional_data[
                                                             'product_ean_code_lead'] == row.product_ean_code_lead]\
                    .empty else 1

                result = self.merged_additional_data[self.merged_additional_data[
                                                         'product_ean_code_lead'] == row.product_ean_code_lead][
                    'price_value']
                if result.empty:
                    result = 0
                elif pd.isnull(result.values[0]):  # None
                    result = 0
                else:
                    result = format(result.values[0], '.2f')

                result2 = self.merged_additional_data[self.merged_additional_data[
                                                          'product_ean_code_lead'] == row.product_ean_code_lead][
                    'fixed_date']
                if result2.empty:
                    result2 = '0'
                elif pd.isnull(result2.values[0]):  # None
                    result2 = '0'
                else:
                    result2 = pd.to_datetime(str(result2.values[0])).strftime('%m.%Y')

                self.write_to_db_result(fk=kpi_fk, result=result, level=self.LEVEL3, score=score, result_2=result2)
            except Exception as e:
                Log.error('{}'.format(e))

    # P3 KPI
    @kpi_runtime()
    def handle_priority_3(self):

        scenes = self.scif['scene_fk'].unique().tolist()

        sections_template_data = parse_template(P3_TEMPLATE_PATH, 'Sections')
        sections_template_data['fixture'] = self.encode_column_in_df(sections_template_data, 'fixture')

        sas_zone_template_data = parse_template(P3_TEMPLATE_PATH, 'P3 SAS zone')
        sas_zone_template_data['fixture'] = self.encode_column_in_df(sas_zone_template_data, 'fixture')

        sections_products_template_data = parse_template(P3_TEMPLATE_PATH, 'SKU_Lists for sections')\
            .merge(self.all_products, how='left', on='product_ean_code', suffixes=['', '_all_products'])
        # check product ean codes from the template
        for product_ean_code in sections_products_template_data['product_ean_code'].unique().tolist():
            try:
                self.all_products[self.all_products['product_ean_code'] == product_ean_code]['product_fk'].values[0]
            except Exception as e:
                Log.warning('Product ean {} is not defined in the DB from SKU_Lists for sessions template'.format(product_ean_code))

        priorities_template_data = parse_template(P3_TEMPLATE_PATH, 'Share priority')\
            .merge(self.all_products, how='left', on='product_ean_code', suffixes=['', '_all_products'])
        # check product ean codes from the template
        for product_ean_code in priorities_template_data['product_ean_code'].unique().tolist():
            try:
                self.all_products[self.all_products['product_ean_code'] == product_ean_code]['product_fk'].values[0]
            except Exception as e:
                Log.warning('Product ean {} is not defined in the DB for Share priority'.format(product_ean_code))

        sequence_template_data = parse_template(P3_TEMPLATE_PATH, 'Sequence list')\
            .merge(self.all_products, how='left', on='product_ean_code', suffixes=['', '_all_products'])
        # check product ean codes from the template
        for product_ean_code in sequence_template_data['product_ean_code'].unique().tolist():
            try:
                self.all_products[self.all_products['product_ean_code'] == product_ean_code]['product_fk'].values[0]
            except Exception as e:
                Log.warning('Product ean {} is not defined in the DB for Sequence list'.format(product_ean_code))

        if self.state in sections_template_data['State'].unique().tolist():
            state_for_calculation = self.state
        else:
            state_for_calculation = 'ALL'

        for scene in scenes:

            if not self.scif.loc[self.scif['scene_fk'] == scene]['template_group'].values[0] == EXIT_TEMPLATE_GROUP:
                continue

            self.sas_zones_scores_dict = {}
            sections_statuses = {}
            template_name = self.scif[self.scif['scene_fk'] == scene]['template_name'].values[0]
            fixture = self.templates.loc[self.templates['template_name'] == template_name]['additional_attribute_1'].values[0]
            if fixture not in sections_template_data['fixture'].unique().tolist():
                continue

            fixture_name_for_db = self.check_fixture_past_present_in_visit(fixture)

            relevant_sections_data = sections_template_data.loc[
                (sections_template_data['store_attribute_3'] == self.scif['additional_attribute_3'].values[0]) &
                (sections_template_data['fixture'] == fixture) &
                (sections_template_data['State'] == state_for_calculation)]
            relevant_sections_data[['section_number']] = relevant_sections_data[['section_number']].astype(int)

            relevant_sas_zone_data = sas_zone_template_data.loc[
                (sas_zone_template_data['store_attribute_3'] == self.scif['additional_attribute_3'].values[0]) &
                (sas_zone_template_data['fixture'] == fixture)]

            scene_products_matrix = self.position_graph\
                .transform_entity_matrix_to_df(self.position_graph.get_entity_matrix(scene, 'product_name'))

            if not relevant_sas_zone_data.empty:
                self.check_sas_zone_in_fixture(scene_products_matrix, relevant_sas_zone_data, fixture)

            for section in sorted(relevant_sections_data['section_number'].unique().tolist()):

                section_data = relevant_sections_data.loc[relevant_sections_data['section_number'] == section]
                section_name = section_data['section_name'].values[0]
                start_sequence, end_sequence, start_shelf, end_shelf = self.get_section_limits(section_data)

                if section_data['Above SAS zone?'].values[0] != 'N':
                    shelf_number = 'shelf_number'
                else:
                    shelf_number = 'shelf_number_from_bottom'

                section_shelf_data = self.match_product_in_scene[
                    (self.match_product_in_scene['scene_fk'] == scene) &
                    (self.match_product_in_scene[shelf_number].between(start_shelf, end_shelf))]\
                    .merge(self.all_products, how='left', left_on='product_fk', right_on='product_fk', suffixes=['', '_all_products'])
                section_shelf_data_all = self.get_absolute_sequence(section_shelf_data)
                if section_shelf_data_all.empty:
                    Log.info('Section {} has no matching positions in scene {}'.format(section_name, scene))
                    continue

                section_shelf_data = section_shelf_data_all.loc[section_shelf_data_all['sequence'].between(start_sequence, end_sequence)]
                if section_shelf_data.empty:
                    Log.info('Section {} has no matching positions in scene {}'.format(section_name, scene))
                    continue

                # Initial pass values
                no_competitors = True
                sku_presence_passed = False
                sku_sequence_passed = False
                no_empties = False
                sku_repeating_passed = False
                misplaced_products = []

                # Checking Competitors
                if len(section_shelf_data[~((section_shelf_data['manufacturer_name'] == BAT) |
                                            (section_shelf_data['product_type'].isin([EMPTY])))]['manufacturer_name'].unique().tolist()) > 0:
                    no_competitors = False

                else:  # No Competitors

                    # Checking SKU Presence
                    outside_shelf_data = self.match_product_in_scene\
                        .merge(self.scene_info[['scene_fk', 'template_name']], how='left', left_on='scene_fk', right_on='scene_fk')
                    outside_shelf_data = outside_shelf_data[
                        ~(
                                (outside_shelf_data['scene_fk'] == scene) &
                                (outside_shelf_data[shelf_number].between(start_shelf, end_shelf))
                        ) &
                        (
                                (outside_shelf_data['template_name'].str.contains(EXIT_TEMPLATE_NAME)) |
                                (outside_shelf_data['template_name'] == EXIT_STOCK_NAME)
                        )]\
                        .merge(self.all_products, how='left', left_on='product_fk', right_on='product_fk', suffixes=['', '_all_products'])\
                        .append(section_shelf_data_all.loc[~(section_shelf_data_all['sequence'].between(start_sequence, end_sequence))], ignore_index=True)

                    specific_section_products_template = sections_products_template_data\
                        .loc[sections_products_template_data['Section'] == str(int(float(section)))]
                    section_products = specific_section_products_template['product_ean_code_lead'].unique().tolist()

                    sku_presence_passed = self.check_sku_presence(specific_section_products_template,
                                                                  section_shelf_data, outside_shelf_data)

                    if sku_presence_passed:

                        # Checking SKU Sequence and Empties
                        last_prod_seq_ind = 0
                        sku_sequence_passed = True
                        no_empties = True
                        for sequence in sorted(section_shelf_data['sequence'].unique().tolist()):

                            product_ean_code = section_shelf_data\
                                .loc[section_shelf_data['sequence'] == sequence]['product_ean_code_lead'].values[0]
                            product_type = section_shelf_data\
                                .loc[section_shelf_data['sequence'] == sequence]['product_type'].values[0]
                            manufacturer_name = section_shelf_data\
                                .loc[section_shelf_data['sequence'] == sequence]['manufacturer_name'].values[0]

                            if manufacturer_name == BAT and product_type == OTHER:
                                continue

                            if product_type == EMPTY:
                                no_empties = False

                            if product_ean_code in sequence_template_data['product_ean_code_lead'].unique().tolist():
                                prod_seq_ind = sequence_template_data\
                                    .loc[sequence_template_data['product_ean_code_lead'] == product_ean_code]['index'].values[0]
                                if not (int(prod_seq_ind) >= int(last_prod_seq_ind)):
                                    sku_sequence_passed = False
                                    break
                                last_prod_seq_ind = prod_seq_ind
                            elif manufacturer_name == BAT and product_type in (SKU, POSM):
                                Log.warning('Product ean {} is not configured in Sequence list template'.format(product_ean_code))
                                sku_sequence_passed = False
                                break

                        if sku_sequence_passed:

                            # Checking SKU Repeating
                            if specific_section_products_template['product_ean_code_lead'].tolist() == \
                                    specific_section_products_template['Index (Duplications priority)'].tolist():
                                priorities_section = \
                                    specific_section_products_template[['product_ean_code_lead', 'Index (Duplications priority)']]
                            else:
                                priorities_section = \
                                    priorities_template_data[['product_ean_code_lead', 'Index (Duplications priority)']]

                            sku_repeating_passed = self.check_sku_repeating(section_shelf_data, priorities_section)

                    else:  # SKU Presence failed

                        # Defining misplaced products
                        misplaced_products_eans = \
                            list(set(section_shelf_data['product_ean_code_lead']).difference(section_products))
                        misplaced_products = \
                            section_shelf_data[section_shelf_data['product_ean_code_lead'].isin(misplaced_products_eans)][
                                'product_name'].unique().tolist()

                # Initial score values
                sku_presence_score = 0
                sku_sequence_score = 0
                sku_sequence_score_2 = 'N'
                sku_repeating_score = 0
                sku_repeating_score_2 = 'N'
                section_score = 0
                section_score_2 = 'FAIL'

                misplaced_products_result = ','.join([product_name.replace("'", "\\'") for product_name in misplaced_products])

                # Defining section scores
                section_status = 0
                if no_competitors:
                    section_status = 1

                    if sku_presence_passed:
                        sku_presence_score = 1
                        section_status = 2

                        if sku_sequence_passed:
                            sku_sequence_score = 1
                            sku_sequence_score_2 = 'Y'
                            if not no_empties:
                                section_status = 3
                            else:
                                section_status = 4

                                if sku_repeating_passed:
                                    sku_repeating_score = 1
                                    sku_repeating_score_2 = 'Y'
                                    section_status = 5

                                    section_score = 1
                                    section_score_2 = 'PASS'

                sections_statuses[section] = section_status

                # Saving to regular set
                self.save_level2_and_level3(SK, self.SKU_PRESENCE_KPI_NAME, result=misplaced_products_result,
                                            score=sku_presence_score, score_2=section_status,
                                            level_3_only=True, level2_name_for_atomic=fixture_name_for_db,
                                            model_id=section_name)
                self.save_level2_and_level3(SK, self.SKU_SEQUENCE_KPI_NAME, result=None,
                                            score=sku_sequence_score, score_2=sku_sequence_score_2,
                                            level_3_only=True, level2_name_for_atomic=fixture_name_for_db,
                                            model_id=section_name)
                self.save_level2_and_level3(SK, self.SKU_REPEATING_KPI_NAME, result=None,
                                            score=sku_repeating_score, score_2=sku_repeating_score_2,
                                            level_3_only=True, level2_name_for_atomic=fixture_name_for_db,
                                            model_id=section_name)
                self.save_level2_and_level3(SK, section_name, result=None,
                                            score=section_score, score_2=section_score_2,
                                            level_3_only=True, level2_name_for_atomic=fixture_name_for_db)

                # Saving to API set
                self.write_to_db_result_for_api(score=misplaced_products_result, level=self.LEVEL3, kpi_set_name=SK_RAW_DATA,
                                                kpi_name=SK_RAW_DATA,
                                                atomic_kpi_name=self.API_SKU_PRESENCE_KPI_NAME.format(
                                                    fixture=fixture_name_for_db,
                                                    section_name=section_name),
                                                level3_score=sku_presence_score)
                self.write_to_db_result_for_api(score=None, level=self.LEVEL3, kpi_set_name=SK_RAW_DATA,
                                                kpi_name=SK_RAW_DATA,
                                                atomic_kpi_name=self.API_SKU_SEQUENCE_KPI_NAME.format(
                                                    fixture=fixture_name_for_db,
                                                    section_name=section_name),
                                                level3_score=sku_sequence_score)
                self.write_to_db_result_for_api(score=None, level=self.LEVEL3, kpi_set_name=SK_RAW_DATA,
                                                kpi_name=SK_RAW_DATA,
                                                atomic_kpi_name=self.API_SKU_REPEATING_KPI_NAME.format(
                                                    fixture=fixture_name_for_db,
                                                    section_name=section_name),
                                                level3_score=sku_repeating_score)
                self.write_to_db_result_for_api(score=None, level=self.LEVEL3, kpi_set_name=SK_RAW_DATA,
                                                kpi_name=SK_RAW_DATA,
                                                atomic_kpi_name=self.API_SECTION_KPI_NAME.format(
                                                    fixture=fixture_name_for_db,
                                                    section_name=section_name),
                                                level3_score=section_score)

            # Equipment level results
            if sections_statuses:
                fixture_score = min(sections_statuses.values())
            else:
                fixture_score = 0
            self.fixtures_statuses_dict[fixture] = fixture_score
            self.save_level2_and_level3(SK, fixture_name_for_db, result=fixture_score, level_2_only=True)
            self.write_to_db_result_for_api(score=fixture_score, level=self.LEVEL3, kpi_set_name=SK_RAW_DATA,
                                            kpi_name=SK_RAW_DATA,
                                            atomic_kpi_name=self.API_EQUIPMENT_KPI_NAME.format(
                                                fixture=fixture_name_for_db))

            fixture_sas_zone_score = self.calculate_sas_zone_compliance(fixture_name_for_db, self.sas_zones_scores_dict, scene)
            self.save_level2_and_level3(SAS, fixture_name_for_db, result=fixture_sas_zone_score, level_2_only=True)
            self.write_to_db_result_for_api(score=None, level=self.LEVEL3,
                                            level3_score=fixture_sas_zone_score, kpi_set_name=SAS_RAW_DATA,
                                            kpi_name=SAS_RAW_DATA,
                                            atomic_kpi_name=self.API_EQUIPMENT_KPI_NAME.format(
                                                fixture=fixture_name_for_db))

        # Store level results
        if self.sas_zone_statuses_dict:
            sas_zone_score = str(sum(self.sas_zone_statuses_dict.values())/100) + '/' +\
                             str(len(self.sas_zone_statuses_dict.values()))
        else:
            sas_zone_score = str(0) + '/' + str(len(self.sas_zone_statuses_dict.values()))

        if self.fixtures_statuses_dict:
            sk_score = min(self.fixtures_statuses_dict.values())  # todo validate this assumption
        else:
            sk_score = 0

        self.save_level1(SK, score=sk_score)
        self.save_level1(SK_RAW_DATA, score=sk_score)
        self.save_level1(SAS, score=sas_zone_score)
        self.save_level1(SAS_RAW_DATA, score=sas_zone_score)

    def check_sku_repeating(self, section_shelf_data, priorities_section):
        """
        The function checks if the section has a product with more facings than other, despite its priority lower
        :param section_shelf_data: match_product_in_scene relevant data
        :param priorities_section: the priorities from the match template
        :return: true if all the products' facings are right
        """

        if len(priorities_section['Index (Duplications priority)'].unique().tolist()) == 1:
            return True

        products_to_check = section_shelf_data \
            .loc[(section_shelf_data['manufacturer_name'] == BAT) &
                 (section_shelf_data['product_type'].isin([SKU, POSM]))]['product_ean_code_lead'].unique().tolist()
        if not products_to_check:
            return False

        section_facings_histogram = []
        for product_ean_code in set(products_to_check):
            if product_ean_code in priorities_section['product_ean_code_lead'].tolist():
                priority = priorities_section\
                    .loc[priorities_section['product_ean_code_lead'] == product_ean_code]['Index (Duplications priority)'].values[0]
                facings = section_shelf_data\
                    .loc[section_shelf_data['product_ean_code_lead'] == product_ean_code]['product_fk'].count()
                section_facings_histogram\
                    .append({'priority': priority, 'product_ean_code': product_ean_code, 'facings': facings})
            else:
                Log.warning('Product ean {} is not configured in Share priority template'.format(product_ean_code))
        section_facings_histogram = pd.DataFrame(section_facings_histogram)

        min_max_facings = pd.DataFrame(columns=['priority', 'max', 'min'])
        for current_priority in section_facings_histogram['priority'].unique().tolist():
            current_priority_data = section_facings_histogram[section_facings_histogram['priority'] == current_priority]
            min_max_facings = min_max_facings.append({'priority': int(current_priority_data['priority'].values[0]),
                                                      'max': current_priority_data['facings'].max(),
                                                      'min': current_priority_data['facings'].min()}, ignore_index=True)
        min_max_facings = min_max_facings.sort_values(by='priority', ascending=False)

        last_priority = 0
        last_min_facings = 0
        is_not_first = False
        for i, row in min_max_facings.iterrows():
            current_priority, current_max_facings = row['priority'], row['max']
            if is_not_first:
                if not ((last_priority - current_priority == 1 and current_max_facings <= last_min_facings) or
                        (last_priority - current_priority > 1 and current_max_facings < last_min_facings) or
                        (last_min_facings == current_max_facings == 1)):
                    return False
            else:
                is_not_first = True
            last_priority = current_priority
            last_min_facings = row['min']

        return True

    def check_sku_presence(self, section_template, products_on_shelf, products_outside):

        if section_template['product_fk_lead'].empty:
            return False  # Empty template

        products_on_shelf = products_on_shelf[~(products_on_shelf['product_type'].isin([EMPTY, OTHER]))]
        if products_on_shelf.empty:
            return False  # Empty section

        mandatory_fks_template = \
            section_template[section_template['Mandatory'] == 'Yes']['product_fk_lead'].unique().tolist()
        mandatory_fks_section = \
            products_on_shelf[products_on_shelf['product_fk_lead'].isin(mandatory_fks_template)]['product_fk_lead'].unique().tolist()
        if len(mandatory_fks_section) == 0:

            products_outside = products_outside[~(products_outside['product_type'].isin([EMPTY, OTHER]))]
            mandatory_fks_outside = \
                products_outside[products_outside['product_fk_lead'].isin(mandatory_fks_template)]['product_fk_lead'].unique().tolist()
            if len(mandatory_fks_outside) == 0:

                brands_template = \
                    self.all_products[self.all_products['product_fk_lead'].isin(mandatory_fks_template)][
                        'brand_name'].unique().tolist()
                brands_section = products_on_shelf['brand_name'].unique().tolist()
                if set(brands_template).issuperset(brands_section):
                    return True
                else:

                    brands_outside = products_outside[products_outside['brand_name'].isin(brands_template)]['brand_name'].unique().tolist()
                    if len(brands_outside) == 0:

                        manufacturers_section = products_on_shelf['manufacturer_name'].unique().tolist()
                        if BAT in manufacturers_section and len(manufacturers_section) == 1:
                            return True
                        else:
                            return False

                    else:
                        return False

            else:
                return False

        elif len(mandatory_fks_section) == len(mandatory_fks_template):

            products_fks_template = section_template['product_fk_lead'].unique().tolist()
            products_fks_section = products_on_shelf[~(products_on_shelf['product_type'].isin([EMPTY, OTHER]))][
                'product_fk_lead'].unique().tolist()
            if set(products_fks_template).issuperset(products_fks_section):
                return True
            else:
                return False

        else:

            mandatory_fks_template = filter(lambda x: x not in mandatory_fks_section, mandatory_fks_template)
            mandatory_fks_outside = products_outside[products_outside['product_fk_lead'].isin(mandatory_fks_template)][
                'product_fk_lead'].unique().tolist()
            if len(mandatory_fks_outside) == 0:
                return True
            else:
                return False

    def check_sas_zone_in_fixture(self, product_matrix, section_data, fixture):
        start_sequence, end_sequence, start_shelf, end_shelf = self.get_section_limits(section_data, sas_flag=True)
        end_shelf += 1
        if start_shelf == 0 and end_shelf == 0:
            no_competitors_in_sas_zone_flag = False
            sas_zone_score = 0
        else:
            no_competitors_in_sas_zone = self.check_no_competitors_in_sas_zone(product_matrix,
                                                                               start_shelf, end_shelf,
                                                                               start_sequence, end_sequence)
            if no_competitors_in_sas_zone:
                no_competitors_in_sas_zone_flag = True
                sas_zone_score = 1
            else:
                no_competitors_in_sas_zone_flag = False
                sas_zone_score = 0
        self.sas_zones_scores_dict[fixture] = sas_zone_score
        return no_competitors_in_sas_zone_flag

    def get_absolute_sequence(self, shelf_data):
        if shelf_data.empty:
            return pd.DataFrame()
        shelf_data_sequence_dict = {}
        for bay in shelf_data['bay_number'].unique().tolist():
            last_bay_seq = 0
            last_abs_seq = 0
            for previous_bay in shelf_data.loc[shelf_data['bay_number'] < bay]['bay_number'].unique().tolist():
                last_bay_seq = max(shelf_data.loc[shelf_data['bay_number'] == previous_bay][
                                       'facing_sequence_number'].unique().tolist())
                last_abs_seq += last_bay_seq
            for i, row in shelf_data.iterrows():
                if row['bay_number'] == bay:
                    current_sequence = row['facing_sequence_number']
                    shelf_data_sequence_dict[row['scene_match_fk']] = {'sequence': last_abs_seq + current_sequence,
                                                                       'scene_match_fk': row['scene_match_fk']}
        shelf_data_sequence_dict_df = pd.DataFrame.from_dict(shelf_data_sequence_dict, orient='index')
        shelf_data = shelf_data.merge(shelf_data_sequence_dict_df, how='left', on=['scene_match_fk'])

        return shelf_data

    def check_fixture_past_present_in_visit(self, fixture):
        if fixture in self.fixtures_presence_histogram.keys():
            fixture_name_for_db = fixture + ' - ' + str(self.fixtures_presence_histogram[fixture])
            self.fixtures_presence_histogram[fixture] += 1
        else:
            self.fixtures_presence_histogram[fixture] = 1
            fixture_name_for_db = fixture

        return fixture_name_for_db

    def get_section_limits(self, section_data, sas_flag=False):
        if not sas_flag:
            if section_data['Above SAS zone?'].values[0] == 'Y':
                start_sequence = int(section_data['start_sequence'].values[0])
                end_sequence = int(section_data['end_sequence'].values[0])
                start_shelf = int(section_data['start_shelf_from_top'].values[0])
                end_shelf = int(section_data['end_shelf_from_top'].values[0])
            else:
                start_sequence = int(section_data['start_sequence'].values[0])
                end_sequence = int(section_data['end_sequence'].values[0])
                start_shelf = int(section_data['start_shelf_from_bottom'].values[0])
                end_shelf = int(section_data['end_shelf_from_bottom'].values[0])
        else:
            start_sequence = int(section_data['start_sequence'].values[0])
            end_sequence = section_data['end_sequence'].values[0]
            start_shelf = int(section_data['start_shelf'].values[0])
            end_shelf = int(section_data['end_shelf'].values[0])

        return start_sequence, end_sequence, start_shelf, end_shelf

    def check_no_competitors_in_sas_zone(self, bay_data, start_shelf, end_shelf, start_seq, end_seq):
        # shelf_rlv_data = bay_data.loc[
        #     (bay_data['shelf_number'] == shelf) & (bay_data['facing_sequence_number'] <= end_seq)
        #     & (bay_data['facing_sequence_number'] >= start_seq)]
        # shelf_rlv_data = shelf_rlv_data.merge(self.all_products, on=['product_fk'])
        # if len(shelf_rlv_data[~shelf_rlv_data['product_type'].isin([EMPTY, OTHER])]['manufacturer_name'].unique().tolist()) == 1 and BAT in shelf_rlv_data[
        #     'manufacturer_name'].values.tolist():
        #     return True
        # else:
        #     return False
        if end_seq.upper() == 'ALL':
            end_seq = bay_data['sequence'].max()
        else:
            end_seq = int(end_seq)
        shelf_rlv_data = bay_data.loc[
            (bay_data['shelf_number'].between(start_shelf, end_shelf)) &
            (bay_data['sequence'].between(start_seq, end_seq))]
        shelf_rlv_data = shelf_rlv_data.merge(self.all_products, on=['product_name'])
        if len(shelf_rlv_data[~shelf_rlv_data['product_type'].isin([EMPTY])][
                   'manufacturer_name'].unique().tolist()) == 1 and BAT in shelf_rlv_data[
            'manufacturer_name'].values.tolist():
            return True
        else:
            return False

    def calculate_sas_zone_compliance(self, fixture, sas_zone_scores_dict, scene):
        if fixture in sas_zone_scores_dict.keys():
            no_competitors_status = sas_zone_scores_dict[fixture]
        else:
            no_competitors_status = 0
        match_display = self.match_display_in_scene
        match_display['display_name'] = self.encode_column_in_df(match_display, 'display_name')
        scene_match_display = match_display[match_display['scene_fk'] == scene]
        sas_template = parse_template(P3_TEMPLATE_PATH, 'SAS Zone Compliance')
        sas_template['Equipment'] = self.encode_column_in_df(sas_template, 'Equipment')
        sas_template['display_name'] = self.encode_column_in_df(sas_template, 'display_name')
        if self.state in sas_template['State'].unique().tolist():
            state_for_calculation = self.state
        else:
            state_for_calculation = 'ALL'
        if self.scif['additional_attribute_3'].values[0] in sas_template['attribute_3'].unique().tolist():
            attribute_3 = self.scif['additional_attribute_3'].values[0]
        else:
            attribute_3 = 'ALL'
        relevant_df = sas_template.loc[(sas_template['Equipment'] == fixture) &
                                       (sas_template['attribute_3'] == attribute_3) &
                                       (sas_template['State'] == state_for_calculation)]
        if relevant_df.empty:
            status = 0
        else:
            status = 100
            for display in relevant_df['display_name'].unique().tolist():
                relevant_display = relevant_df.loc[relevant_df['display_name'] == display].iloc[0]
                if not relevant_display.empty:
                    if set(relevant_display['Names of template in SR'].replace(".jpg", "").replace(".png", "").split(", ")) \
                            & set(scene_match_display['display_name'].unique().tolist()):
                        presence_score = 100
                    else:
                        presence_score = 0
                        status = 0
                    self.save_level2_and_level3(SAS, display, result=None, score=presence_score, level_3_only=True,
                                                level2_name_for_atomic=fixture)
                    self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=presence_score,
                                                    kpi_set_name=SAS_RAW_DATA, kpi_name=SAS_RAW_DATA,
                                                    atomic_kpi_name=self.API_DISPLAY_KPI_NAME.format(
                                                        fixture=fixture, display=display))
                    # for sr_display in relevant_display['Names of template in SR'].split(", "):
                    #     if sr_display in scene_match_display['display_name'].unique().tolist():
                    #         presence_score = 100
                    #     else:
                    #         presence_score = 0
                    #         status = 0
                    #     self.save_level2_and_level3(SAS, display, result=None, score=presence_score, level_3_only=True,
                    #                                 level2_name_for_atomic=fixture)
                    #     self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=presence_score,
                    #                                     kpi_set_name=SAS_RAW_DATA, kpi_name=SAS_RAW_DATA,
                    #                                     atomic_kpi_name=self.API_DISPLAY_KPI_NAME.format(
                    #                                         fixture=fixture, display=display.encode('utf-8')))
        self.sas_zone_statuses_dict[fixture] = status
        self.save_level2_and_level3(SAS, self.NO_COMPETITORS_IN_SAS_ZONE, result=None, score=no_competitors_status,
                                    level_3_only=True,
                                    level2_name_for_atomic=fixture)
        self.write_to_db_result_for_api(score=None, level=self.LEVEL3, level3_score=no_competitors_status,
                                        kpi_set_name=SAS_RAW_DATA, kpi_name=SAS_RAW_DATA,
                                        atomic_kpi_name=self.API_NO_COMPETITORS_IN_SAS_ZONE.format(
                                            fixture=fixture))
        return status

    # P4 KPI
    @kpi_runtime()
    def handle_priority_4(self):
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == POSM_AVAILABILITY]['kpi_set_fk'].iloc[0]
        posm_template = self.get_custom_template(P4_PATH, 'Availability')
        posm_template['KPI Display Name'] = self.encode_column_in_df(posm_template, 'KPI Display Name')
        posm_template['Group Name'] = self.encode_column_in_df(posm_template, 'Group Name')
        posm_template['Atomic KPI Name'] = self.encode_column_in_df(posm_template, 'Atomic KPI Name')
        posm_template['Product Name'] = self.encode_column_in_df(posm_template, 'Product Name')
        if self.state in posm_template['State'].unique().tolist():
            state_for_calculation = self.state
        else:
            state_for_calculation = 'ALL'
        posm_template = posm_template[posm_template['State'] == state_for_calculation]
        attribute_3 = self.scif['additional_attribute_3'].iloc[0]
        attribute_3_in_template = posm_template[ATTRIBUTE_3].unique()
        if attribute_3 not in attribute_3_in_template:
            attribute_3 = 'OTHER'
        posm_template = posm_template.loc[posm_template[ATTRIBUTE_3].isin(['ALL', attribute_3])]
        posm_template = posm_template.loc[posm_template['Product Name'] != '']
        score = 0
        self.posm_in_session = self.tools.get_posm_availability()
        equipment_in_store = 0
        equipments = posm_template['KPI Display Name'].unique().tolist()
        self.save_level1(set_name=P4_API_SET, score=None)
        for equipment in equipments:
            if equipment in self.scif['additional_attribute_1'].unique().tolist():
                equipment_template = posm_template.loc[posm_template['KPI Display Name'] == equipment]
                scene_type = equipment_template['Template Group'].values[0]
                scenes = self.scif.loc[(self.scif['additional_attribute_1'] == equipment) &
                                       (self.scif['template_group'] == scene_type)]['scene_id'].unique()
                for scene in scenes:
                    equipment_in_store += 1
                    # this will change the display name for the db according to instances:
                    #  number of scenes with relevant equipment
                    if equipment in self.p4_display_count:
                        self.p4_display_count[equipment] += 1
                        count = self.p4_display_count[equipment]
                    else:
                        self.p4_display_count[equipment] = 1
                        count = self.p4_display_count[equipment]

                    if count == 1:
                        equipment_to_db = equipment
                    else:
                        equipment_to_db = '{} # {}'.format(equipment, count)

                    if not equipment_template.empty:
                        try:
                            result = self.calculate_passed_equipments(equipment_template, equipment_to_db, scene)  # has equipment passed?
                        except IndexError:
                            Log.warning('The KPI is not in the DB yet')
                            result = 0
                    else:
                        result = 0

                    score = score + 1 if result else score
        set_score = '{}/{}'.format(score, equipment_in_store)
        self.write_to_db_result(set_fk, set_score, level=self.LEVEL1)
        return

    def calculate_passed_equipments(self, equipment_template, equipment_name, scene_fk):
        """
        Gets the count for posm products in specific equipment.
        :param equipment_template: a data frame filtered by equipment
        :return: num of passed posm in group
        """
        groups = equipment_template['Group Name'].unique().tolist()
        group_counter = 0
        for group in groups:
            group_template = equipment_template.loc[equipment_template['Group Name'] == group]
            if not group_template.empty:
                result = self.calculate_passed_groups(group_template, equipment_name, scene_fk)
            else:
                result = 0
            group_counter = group_counter + 1 if result else group_counter

        kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_name'] == POSM_AVAILABILITY) &
                                          (self.kpi_static_data['kpi_name'] == equipment_name) &
                                          (~self.kpi_static_data['atomic_kpi_fk'].isnull())]['kpi_fk'].iloc[0]
        score = 1 if group_counter == len(groups) else 0
        threshold = len(groups)
        self.write_to_db_result(kpi_fk, result=group_counter, score_2=score, score_3=threshold,
                                level=self.LEVEL2)
        return score

    def calculate_passed_groups(self, group_template, equipment_name, scene_fk):
        """
        Gets the count for posm products in specific equipment.
        :param group_template: a data frame filtered by group
        :return: num of passed posm in group
        """
        group_name = group_template['Group Name'].iloc[0]
        posm_counter = 0
        for i in xrange(len(group_template)):
            row = group_template.iloc[i]
            if row['Product Name']:
                result = self.calculate_specific_posm(row, equipment_name, group_name, scene_fk)
                posm_counter += 1 if result else 0
        kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_name'] == POSM_AVAILABILITY) &
                                          (self.kpi_static_data['kpi_name'] == equipment_name) &
                                          (self.kpi_static_data['atomic_kpi_name'] == group_name)][
            'atomic_kpi_fk'].iloc[0]
        score = 1 if posm_counter == len(group_template) else 0
        self.write_to_db_result(kpi_fk, result=posm_counter, score=score,
                                threshold=len(group_template), level=self.LEVEL3)
        return score

    def get_posm_filters(self, filters, row):
        posm_filters = {}
        for current_filter in filters:
            value = row[current_filter]
            if value and value.upper() != 'ALL':
                if current_filter in self.filters_params:
                    posm_filters[self.filters_params[current_filter]] = value
                else:
                    posm_filters[current_filter] = row[value]
        return posm_filters

    def calculate_specific_posm(self, row, equipment_name, group_name, scene_fk):
        atomic_name = row['Atomic KPI Name']
        posm_count = 0
        possible_products = row['Product Name'].replace(".jpg", "").replace(".png", "").split(", ")
        for product in possible_products:
            filters = self.get_posm_filters(['Template Group', 'KPI Display Name'], row)
            filters['display_name'] = product
            filters['scene_id'] = scene_fk
            result = self.filter_posm_in_session(filters)
            name = '{};{};{};{}'.format(equipment_name, group_name, atomic_name, product)
            self.write_to_db_result_for_api(score=result, level=self.LEVEL3, level3_score=None,
                                            kpi_set_name=P4_API_SET,
                                            kpi_name=P4_API_SET,
                                            atomic_kpi_name=name)
            posm_count = posm_count + 1 if result else posm_count

        score = 1 if posm_count else 0
        try:
            kpi_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_name'] == POSM_AVAILABILITY) &
                                              (self.kpi_static_data['kpi_name'] == equipment_name) &
                                              (self.kpi_static_data['atomic_kpi_name'] == atomic_name)][
                'atomic_kpi_fk'].iloc[0]
            self.write_to_db_result(kpi_fk, result=score, score=score, level=self.LEVEL3)
        except IndexError as e:
            Log.warning("KPI {}:{}:{}:{} was not found in static.".format(POSM_AVAILABILITY,
                                                                          equipment_name, group_name, atomic_name))
        return 1 if posm_count else 0

    def filter_posm_in_session(self, filters):
        posm_df = self.posm_in_session[self.tools.get_filter_condition(self.posm_in_session, **filters)]
        if posm_df.empty:
            return 0
        else:
            return 1

    # P5 KPI
    @kpi_runtime()
    def handle_priority_5(self):
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == SHARE_OF]['kpi_set_fk'].iloc[0]
        kpi_template = self.get_custom_template(P5_PATH, 'KPIs')
        kpi_template['KPI Name(scene type attribute 1)'] = self.encode_column_in_df(
            kpi_template, 'KPI Name(scene type attribute 1)')
        score = 0
        set_score = 0
        for i in xrange(len(kpi_template)):
            row = kpi_template.iloc[i]
            kpi_name = row['KPI Name(scene type attribute 1)']

            if row['KPI Type'] == 'Share of Assortment':
                score = self.calculate_soa(row)
            elif row['KPI Type'] == 'Share of Shelf':
                score = self.calculate_sos(row)
            if row['is_set_score'] == '1':
                set_score = score

            atomic_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == kpi_name]['atomic_kpi_fk'].iloc[0]
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].iloc[0]
            self.write_to_db_result(atomic_fk, result=format(score * 100, '.2f'), score=format(score * 100, '.0f'),
                                    score_2=format(score, '.2f'), level=self.LEVEL3)
            self.write_to_db_result(kpi_fk, result=format(score, '.0f'), score_2=format(score, '.2f'), level=self.LEVEL2)
        self.write_to_db_result(set_fk, result=format(set_score * 100, '.2f'), level=self.LEVEL1)

    def calculate_soa(self, row):
        manufacturer = row['Manufacturer']
        filters = self.get_manufacturer_filters(manufacturer)
        filters['product_type'] = 'SKU'
        # filters['template_group'] = '2. На выходе'
        manufacturer_uniques = self.tools.calculate_assortment(assortment_entity='product_ean_code',
                                                               minimum_assortment_for_entity=1, **filters)
        general_filters = {'product_type': 'SKU'}
        # general_filters['template_group'] = '2. На выходе'
        other_uniques = self.tools.calculate_assortment(**general_filters)
        result = float(manufacturer_uniques) / other_uniques if other_uniques else 0
        return result

    def calculate_sos(self, row):
        manufacturer = row['Manufacturer']
        filters = self.get_manufacturer_filters(manufacturer)
        filters['product_type'] = ['SKU', 'Other']
        # filters['template_group'] = '2. На выходе'
        # general_filters = {'template_group': '2. На выходе'}
        result = self.tools.calculate_share_of_shelf(sos_filters=filters, include_empty=True)
        return result

    def get_manufacturer_filters(self, manufacturer):
        """
        This function get the manufacturer field in setup file, and insert it to the filters as needed-
         If there is a 'Not ' before the name, this name will be excluded- and only this name will be in filters.

        :param manufacturer: a string of manufacturers, divided by ','.
        :return: a dictionary of filters as tuples with include/exclude param.
        """
        manufacturer = manufacturer.split(', ')
        manufacturer_filter = []
        filters = []
        for field in manufacturer:
            if field:
                if field.startswith('Not'):
                    include = self.tools.EXCLUDE_FILTER
                    field = field.replace('Not ', '')
                    return {'manufacturer_name': (field, include)}
                else:
                    manufacturer_filter.append(field)
                filters = {'manufacturer_name': manufacturer_filter}
        return filters

    # bundle functions

    # def bundle_fk_ean(self, product_fk):
    #     """
    #     :param product_fk: number
    #     :return: list of its bundles' ean_codes
    #     """
    #     return self.all_products.loc[self.all_products['substitution_product_fk'] == product_fk][
    #         'product_ean_code'].unique().tolist()
    #
    # def bundle_fk_fk(self, product_fk):
    #     """
    #     :param product_fk: number
    #     :return: list of its bundles' fks
    #     """
    #     return self.all_products.loc[self.all_products['substitution_product_fk'] == product_fk][
    #         'product_fk'].unique().tolist()
    #
    # def bundles_fk_ean(self, products_fk):
    #     """
    #     :param products_fk: list of numbers
    #     :return: list of their bundles' ean_codes
    #     """
    #     return self.all_products.loc[self.all_products['substitution_product_fk'].isin(products_fk)][
    #         'product_ean_code'].unique().tolist()
    #
    # def bundles_fk_fk(self, products_fk):
    #     """
    #     :param products_fk: list of numbers
    #     :return: list of their bundles' fks
    #     """
    #     return self.all_products.loc[self.all_products['substitution_product_fk'].isin(products_fk)][
    #         'product_fk'].unique().tolist()
    #
    # def bundles_ean_ean(self, products_eans):
    #     """
    #     :param products_eans: list of eans
    #     :return: list of its bundles' eans
    #     """
    #     products_fks = self.all_products[self.all_products['product_ean_code'].isin(products_eans)]['product_fk'].unique().tolist()
    #     return self.bundles_fk_ean(products_fks)
    #
    # def lead_ean_ean_one(self, product_ean_code):
    #     try:
    #         bundle_lead_fk = \
    #             self.all_products.loc[self.all_products['product_ean_code'] == product_ean_code][
    #                 'substitution_product_fk'].values[0]
    #     except IndexError:
    #         Log.warning('The product "{}" is not defined in products'.format(product_ean_code))
    #         return None
    #     if bundle_lead_fk is None or np.isnan(bundle_lead_fk):
    #         bundle_lead = None
    #     else:
    #         bundle_lead = \
    #             self.all_products.loc[self.all_products['product_fk'] == bundle_lead_fk]['product_ean_code'].values[0]
    #
    #     return bundle_lead
    #
    # def leads_ean_ean(self, products_ean_code):
    #     list_of_bundles = []
    #     for product in products_ean_code:
    #         if product is None:
    #             continue
    #         bundle_lead = self.lead_ean_ean_one(product)
    #         if bundle_lead:
    #             list_of_bundles.append(bundle_lead)
    #     return list_of_bundles
    #
    # def lead_fk_ean_one(self, product_fk):
    #     bundle_lead_fk = \
    #         self.all_products.loc[self.all_products['product_fk'] == product_fk]['substitution_product_fk'].values[0]
    #     if bundle_lead_fk is None or np.isnan(bundle_lead_fk):
    #         bundle_lead = None
    #     else:
    #         bundle_lead = \
    #             self.all_products.loc[self.all_products['product_fk'] == bundle_lead_fk]['product_ean_code'].values[0]
    #
    #     return bundle_lead


    # DB FUNCTIONS

    def get_kpi_fk_by_names(self, set_name, kpi_name, atomic_name=None, model_id=None, include_atomic=False):
        if include_atomic:
            kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                            (self.kpi_static_data['kpi_name'] == kpi_name)]
            if kpi_data.empty:
                Log.warning('KPI {} in set {} is not defined in the DB'.format(kpi_name, set_name))
                return None
            kpi_fk = kpi_data['kpi_fk'].values[0]
            atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
            return kpi_fk, atomic_kpi_fk
        elif not atomic_name and not model_id:
            data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name)]['kpi_fk'].values
            if not data.any():
                Log.warning('KPI [{}] in set [{}] is not defined in the DB'.format(kpi_name, set_name))
                return None
        elif not model_id:
            data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name) &
                                        (self.kpi_static_data['atomic_kpi_name'] == atomic_name)][
                'atomic_kpi_fk'].values
            if not data.any():
                Log.warning('set [{}]: KPI [{}] with atomic [{}] is not defined in the DB'.format(
                    set_name, kpi_name, atomic_name))
                return None
        else:
            data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name) &
                                        (self.kpi_static_data['atomic_kpi_name'] == atomic_name) &
                                        (self.kpi_static_data['section'] == model_id)]['atomic_kpi_fk'].values
            if not data.any():
                Log.warning('set [{}]: KPI [{}] with atomic [{}] and model_id [{}] is not defined in the DB'.format(
                    set_name, kpi_name, atomic_name, model_id))
                return None
        return data[0]

    def write_to_db_result_for_api(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict_for_api(score, level, threshold, level3_score, **kwargs)
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

    def create_attributes_dict_for_api(self, score, level, threshold=None, level3_score=None, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            set_name = kwargs['kpi_set_name']
            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
            if score is not None:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            format(score, '.2f'), set_fk)],
                                          columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                                   'kpi_set_fk'])
            else:
                attributes = pd.DataFrame([(set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                            None, set_fk)],
                                          columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                                   'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = kwargs['kpi_name']
            kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]['kpi_fk'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        kpi_fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
            # self.kpi_results[kpi_name] = score
        elif level == self.LEVEL3:
            kpi_name = kwargs['kpi_name']
            atomic_kpi_name = kwargs['atomic_kpi_name']
            kpi_set_name = kwargs['kpi_set_name']
            kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_name'] == kpi_name) &
                                          (self.kpi_static_data['kpi_set_name'] == kpi_set_name)]['kpi_fk'].values[0]
            if level3_score is None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk'])
            elif level3_score is not None and threshold is None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            level3_score, None)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'score', 'threshold'])
            elif level3_score is None and threshold is not None:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, None)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(), score, kpi_fk,
                                            threshold, level3_score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'threshold', 'score'])
                # if kpi_set_name not in self.atomic_results.keys():
                #     self.atomic_results[kpi_set_name] = {}
                # self.atomic_results[kpi_set_name][atomic_kpi_name] = score
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def save_level1(self, set_name, score, score2=None, score3=None):
        kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]
        kpi_set_fk = kpi_data['kpi_set_fk'].values[0]
        self.write_to_db_result(kpi_set_fk, score, self.LEVEL1, score_2=score2, score_3=score3)

    def save_level2_and_level3(self, set_name, kpi_name, result, score=None, threshold=None, level_2_only=False,
                               level_3_only=False, level2_name_for_atomic=None, score_2=None, score_3=None,
                               model_id=None):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        if level_2_only:
            kpi_fk = self.get_kpi_fk_by_names(set_name, kpi_name)
            if kpi_fk:
                self.write_to_db_result(kpi_fk, result, self.LEVEL2, score_2=score_2, score_3=score_3)
        elif level_3_only:
            atomic_kpi_fk = self.get_kpi_fk_by_names(set_name, level2_name_for_atomic, kpi_name, model_id)
            if atomic_kpi_fk:
                if score is None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score_2=score_2, score_3=score_3)
                elif score is not None and threshold is None:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, score_2=score_2,
                                            score_3=score_3)
                else:
                    self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, score_2=score_2,
                                            score_3=score_3, threshold=threshold)
        else:
            kpi_fk, atomic_kpi_fk = self.get_kpi_fk_by_names(set_name, kpi_name, include_atomic=True)
            self.write_to_db_result(kpi_fk, result, self.LEVEL2)
            if score is None and threshold is None:
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
            elif score is not None and threshold is None:
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score)
            else:
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, threshold=threshold)

    def write_to_db_result(self, fk, result, level, score=None, threshold=None, score_2=None, score_3=None,
                           result_2=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, result, level, score=score, threshold=threshold, score2=score_2,
                                                 score_3=score_3, result_2=result_2)
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

    def create_attributes_dict(self, fk, result, level, score2=None, score=None, threshold=None, score_3=None,
                               result_2=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score2, fk, score_3)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk', 'score_3'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, result, score2, score_3)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score',
                                               'score_2', 'score_3'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        result, kpi_fk, fk, threshold, score, score2, score_3, result_2)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'score', 'score_2', 'score_3', 'result_2'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = BATRUQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
            # print query
        self.rds_conn.db.commit()

# @staticmethod
# def most_common(lst):
#     product_counter = max(zip((lst.count(item) for item in set(lst)), set(lst)))
#     if product_counter[0] > 1:
#         current_value = product_counter[0]
#         current_product = product_counter[1]
#         new_lst = lst
#         new_lst.remove(current_product)
#         new_product_counter = max(zip((new_lst.count(item) for item in set(new_lst)), set(new_lst)))
#         new_lst.append(current_product)
#         if new_product_counter[0] == current_value and new_product_counter[1] != current_product:
#             return [new_product_counter[1], current_product]
#         else:
#             return product_counter[1]
#     else:
#         return None


# def _get_latest_session_for_cycle(self, previous_sessions_in_store):
#     """
#     Find latest session for the in the same store as current session
#     :param previous_sessions_in_store:
#     :type previous_sessions_in_store: pandas.DataFrame
#     :return:
#     :rtype pd.DataFrame
#     """
#     max_start_time_per_cycle = previous_sessions_in_store.groupby('plan_fk', as_index=False)['start_time'].max()
#     latest_session_for_cycle = previous_sessions_in_store.merge(max_start_time_per_cycle,
#                                                                 on=['plan_fk', 'start_time'])
#     latest_single_session_for_cycle = latest_session_for_cycle.groupby(['plan_fk', 'cycle_start_date'],
#                                                                        as_index=False)['session_id'].max()
#     return latest_single_session_for_cycle.sort_values(['cycle_start_date'], ascending=False).reset_index(drop=True)
