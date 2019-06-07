import os
import json
import pandas as pd
import numpy as np

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'nidhin'
OWN_MAN_NAME = 'Lion'  # case insensitive
TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'
ASSORTMENT_TEMPLATE_NAME = 'Assortments.xlsx'

KPI_NAMES_SHEET = 'kpis'
ASSORTMENT_SHEET = 'assortment'
KPI_DETAILS_SHEET = 'details'
KPI_INC_EXC_SHEET = 'exclude_include'
# Column Name
KPI_NAME_COL = 'kpi_name'
KPI_ACTIVE = 'active'
KPI_PARENT_COL = 'kpi_parent'
KPI_TYPE_COL = 'type'
KPI_FAMILY_COL = 'kpi_family'
STORE_POLICY = 'store_policy'
OUTPUT_TYPE = 'output'
# FAMILIES ALLOWED
FSOS = 'FSOS'
DISTRIBUTION = 'Distrbution'
OOS = 'OOS'
Count = 'Count'
# Output Type
NUM_OUT = 'number'
LIST_OUT = 'list'
PARAM_COUNT = 4
PARAM_DB_MAP = {
    # key: the value in excel as param values
    # value:
    #       key - to be checked in DataBase
    #       name - name as in DataBase
    'brand': {'key': 'brand_fk', 'name': 'brand_name'},
    'sku': {'key': 'product_fk', 'name': 'product_name'},
    'category': {'key': 'category_fk', 'name': 'category'},
    'scene_type': {'key': 'template_fk', 'name': 'template_name'},
    'sub_category': {'key': 'sub_category_fk', 'name': 'sub_category'},
    'manufacturer': {'key': 'manufacturer_fk', 'name': 'manufacturer_name'},
    'store': {'key': 'store_fk', 'name': 'store_name'},
}
# list of `exclude_include` sheet columns
INC_EXC_LIST = ['stacking', 'others', 'irrelevant', 'empty',
                'categories_to_exclude', 'scene_types_to_include', 'brands',
                'ean_codes']
# assortment KPIs
# Codes
OOS_CODE = 1
PRESENT_CODE = 2
EXTRA_CODE = 3
# KPI Names
DST_MAN_BY_STORE_PERC = 'DST_MAN_BY_STORE_PERC'
OOS_MAN_BY_STORE_PERC = 'OOS_MAN_BY_STORE_PERC'
PRODUCT_PRESENCE_BY_STORE_LIST = 'PRODUCT_PRESENCE_BY_STORE_LIST'
OOS_PRODUCT_BY_STORE_LIST = 'OOS_PRODUCT_BY_STORE_LIST'
# map to save list kpis
CODE_KPI_MAP = {
    OOS_CODE: OOS_PRODUCT_BY_STORE_LIST,
    PRESENT_CODE:PRODUCT_PRESENCE_BY_STORE_LIST,
}


class LIONNZToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info['store_fk'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scene_template_info = self.scif[['scene_fk', 'template_fk', 'template_name']].drop_duplicates()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                              '..', TEMPLATE_PARENT_FOLDER,
                                              TEMPLATE_NAME)
        self.assortment_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                     '..', TEMPLATE_PARENT_FOLDER,
                                                     ASSORTMENT_TEMPLATE_NAME)
        self.own_man_fk = self.all_products[
            self.all_products['manufacturer_name'].str.lower() == OWN_MAN_NAME.lower()
        ]['manufacturer_fk'].values[0]
        self.kpi_template = pd.ExcelFile(self.kpi_template_path)
        self.empty_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('empty', case=False)]['product_fk'].values
        self.irrelevant_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('irrelevant', case=False)]['product_fk'].values
        self.other_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('other', case=False)]['product_fk'].values

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.filter_and_send_kpi_to_calc()
        self.calculate_assortment_kpis()
        self.common.commit_results_data()
        return 0  # to mark successful run of script

    def calculate_assortment_kpis(self):
        distribution_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == DST_MAN_BY_STORE_PERC)
                                   & (self.kpi_static_data['delete_time'].isnull())]
        oos_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_MAN_BY_STORE_PERC)
                                   & (self.kpi_static_data['delete_time'].isnull())]
        prod_presence_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == PRODUCT_PRESENCE_BY_STORE_LIST)
                                           & (self.kpi_static_data['delete_time'].isnull())]
        oos_prod_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == OOS_PRODUCT_BY_STORE_LIST)
                                           & (self.kpi_static_data['delete_time'].isnull())]

        def __return_valid_store_policies(policy):
            policy_json = json.loads(policy)
            store_json = json.loads(self.store_info.reset_index().to_json(orient='records'))[0]
            valid_store = True
            for key, values in policy_json.iteritems():
                if str(store_json[key]) in values:
                    continue
                else:
                    valid_store = False
                    break
            return valid_store
        policy_data = self.get_policies(distribution_kpi.iloc[0].pk)
        resp = policy_data['policy'].apply(__return_valid_store_policies)
        valid_policy_data = policy_data[resp]
        # calculate and save the percentage values for distribution and oos
        self.calculate_and_save_distribution_and_oos(
            assortment_product_fks=valid_policy_data['product_fk'],
            distribution_kpi_fk=distribution_kpi.iloc[0].pk,
            oos_kpi_fk=oos_kpi.iloc[0].pk
        )
        # calculate and save prod presence and oos products
        self.calculate_and_save_prod_presence_and_oos_products(
            assortment_product_fks=valid_policy_data['product_fk'],
            prod_presence_kpi_fk=prod_presence_kpi.iloc[0].pk,
            oos_prod_kpi_fk=oos_prod_kpi.iloc[0].pk,
            distribution_kpi_name=DST_MAN_BY_STORE_PERC,
            oos_kpi_name=OOS_MAN_BY_STORE_PERC
        )

    def calculate_and_save_prod_presence_and_oos_products(self, assortment_product_fks,
                                                          prod_presence_kpi_fk, oos_prod_kpi_fk,
                                                          distribution_kpi_name, oos_kpi_name):
        total_products_in_scene = self.scif["item_id"].unique()
        present_products = np.intersect1d(total_products_in_scene, assortment_product_fks)
        extra_products = np.setdiff1d(total_products_in_scene, present_products)
        oos_products = np.setdiff1d(assortment_product_fks, present_products)
        product_map = {
            OOS_CODE: present_products,
            PRESENT_CODE: extra_products,
            EXTRA_CODE: oos_products
        }
        # save product presence; with distribution % kpi as parent
        for assortment_code, product_fks in product_map.iteritems():
            for each_fk in product_fks:
                self.common.write_to_db_result(fk=prod_presence_kpi_fk,
                                               numerator_id=each_fk,
                                               denominator_id=self.store_id,
                                               context_id=self.store_id,
                                               result=assortment_code,
                                               score=assortment_code,
                                               identifier_result=CODE_KPI_MAP.get(assortment_code),
                                               identifier_parent=distribution_kpi_name,
                                               should_enter=True
                                               )
            if assortment_code == OOS_CODE:
                # save OOS products; with OOS % kpi as parent
                for each_fk in product_fks:
                    self.common.write_to_db_result(fk=oos_prod_kpi_fk,
                                                   numerator_id=each_fk,
                                                   denominator_id=self.store_id,
                                                   context_id=self.store_id,
                                                   result=assortment_code,
                                                   score=assortment_code,
                                                   identifier_result=CODE_KPI_MAP.get(assortment_code),
                                                   identifier_parent=oos_kpi_name,
                                                   should_enter=True
                                                   )

    def calculate_and_save_distribution_and_oos(self, assortment_product_fks, distribution_kpi_fk, oos_kpi_fk):
        """Function to calculate distribution and OOS percentage.
        Saves distribution and oos percentage as values.
        """
        Log.info("Calculate distribution and OOS for {}".format(self.project_name))
        #  count of lion sku / all sku assortment count
        larger_dat, smaller_dat = self.scif["item_id"].unique(), assortment_product_fks
        if len(assortment_product_fks) > len(self.scif["item_id"].unique()):
            larger_dat = assortment_product_fks
            smaller_dat = self.scif["item_id"].unique()
        count_of_prod_in_scene = len([x for x in smaller_dat if x in larger_dat])
        distribution_perc = count_of_prod_in_scene / float(len(assortment_product_fks)) * 100
        oos_perc = 100 - distribution_perc
        self.common.write_to_db_result(fk=distribution_kpi_fk,
                                       numerator_id=self.own_man_fk,
                                       denominator_id=self.store_id,
                                       context_id=self.store_id,
                                       result=distribution_perc,
                                       score=distribution_perc,
                                       )
        self.common.write_to_db_result(fk=oos_kpi_fk,
                                       numerator_id=self.own_man_fk,
                                       denominator_id=self.store_id,
                                       context_id=self.store_id,
                                       result=oos_perc,
                                       score=distribution_perc,
                                       )

    def get_policies(self, kpi_fk):
        query = """ select a.kpi_fk, p.policy_name, p.policy, atag.assortment_group_fk, atp.assortment_fk, atp.product_fk, 
                        atp.start_date, atp.end_date from pservice.assortment_to_product atp 
                        join pservice.assortment_to_assortment_group atag on atp.assortment_fk = atag.assortment_fk 
                        join pservice.assortment a on a.pk = atag.assortment_group_fk
                        join pservice.policy p on p.pk = a.store_policy_group_fk
                    where a.kpi_fk={kpi_fk};
                    """
        policies = pd.read_sql_query(query.format(kpi_fk=kpi_fk), self.rds_conn.db)
        return policies

    def filter_and_send_kpi_to_calc(self):
        kpi_sheet = self.kpi_template.parse(KPI_NAMES_SHEET)
        kpi_sheet[KPI_FAMILY_COL] = kpi_sheet[KPI_FAMILY_COL].fillna(method='ffill')
        kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
        kpi_include_exclude = self.kpi_template.parse(KPI_INC_EXC_SHEET)
        for index, kpi_sheet_row in kpi_sheet.iterrows():
            if not is_nan(kpi_sheet_row[KPI_ACTIVE]):
                if str(kpi_sheet_row[KPI_ACTIVE]).strip().lower() in ['0.0', 'n', 'no']:
                    print("KPI :{} deactivated in sheet.".format(kpi_sheet_row[KPI_NAME_COL]))
                    continue
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi_sheet_row[KPI_NAME_COL])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME_COL]))
                return False
            else:
                print("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME_COL]))
                detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi[KPI_TYPE_COL].values[0]]
                # check for store types allowed
                permitted_store_types = [x.strip() for x in detail[STORE_POLICY].values[0].split(',') if x.strip()]
                if self.store_info.store_type.iloc[0] not in permitted_store_types:
                    print("Not permitted store type - {}".format(kpi_sheet_row[KPI_NAME_COL]))
                    continue
                pd.reset_option('mode.chained_assignment')
                with pd.option_context('mode.chained_assignment', None):
                    detail['pk'] = kpi['pk'].iloc[0]
                # gather details
                groupers, query_string = get_groupers_and_query_string(detail)
                _include_exclude = kpi_include_exclude[kpi_details[KPI_NAME_COL] == kpi[KPI_TYPE_COL].values[0]]
                # gather include exclude
                include_exclude_data_dict = get_include_exclude(_include_exclude)
                dataframe_to_process = self.get_sanitized_match_prod_scene(include_exclude_data_dict)
            if kpi_sheet_row[KPI_FAMILY_COL] == FSOS:
                self.calculate_fsos(detail, groupers, query_string, dataframe_to_process)
            elif kpi_sheet_row[KPI_FAMILY_COL] == Count:
                self.calculate_count(detail, groupers, query_string, dataframe_to_process)
            else:
                pass
        return True

    def calculate_fsos(self, kpi, groupers, query_string, dataframe_to_process):
        if query_string:
            grouped_data_frame = dataframe_to_process.query(query_string).groupby(groupers)
        else:
            grouped_data_frame = dataframe_to_process.groupby(groupers)
        for group_id_tup, group_data in grouped_data_frame:
            param_id_map = dict(zip(groupers, group_id_tup))
            numerator_id = param_id_map.get(PARAM_DB_MAP[kpi['numerator'].iloc[0]]['key'])
            denominator_id = param_id_map.get(PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'])
            context_id = (get_context_id(context_string=kpi['context'].iloc[0], param_id_map=param_id_map)
                          or self.store_id)
            result = len(group_data) / float(len(dataframe_to_process))
            should_enter = False
            identifier_parent = None
            identifier_result = None
            if not is_nan(kpi['kpi_parent'].iloc[0]):
                should_enter = True
                identifier_result = kpi[KPI_NAME_COL]
                identifier_parent = kpi[KPI_PARENT_COL]
            self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                           numerator_id=numerator_id,
                                           denominator_id=denominator_id,
                                           context_id=context_id,
                                           result=result,
                                           identifier_result=identifier_result,
                                           identifier_parent=identifier_parent,
                                           should_enter=should_enter,
                                           )

        return True

    def calculate_count(self, kpi, groupers, query_string, dataframe_to_process):
        if query_string:
            grouped_data_frame = dataframe_to_process.query(query_string).groupby(groupers)
        else:
            grouped_data_frame = dataframe_to_process.groupby(groupers)
        for group_id_tup, group_data in grouped_data_frame:
            param_id_map = dict(zip(groupers, group_id_tup))
            numerator_id = param_id_map.get(PARAM_DB_MAP[kpi['numerator'].iloc[0]]['key'])
            denominator_id = param_id_map.get(PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'])
            context_id = (get_context_id(context_string=kpi['context'].iloc[0], param_id_map=param_id_map)
                          or self.store_id)
            result = len(group_data)
            self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                           numerator_id=numerator_id,
                                           denominator_id=denominator_id,
                                           context_id=context_id,
                                           result=result,
                                           )

        return True

    def get_sanitized_match_prod_scene(self, include_exclude_data_dict):
        scene_product_data = self.match_product_in_scene.merge(
            self.products, how='left', on=['product_fk'],  suffixes=('', '_prod')
        )
        sanitized_products_in_scene = scene_product_data.merge(
            self.scene_template_info, how='left', on='scene_fk', suffixes=('', '_scene')
        )
        # flags
        should_include_empty = include_exclude_data_dict.get('empty')
        should_include_irrelevant = include_exclude_data_dict.get('irrelevant')
        should_include_others = include_exclude_data_dict.get('others')
        should_include_stacking = include_exclude_data_dict.get('stacking')
        # list
        scene_types_to_include = include_exclude_data_dict.get('scene_types_to_include', False)
        categories_to_exclude = include_exclude_data_dict.get('categories_to_exclude', False)
        query = ''
        product_ids_to_exclude = []
        if not should_include_irrelevant:
            # add product ids to exclude with irrelevant
            product_ids_to_exclude.extend(self.irrelevant_prod_ids)
        if not should_include_others:
            # add product ids to exclude with others
            product_ids_to_exclude.extend(self.other_prod_ids)
        if not should_include_empty:
            # add product ids to exclude with empty
            product_ids_to_exclude.extend(self.empty_prod_ids)
        if product_ids_to_exclude:
            query += ' and product_fk not in {}'.format(product_ids_to_exclude)
        if not should_include_stacking:
            # exclude stacking
            query += ' and stacking_layer==1'
        if scene_types_to_include:
            # list of scene types to include is present, otherwise all included
            query += ' and template_name in {}'.format(scene_types_to_include)
        if categories_to_exclude:
            # list of categories  to include is present, otherwise all included
            query += ' and category not in {}'.format(categories_to_exclude)
        sanitized_products_in_scene = sanitized_products_in_scene.query(query.strip(' and '))
        return sanitized_products_in_scene


def get_context_id(context_string, param_id_map):
    """Function to return context ID.

    Return the context ID from the dict provided based on the numerator/denominator in excel. Or return `None`.
    Use appropriate ID in caller; if context ID cannot be found. Usually store_id.
    """
    if context_string.strip() and context_string.strip() != 'store':
        return param_id_map.get(context_string, None)
    return None


def get_include_exclude(kpi_include_exclude):
    output = {}
    for key in INC_EXC_LIST:
        if not is_nan(kpi_include_exclude.get(key).values[0]) and \
                str(kpi_include_exclude.get(key).values[0]).strip() and \
                str(kpi_include_exclude.get(key).values[0]).strip().lower() != 'all':
            _data = kpi_include_exclude.get(key).values[0]
            if type(_data) == unicode:
                _data = [x.strip() for x in _data.split(',') if x.strip()]
            output[key] = _data
    return output


def get_groupers_and_query_string(detail):
    keys = ['param_{}'.format(x) for x in range(1, PARAM_COUNT + 1)]
    filters = []
    filter_string = ''
    for each_key in keys:
        if not is_nan(detail[each_key].iloc[0]):
            filters.append(PARAM_DB_MAP[detail[each_key].iloc[0]]['key'])
            if not is_nan(detail[each_key + "_value"].iloc[0]):
                filter_data = [str(x.strip()) for x in detail[each_key + "_value"].iloc[0].split(',') if x.strip()]
                filter_string += ' {key} in {data_list} and'.format(
                    key=PARAM_DB_MAP[detail[each_key].iloc[0]]['name'],
                    data_list=filter_data
                )
    return filters, filter_string.strip('and')


def is_nan(value):
    if value != value:
        return True
    return False
