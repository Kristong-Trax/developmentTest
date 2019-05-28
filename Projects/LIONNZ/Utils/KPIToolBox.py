import os
import pandas as pd

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'nidhin'

TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'
ASSORTMENT_TEMPLATE_NAME = 'Assortments.xlsx'

KPI_NAMES_SHEET = 'kpis'
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
                'categories_to_exclude', 'scene_types_to_include', ]


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
        # self.common.commit_results_data()
        return 0  # to mark successful run of script

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
                self.calulcate_fsos(detail, groupers, query_string, dataframe_to_process)
            elif kpi_sheet_row[KPI_FAMILY_COL] == DISTRIBUTION:
                self.calculate_distribution(detail, groupers, query_string, dataframe_to_process)
            elif kpi_sheet_row[KPI_FAMILY_COL] == OOS:
                self.calculate_OOS(detail, groupers, query_string, dataframe_to_process)
            elif kpi_sheet_row[KPI_FAMILY_COL] == Count:
                self.calculate_count(detail, groupers, query_string, dataframe_to_process)
            else:
                pass
        return True

    def calulcate_fsos(self, kpi, groupers, query_string, dataframe_to_process):
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

    def calculate_distribution(self, kpi, groupers, query_string, dataframe_to_process):
        pass

    def calculate_OOS(self, kpi, groupers, query_string, dataframe_to_process):
        pass

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
