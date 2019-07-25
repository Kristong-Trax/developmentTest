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
                'categories_to_exclude', 'scene_types_to_exclude',
                'brands_to_exclude', 'ean_codes_to_exclude']
# assortment KPIs
# Codes
OOS_CODE = 1
PRESENT_CODE = 2
EXTRA_CODE = 3
# KPI Names
DST_MAN_BY_STORE_PERC = 'DST_MAN_BY_STORE_PERC'
OOS_MAN_BY_STORE_PERC = 'OOS_MAN_BY_STORE_PERC'
PRODUCT_PRESENCE_BY_STORE_LIST = 'DST_MAN_BY_STORE_PERC - SKU'
OOS_PRODUCT_BY_STORE_LIST = 'OOS_MAN_BY_STORE_PERC - SKU'
# map to save list kpis
CODE_KPI_MAP = {
    OOS_CODE: OOS_PRODUCT_BY_STORE_LIST,
    PRESENT_CODE: PRODUCT_PRESENCE_BY_STORE_LIST,
}
# policy JSON map: key is what is in the policy and value corresponds to the one present in the self.store_info below
POLICY_STORE_MAP = {
    'retailer': 'retailer_name',
    'region': 'region_name',
}


class LIONNZToolBox:
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
        self.scene_template_info = self.scif[['scene_fk',
                                              'template_fk', 'template_name']].drop_duplicates()
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
        pd.reset_option('mode.chained_assignment')
        with pd.option_context('mode.chained_assignment', None):
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
            # map the necessary keys to those names knows
            for policy_value, store_info_value in POLICY_STORE_MAP.iteritems():
                if policy_value in policy_json:
                    policy_json[store_info_value] = policy_json.pop(policy_value)
            for key, values in policy_json.iteritems():
                if str(store_json[key]) in values:
                    continue
                else:
                    valid_store = False
                    break
            return valid_store

        policy_data = self.get_policies(distribution_kpi.iloc[0].pk)
        if policy_data.empty:
            Log.info("No Assortments Loaded.")
            return 0
        resp = policy_data['policy'].apply(__return_valid_store_policies)
        valid_policy_data = policy_data[resp]
        if valid_policy_data.empty:
            Log.info("No policy applicable for session {sess} and kpi {kpi}.".format(
                sess=self.session_uid,
                kpi=distribution_kpi.iloc[0].type))
            return 0
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
        # all assortment products are only in own manufacturers context
        total_own_products_in_scene = self.scif[
            self.scif['manufacturer_fk'].astype(int) == self.own_man_fk
        ]["item_id"].unique()
        present_products = np.intersect1d(total_own_products_in_scene, assortment_product_fks)
        extra_products = np.setdiff1d(total_own_products_in_scene, present_products)
        oos_products = np.setdiff1d(assortment_product_fks, present_products)
        product_map = {
            OOS_CODE: oos_products,
            PRESENT_CODE: present_products,
            EXTRA_CODE: extra_products
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
                                               identifier_parent="{}_{}".format(distribution_kpi_name, self.store_id),
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
                                                   identifier_parent="{}_{}".format(oos_kpi_name, self.store_id),
                                                   should_enter=True
                                                   )

    def calculate_and_save_distribution_and_oos(self, assortment_product_fks, distribution_kpi_fk, oos_kpi_fk):
        """Function to calculate distribution and OOS percentage.
        Saves distribution and oos percentage as values.
        """
        Log.info("Calculate distribution and OOS for {}".format(self.project_name))
        scene_products = pd.Series(self.scif["item_id"].unique())
        total_products_in_assortment = len(assortment_product_fks)
        count_of_assortment_prod_in_scene = assortment_product_fks.isin(scene_products).sum()
        oos_count = total_products_in_assortment - count_of_assortment_prod_in_scene
        #  count of lion sku / all sku assortment count
        if not total_products_in_assortment:
            Log.info("No assortments applicable for session {sess}.".format(sess=self.session_uid))
            return 0
        distribution_perc = count_of_assortment_prod_in_scene / float(total_products_in_assortment) * 100
        oos_perc = 100 - distribution_perc
        self.common.write_to_db_result(fk=distribution_kpi_fk,
                                       numerator_id=self.own_man_fk,
                                       numerator_result=count_of_assortment_prod_in_scene,
                                       denominator_id=self.store_id,
                                       denominator_result=total_products_in_assortment,
                                       context_id=self.store_id,
                                       result=distribution_perc,
                                       score=distribution_perc,
                                       identifier_result="{}_{}".format(DST_MAN_BY_STORE_PERC, self.store_id),
                                       should_enter=True
                                       )
        self.common.write_to_db_result(fk=oos_kpi_fk,
                                       numerator_id=self.own_man_fk,
                                       numerator_result=oos_count,
                                       denominator_id=self.store_id,
                                       denominator_result=total_products_in_assortment,
                                       context_id=self.store_id,
                                       result=oos_perc,
                                       score=oos_perc,
                                       identifier_result="{}_{}".format(OOS_MAN_BY_STORE_PERC, self.store_id),
                                       should_enter=True
                                       )

    def get_policies(self, kpi_fk):
        query = """ select a.kpi_fk, p.policy_name, p.policy, atag.assortment_group_fk,
                        atp.assortment_fk, atp.product_fk, atp.start_date, atp.end_date
                    from pservice.assortment_to_product atp 
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
                    Log.warning("KPI :{} deactivated in sheet.".format(kpi_sheet_row[KPI_NAME_COL]))
                    continue
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi_sheet_row[KPI_NAME_COL])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                Log.warning("*** KPI Name:{name} not found in DB for session {sess} ***".format(
                    name=kpi_sheet_row[KPI_NAME_COL],
                    sess=self.session_uid
                ))
                return False
            else:
                Log.info("KPI Name:{name} found in DB for session {sess}".format(
                    name=kpi_sheet_row[KPI_NAME_COL],
                    sess=self.session_uid
                ))
                detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi[KPI_TYPE_COL].values[0]]
                # check for store types allowed
                permitted_store_types = [x.strip().lower()
                                         for x in detail[STORE_POLICY].values[0].split(',') if x.strip()]
                if self.store_info.store_type.iloc[0].lower() not in permitted_store_types:
                    Log.warning("Not permitted store type - {type} for session {sess}".format(
                        type=kpi_sheet_row[KPI_NAME_COL],
                        sess=self.session_uid
                    ))
                    continue
                detail['pk'] = kpi['pk'].iloc[0]
                # gather details
                groupers, query_string = get_groupers_and_query_string(detail)
                _include_exclude = kpi_include_exclude[kpi_details[KPI_NAME_COL]
                                                       == kpi[KPI_TYPE_COL].values[0]]
                # gather include exclude
                include_exclude_data_dict = get_include_exclude(_include_exclude)
                dataframe_to_process = self.get_sanitized_match_prod_scene(
                    include_exclude_data_dict)
            if kpi_sheet_row[KPI_FAMILY_COL] == FSOS:
                self.calculate_fsos(detail, groupers, query_string, dataframe_to_process)
            elif kpi_sheet_row[KPI_FAMILY_COL] == Count:
                self.calculate_count(detail, groupers, query_string, dataframe_to_process)
            else:
                pass
        return True

    def calculate_fsos(self, kpi, groupers, query_string, dataframe_to_process):
        Log.info("Calculate {name} for session {sess}".format(
            name=kpi.kpi_name.iloc[0],
            sess=self.session_uid
        ))
        if query_string:
            grouped_data_frame = dataframe_to_process.query(query_string).groupby(groupers)
        else:
            grouped_data_frame = dataframe_to_process.groupby(groupers)
        # for the two kpis, we need to show zero presence of own manufacturer.
        # else the flow will be stuck in case own manufacturers are absent altogether.
        if '_own_' in kpi['kpi_name'].iloc[0].lower() and \
                '_whole_store' not in kpi['kpi_name'].iloc[0].lower():
            self.scif['store_fk'] = self.store_id
            dataframe_to_process['store_fk'] = self.store_id
            scif_with_den_context = self.scif[[PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                                               PARAM_DB_MAP[kpi['context'].iloc[0]]['key']]].drop_duplicates()
            df_with_den_context = dataframe_to_process.query(query_string)[[
                PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                PARAM_DB_MAP[kpi['context'].iloc[0]]['key']
            ]].drop_duplicates()
            denominators_df_to_save_zero = scif_with_den_context[(~scif_with_den_context[
                PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key']
            ].isin(df_with_den_context[PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key']]))]

            kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
            identifier_parent = None
            if not is_nan(kpi[KPI_PARENT_COL].iloc[0]):
                kpi_parent = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi[KPI_PARENT_COL].iloc[0])
                                                  & (self.kpi_static_data['delete_time'].isnull())]
                kpi_parent_detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi_parent[KPI_TYPE_COL].values[0]]
                # hard coding; expecting only `FSOS_OWN_MANUFACTURER_IN_WHOLE_STORE` as parent
                parent_denominator_id = self.store_id
                parent_context_id = self.store_id
                identifier_parent = "{}_{}_{}_{}".format(
                    kpi_parent_detail['kpi_name'].iloc[0],
                    kpi_parent['pk'].iloc[0],
                    # parent_numerator_id,
                    parent_denominator_id,
                    parent_context_id
                )

            numerator_fk = self.own_man_fk
            result = numerator_result = 0  # SAVE ALL RESULTS AS ZERO
            for idx, each_row in denominators_df_to_save_zero.iterrows():
                context_id = each_row[PARAM_DB_MAP[kpi['context'].iloc[0]]['key']]
                # query out empty product IDs since FSOS is not interested in them.
                each_den_fk = each_row[PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key']]
                _query = "{key}=='{value_id}' and product_fk not in {exc_prod_ids}".format(
                    key=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                    value_id=each_den_fk,
                    exc_prod_ids=self.empty_prod_ids.tolist() + self.irrelevant_prod_ids.tolist()
                )
                # find number of products in that context
                denominator_result = len(dataframe_to_process.query(_query))
                if not denominator_result:
                    continue
                self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                               numerator_id=numerator_fk,
                                               denominator_id=each_den_fk,
                                               context_id=context_id,
                                               result=result,
                                               numerator_result=numerator_result,
                                               denominator_result=denominator_result,
                                               identifier_result="{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   each_den_fk,
                                                   context_id
                                               ),
                                               identifier_parent=identifier_parent,
                                               should_enter=True,
                                               )

        for group_id_tup, group_data in grouped_data_frame:
            if type(group_id_tup) not in [tuple, list]:
                # convert to a tuple
                group_id_tup = group_id_tup,
            param_id_map = dict(zip(groupers, group_id_tup))
            numerator_id = param_id_map.get(PARAM_DB_MAP[kpi['numerator'].iloc[0]]['key'])
            denominator_id = (get_parameter_id(key_value=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                                               param_id_map=param_id_map) or self.store_id)
            context_id = (get_parameter_id(key_value=PARAM_DB_MAP[kpi['context'].iloc[0]]['key'],
                                           param_id_map=param_id_map) or self.store_id)
            if PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'] == 'store_fk':
                denominator_df = dataframe_to_process
            else:
                denominator_df = dataframe_to_process.query('{key} == {value}'.format(
                    key=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                    value=denominator_id))
            if not len(denominator_df):
                Log.info("No denominator data for session {sess} to calculate  {name}".format(
                    sess=self.session_uid,
                    name=kpi.kpi_name.iloc[0]
                ))
                continue
            result = len(group_data) / float(len(denominator_df))
            if not is_nan(kpi[KPI_PARENT_COL].iloc[0]):
                kpi_parent = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi[KPI_PARENT_COL].iloc[0])
                                                  & (self.kpi_static_data['delete_time'].isnull())]
                kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
                kpi_parent_detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi_parent[KPI_TYPE_COL].values[0]]
                parent_denominator_id = (get_parameter_id(key_value=PARAM_DB_MAP[kpi_parent_detail['denominator'].iloc[0]]['key'],
                                                          param_id_map=param_id_map) or self.store_id)
                parent_context_id = (get_parameter_id(key_value=PARAM_DB_MAP[kpi_parent_detail['context'].iloc[0]]['key'],
                                                      param_id_map=param_id_map) or self.store_id)
                self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                               numerator_id=numerator_id,
                                               denominator_id=denominator_id,
                                               context_id=context_id,
                                               result=result,
                                               numerator_result=len(group_data),
                                               denominator_result=len(denominator_df),
                                               identifier_result="{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   denominator_id,
                                                   context_id
                                               ),
                                               identifier_parent="{}_{}_{}_{}".format(
                                                   kpi_parent_detail['kpi_name'].iloc[0],
                                                   kpi_parent['pk'].iloc[0],
                                                   # parent_numerator_id,
                                                   parent_denominator_id,
                                                   parent_context_id
                                               ),
                                               should_enter=True,
                                               )
            else:
                # its the parent. Save the identifier result.
                self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                               numerator_id=numerator_id,
                                               denominator_id=denominator_id,
                                               context_id=context_id,
                                               result=result,
                                               numerator_result=len(group_data),
                                               denominator_result=len(denominator_df),
                                               identifier_result="{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   denominator_id,
                                                   context_id
                                               ),
                                               should_enter=True,
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
            context_id = (get_parameter_id(key_value=PARAM_DB_MAP[kpi['context'].iloc[0]]['key'],
                                           param_id_map=param_id_map)
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
        include_empty = include_exclude_data_dict.get('empty')
        include_irrelevant = include_exclude_data_dict.get('irrelevant')
        include_others = include_exclude_data_dict.get('others')
        include_stacking = include_exclude_data_dict.get('stacking')
        # list
        scene_types_to_exclude = include_exclude_data_dict.get('scene_types_to_exclude', False)
        categories_to_exclude = include_exclude_data_dict.get('categories_to_exclude', False)
        brands_to_exclude = include_exclude_data_dict.get('brands_to_exclude', False)
        ean_codes_to_exclude = include_exclude_data_dict.get('ean_codes_to_exclude', False)
        # Start removing items
        if scene_types_to_exclude:
            # list of scene types to include is present, otherwise all included
            Log.info("Exclude template/scene type {}".format(scene_types_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[sanitized_products_in_scene['template_name'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in scene_types_to_exclude]
                )].index,
                inplace=True
            )
        if not include_stacking:
            # exclude stacking if the flag is set
            Log.info("Exclude stacking other than in layer 1 or negative stacking [menu]")
            sanitized_products_in_scene = sanitized_products_in_scene.loc[
                sanitized_products_in_scene['stacking_layer'] <= 1]
        if categories_to_exclude:
            # list of categories to exclude is present, otherwise all included
            Log.info("Exclude categories {}".format(categories_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[sanitized_products_in_scene['category'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in categories_to_exclude]
                )].index,
                inplace=True
            )
        if brands_to_exclude:
            # list of brands to exclude is present, otherwise all included
            Log.info("Exclude brands {}".format(brands_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[sanitized_products_in_scene['brand_name'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in brands_to_exclude]
                )].index,
                inplace=True
            )
        if ean_codes_to_exclude:
            # list of ean_codes to exclude is present, otherwise all included
            Log.info("Exclude ean codes {}".format(ean_codes_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[sanitized_products_in_scene['product_ean_code'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in ean_codes_to_exclude]
                )].index,
                inplace=True
            )
        product_ids_to_exclude = []
        if not include_irrelevant:
            # add product ids to exclude with irrelevant
            product_ids_to_exclude.extend(self.irrelevant_prod_ids)
        if not include_others:
            # add product ids to exclude with others
            product_ids_to_exclude.extend(self.other_prod_ids)
        if not include_empty:
            # add product ids to exclude with empty
            product_ids_to_exclude.extend(self.empty_prod_ids)
        if product_ids_to_exclude:
            Log.info("Exclude product ids {}".format(product_ids_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[
                    sanitized_products_in_scene['product_fk'].isin(product_ids_to_exclude)].index,
                inplace=True
            )
        return sanitized_products_in_scene


def get_parameter_id(key_value, param_id_map):
    """Function to return parameter ID.

    Return the context ID from the dict provided based on the numerator/denominator in excel. Or return `None`.
    Use appropriate default ID in caller; if context ID cannot be found. Usually store_id.
    """
    if key_value.strip() and key_value.strip() != 'store_fk':
        return param_id_map.get(key_value, None)
    return None


def get_include_exclude(kpi_include_exclude):
    output = {}
    for key in INC_EXC_LIST:
        if not is_nan(kpi_include_exclude.get(key).values[0]) and \
                str(kpi_include_exclude.get(key).values[0]).strip() != '' and \
                str(kpi_include_exclude.get(key).values[0]).strip().lower() not in ['na', 'n/a', 'n / a']:
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
                filter_data = [str(x.strip()) for x in detail[each_key +
                                                              "_value"].iloc[0].split(',') if x.strip()]
                filter_string += ' {key} in {data_list} and'.format(
                    key=PARAM_DB_MAP[detail[each_key].iloc[0]]['name'],
                    data_list=filter_data
                )
    return filters, filter_string.strip('and')


def is_nan(value):
    if value != value:
        return True
    return False
