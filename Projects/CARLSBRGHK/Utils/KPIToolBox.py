import os
import json
import pandas as pd
import numpy as np

from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


__author__ = 'nidhin'

TEMPLATE_PARENT_FOLDER = 'Data'
TEMPLATE_NAME = 'Template.xlsx'

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
PARAM_COUNT = 5
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
# # Assortment
MSL_AMBIENT_SHELF_PERC = 'MSL_AMBIENT_SHELF_PERC'
MSL_COOLER_PERC = 'MSL_COOLER_PERC'
MSL_FLOOR_STACK_PERC = 'MSL_FLOOR_STACK_PERC'

MSL_AMBIENT_SHELF_LIST = 'MSL_AMBIENT_SHELF_PERC - SKU'
MSL_COOLER_LIST = 'MSL_COOLER_PERC - SKU'
MSL_FLOOR_STACK_LIST = 'MSL_FLOOR_STACK_PERC - SKU'
#  ## Category based Assortment KPIs
MSL_AMBIENT_SHELF_BY_CATEGORY_PERC = 'MSL_AMBIENT_SHELF_ALL_CATEGORY_PERC'
MSL_COOLER_BY_CATEGORY_PERC = 'MSL_COOLER_ALL_CATEGORY_PERC'
MSL_FLOOR_STACK_BY_CATEGORY_PERC = 'MSL_FLOOR_STACK_ALL_CATEGORY_PERC'

MSL_AMBIENT_SHELF_BY_CATEGORY_LIST = 'MSL_AMBIENT_SHELF_ALL_CATEGORY_PERC - SKU'
MSL_COOLER_BY_CATEGORY_LIST = 'MSL_COOLER_ALL_CATEGORY_PERC - SKU'
MSL_FLOOR_BY_CATEGORY_LIST = 'MSL_FLOOR_STACK_ALL_CATEGORY_PERC - SKU'
ASSORTMENT_DATA = [
    (MSL_AMBIENT_SHELF_PERC, MSL_AMBIENT_SHELF_BY_CATEGORY_PERC),
    (MSL_COOLER_PERC, MSL_COOLER_BY_CATEGORY_PERC),
    (MSL_FLOOR_STACK_PERC, MSL_FLOOR_STACK_BY_CATEGORY_PERC),
    ]
# # Assortment ends

COUNT_SKU_IN_STOCK = 'COUNT_SKU_IN_STOCK'
COUNT_SKU_FACINGS_FLOOR_STACK = 'COUNT_SKU_FACINGS_FLOOR_STACK'
FSOS_AT_SKU = 'FSOS_ALL'
# map to save list kpis
# CODE_KPI_MAP = {
#     OOS_CODE: OOS_PRODUCT_BY_STORE_LIST,
#     PRESENT_CODE: PRODUCT_PRESENCE_BY_STORE_LIST,
# }
# policy JSON map: key is what is in the policy and value corresponds to the one present in the self.store_info below
POLICY_STORE_MAP = {
    'retailer': 'retailer_name',
    'region': 'region_name',
}


class CARLSBERGToolBox:
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
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()
        self.scene_template_info = self.scif[['scene_fk',
                                              'template_fk', 'template_name']].drop_duplicates()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.kpi_template_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                              '..', TEMPLATE_PARENT_FOLDER,
                                              TEMPLATE_NAME)
        own_man_fk_data = self.data_provider.own_manufacturer.param_value.values[0]
        if not own_man_fk_data:
            # OWN manufacturer is not set
            Log.error("Own Manufacturer is not set for project: {}".format(self.project_name))
        self.own_man_fk = self.own_manufacturer_fk = int(own_man_fk_data)
        self.kpi_template = pd.ExcelFile(self.kpi_template_path)
        self.empty_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('empty', case=False)]['product_fk'].values
        self.irrelevant_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('irrelevant', case=False)]['product_fk'].values
        self.other_prod_ids = self.all_products[
            self.all_products.product_name.str.contains('other', case=False)]['product_fk'].values

    def main_calculation(self, only_stock_calc):
        """
        This function calculates the KPI results.
        """
        pd.reset_option('mode.chained_assignment')
        with pd.option_context('mode.chained_assignment', None):
            if only_stock_calc:
                self.calculate_sku_count_in_stock()
            else:
                self.calculate_fsos_kpis()
                self.calculate_assortment_kpis()
                self.calculate_sku_facings_in_floor_stack()
                self.calculate_sku_count_in_stock()
                self.calculate_fsos_sku()
        self.common.commit_results_data()
        return 0  # to mark successful run of script

    def calculate_sku_facings_in_floor_stack(self):
        Log.info("Calculate COUNT_SKU_FACINGS_IN_FLOOR_STACK for {} - {}".format(self.project_name, self.session_uid))
        external_target_data = self.external_targets[
            self.external_targets['kpi_type'] == COUNT_SKU_FACINGS_FLOOR_STACK]
        if external_target_data.empty:
            Log.warning("Ext Target for COUNT_SKU_FACINGS_IN_FLOOR_STACK not found")
            return True
        include_exclude_data_dict = external_target_data.loc[:, ['include_brand_fks', 'include_category_fks',
                                                                 'template_fks', 'empty_exclude', 'irrelevant_exclude',
                                                                 'others_exclude', 'stacking_exclude'
                                                                 ]].dropna(axis='columns').to_dict('records')[0]
        for k, v in include_exclude_data_dict.iteritems():
            if type(v) == float:
                include_exclude_data_dict[k] = int(v)
        dataframe_to_process = self.get_sanitized_match_prod_scene(include_exclude_data_dict)
        if dataframe_to_process.empty:
            Log.info(
                "No data to calculate COUNT_SKU_FACINGS_IN_FLOOR_STACK for {} - {}".format(
                    self.project_name, self.session_uid))
            return True
        floor_stack_template_fk = int(dataframe_to_process['template_fk'].unique()[0])
        for each_prod_fk, prod_group in dataframe_to_process.groupby('product_fk'):
            # get manufacturer
            product_df = self.all_products[self.all_products['product_fk']==each_prod_fk]
            if product_df.empty:
                Log.warning("Product with pk:{pk} not found during session:{sess}".format(
                    pk=each_prod_fk,
                    sess=self.session_uid
                ))
                continue
            self.common.write_to_db_result(fk=external_target_data.iloc[0].kpi_fk,
                                           numerator_id=each_prod_fk,
                                           denominator_id=product_df.iloc[0].manufacturer_fk,
                                           context_id=floor_stack_template_fk,
                                           result=len(prod_group),
                                           score=len(prod_group),
                                           )
        return True

    def get_manual_collection_stock_data(self):
        """
            Method to get the manual stock collection data for the current session.
        """
        query = """
            SELECT 
                *
            FROM
                probedata.manual_collection_number collection
                    JOIN
                static.manual_collection_number_attributes attr ON attr.pk = 
                collection.manual_collection_number_attributes_fk
            WHERE
                collection.session_fk = {session_fk}
                AND collection.deleted_time IS NULL
                AND attr.delete_date IS NULL;
        """
        stock_manual_collection_data = pd.read_sql_query(query.format(
            session_fk=self.session_info.iloc[0].pk), self.rds_conn.db)
        return stock_manual_collection_data

    def calculate_sku_count_in_stock(self):
        # write for COUNT_SKU_IN_STOCK
        kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == COUNT_SKU_IN_STOCK)
                                   & (self.kpi_static_data['delete_time'].isnull())]
        if kpi.empty:
            Log.warning("*** KPI Name:{name} not found in DB for session {sess} ***".format(
                name=COUNT_SKU_IN_STOCK,
                sess=self.session_uid
            ))
            return False
        else:
            Log.info("Calculate COUNT_SKU_IN_STOCK for {} - {}".format(self.project_name, self.session_uid))
        stock_manual_collection_df = self.get_manual_collection_stock_data()
        collection_with_prod_df = stock_manual_collection_df.merge(
            self.all_products, how='left', on=['product_fk'], suffixes=('', '_prod')
        )
        for index, row in collection_with_prod_df.iterrows():
            Log.info("Saving stock value for {product} as {value} in session {sess}".format(
                product=row.product_name,
                value=row.value,
                sess=self.session_uid
            ))
            self.common.write_to_db_result(fk=kpi.iloc[0].pk,
                                           numerator_id=row.product_fk,
                                           denominator_id=row.manufacturer_fk,
                                           context_id=self.store_id,
                                           result=row.value,
                                           score=row.value,
                                           )
        return True

    def calculate_assortment_kpis(self):
        for each_assortment in ASSORTMENT_DATA:
            # each_assortment:
            # 0 - store level
            # 1 - category level
            msl_store_level, msl_cat_level = each_assortment
            Log.info("Starting {sess}, calulcation for store level: {s_kpi} and category level: {c_kpi}.".format(
                sess=self.session_uid,
                s_kpi=msl_store_level,
                c_kpi=msl_cat_level,
            ))
            external_target_data = self.external_targets[self.external_targets['kpi_type']==msl_store_level]
            if external_target_data.empty:
                Log.info("{} has no external target data to calculate for session {}.".format(msl_store_level,
                                                                                              self.session_uid))
                continue
            external_target_dict = external_target_data.iloc[0].to_dict()
            valid_category_fks = external_target_dict.get('include_category_fks', False)
            if valid_category_fks and type(valid_category_fks) != list:
                valid_category_fks = [valid_category_fks]
            valid_scene_fks = external_target_dict.get('template_fks', False)
            if valid_scene_fks and type(valid_scene_fks) != list:
                valid_scene_fks = [valid_scene_fks]
            valid_brand_fks = external_target_dict.get('include_brand_fks', False)
            if valid_brand_fks and type(valid_brand_fks) != list:
                valid_brand_fks = [valid_brand_fks]
            valid_scif = self.scif  # [self.scif['manufacturer_fk']==self.own_man_fk]
            if valid_scene_fks:
                valid_scif = valid_scif[(valid_scif['template_fk'].isin(valid_scene_fks))]
            if valid_category_fks and not is_nan(valid_category_fks):
                valid_scif = valid_scif[(valid_scif['category_fk'].isin(valid_category_fks))]
            if valid_brand_fks and not is_nan(valid_brand_fks):
                valid_scif = valid_scif[(valid_scif['brand_fk'].isin(valid_brand_fks))]
            if valid_scif.empty:
                Log.info("Session {sess} has no data to calculate {st}/{cat}".format(
                    sess=self.session_uid,
                    st=msl_store_level,
                    cat=msl_cat_level
                ))
                continue
            distribution_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == msl_store_level)
                                                    & (self.kpi_static_data['delete_time'].isnull())]
            prod_presence_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == msl_store_level + ' - SKU')
                                                     & (self.kpi_static_data['delete_time'].isnull())]
            # Category based Assortments
            distribution_by_cat_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == msl_cat_level)
                                                           & (self.kpi_static_data['delete_time'].isnull())]
            prod_presence_by_cat_kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] ==
                                                             msl_cat_level + ' - SKU')
                                                            & (self.kpi_static_data['delete_time'].isnull())]

            def __return_valid_store_policies(policy):
                valid_store = True
                policy_json = json.loads(policy)
                # special case where its only one assortment for all
                # that is there is only one key and it is is_active => Y
                if len(policy_json) == 1 and policy_json.get('is_active') == ['Y']:
                    return valid_store

                store_json = json.loads(self.store_info.reset_index().to_json(orient='records'))[0]
                # map the necessary keys to those names knows
                for policy_value, store_info_value in POLICY_STORE_MAP.iteritems():
                    if policy_value in policy_json:
                        policy_json[store_info_value] = policy_json.pop(policy_value)
                for key, values in policy_json.iteritems():
                    if str(store_json.get(key, 'is_active')) in values:
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
            # Getting Template FK
            # It is assured there is one KPI for one template fk
            template_fk = 0
            if not valid_scif.empty:
                template_fk = int(valid_scif['template_fk'].unique()[0])
            Log.info("Calculate Assortment KPI for template: {}".format(template_fk))
            # calculate and save the percentage values for distribution and oos
            self.calculate_and_save_distribution(
                valid_scif=valid_scif,
                assortment_product_fks=valid_policy_data['product_fk'],
                distribution_kpi_fk=distribution_kpi.iloc[0].pk,
                dst_kpi_name=msl_store_level,
                template_fk=template_fk
            )
            # calculate and save prod presence and oos products
            self.calculate_and_save_prod_presence(
                valid_scif=valid_scif,
                assortment_product_fks=valid_policy_data['product_fk'],
                prod_presence_kpi_fk=prod_presence_kpi.iloc[0].pk,
                distribution_kpi_name=msl_store_level,
                template_fk=template_fk,
                distribution_kpi_fk=distribution_kpi.iloc[0].pk,
            )
            # calculate and save the percentage values for distribution and oos
            self.calculate_and_save_distribution_per_category(
                valid_scif=valid_scif,
                assortment_product_fks=valid_policy_data['product_fk'],
                distribution_kpi_fk=distribution_by_cat_kpi.iloc[0].pk,
                dst_kpi_name=msl_cat_level,
                template_fk=template_fk
            )
            # calculate and save prod presence and oos products
            self.calculate_and_save_prod_presence_per_category(
                valid_scif=valid_scif,
                assortment_product_fks=valid_policy_data['product_fk'],
                prod_presence_kpi_fk=prod_presence_by_cat_kpi.iloc[0].pk,
                distribution_kpi_name=msl_cat_level,
                template_fk=template_fk,
                distribution_kpi_fk=distribution_by_cat_kpi.iloc[0].pk,
            )

    def calculate_and_save_distribution(self, valid_scif, assortment_product_fks,
                                        distribution_kpi_fk, dst_kpi_name, template_fk):
        """Function to calculate distribution percentage.
        Saves distribution and oos percentage as values.
        """

        Log.info("Calculate {} distribution for {}".format(dst_kpi_name, self.session_uid))
        scene_products = pd.Series(valid_scif["item_id"].unique())
        total_products_in_assortment = len(assortment_product_fks)
        count_of_assortment_prod_in_scene = assortment_product_fks.isin(scene_products).sum()
        #  count of own man sku / all sku assortment count
        if not total_products_in_assortment:
            Log.info("No assortments applicable for session {sess}.".format(sess=self.session_uid))
            return 0
        distribution_perc = count_of_assortment_prod_in_scene / float(total_products_in_assortment) * 100
        self.common.write_to_db_result(fk=distribution_kpi_fk,
                                       numerator_id=self.own_man_fk,
                                       denominator_id=self.store_id,
                                       context_id=template_fk,
                                       numerator_result=count_of_assortment_prod_in_scene,
                                       denominator_result=total_products_in_assortment,
                                       result=distribution_perc,
                                       score=distribution_perc,
                                       identifier_result="{}_{}".format(distribution_kpi_fk,
                                                                        template_fk),
                                       should_enter=True
                                       )

    def calculate_and_save_prod_presence(self, valid_scif, assortment_product_fks,
                                         prod_presence_kpi_fk, distribution_kpi_name,
                                         template_fk, distribution_kpi_fk):
        # all assortment products are only in own manufacturers context;
        # but we have the products and hence no need to filter out denominator
        Log.info("Calculate {} - SKU for {}".format(distribution_kpi_name, self.session_uid))
        total_products_in_scene = valid_scif["item_id"].unique()
        # EXTRA is NOT based on own products
        # total_own_products_in_scene = valid_scif[valid_scif['manufacturer_fk']==self.own_man_fk]["item_id"].unique()
        present_products = np.intersect1d(total_products_in_scene, assortment_product_fks)
        extra_products = np.setdiff1d(total_products_in_scene, present_products)
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
                                               context_id=template_fk,
                                               result=assortment_code,
                                               score=assortment_code,
                                               identifier_result="{}_{}_{}_{}".format(
                                                   each_fk,
                                                   prod_presence_kpi_fk,
                                                   distribution_kpi_fk,
                                                   template_fk),
                                               identifier_parent="{}_{}".format(distribution_kpi_fk,
                                                                                template_fk),
                                               should_enter=True
                                               )

    def calculate_and_save_distribution_per_category(self, valid_scif, assortment_product_fks,
                                                     distribution_kpi_fk, dst_kpi_name, template_fk):
        """Function to calculate distribution and OOS percentage by Category.
        Saves distribution and oos percentage as values.
        """
        Log.info("Calculate {} for {}".format(dst_kpi_name, self.session_uid))
        categories_in_scif = valid_scif.category_fk.unique()
        categories_in_assortment = self.all_products[
            self.all_products.product_fk.isin(assortment_product_fks.tolist())].category_fk.unique()
        categories_to_save_zero = list(set(categories_in_assortment) - set(categories_in_scif))
        scene_category_group = valid_scif.groupby('category_fk')
        for category_fk, each_scif_data in scene_category_group:
            scene_products = pd.Series(each_scif_data["item_id"].unique())
            # find products in assortment belonging to category_fk
            curr_category_products_in_assortment = len(self.all_products[
                (self.all_products.product_fk.isin(assortment_product_fks))
                & (self.all_products.category_fk == category_fk)])
            count_of_assortment_prod_in_scene = assortment_product_fks.isin(scene_products).sum()
            #  count of own sku / all sku assortment count
            if not curr_category_products_in_assortment:
                Log.info("No products from assortment with category: {cat} found in session {sess}.".format(
                    cat=category_fk,
                    sess=self.session_uid))
                continue
            else:
                distribution_perc = count_of_assortment_prod_in_scene / float(curr_category_products_in_assortment) * 100
            self.common.write_to_db_result(fk=distribution_kpi_fk,
                                           numerator_id=self.own_man_fk,
                                           denominator_id=category_fk,
                                           context_id=template_fk,
                                           numerator_result=count_of_assortment_prod_in_scene,
                                           denominator_result=curr_category_products_in_assortment,
                                           result=distribution_perc,
                                           score=distribution_perc,
                                           identifier_result="{}_{}_{}".format(distribution_kpi_fk,
                                                                               category_fk,
                                                                               template_fk),
                                           should_enter=True
                                           )
        Log.info('Save zero MSL for the categories {} because they were not found in session.'.format(
            categories_to_save_zero))
        for each_cat_to_save_zero in categories_to_save_zero:
            self.common.write_to_db_result(fk=distribution_kpi_fk,
                                           numerator_id=self.own_man_fk,
                                           denominator_id=each_cat_to_save_zero,
                                           context_id=template_fk,
                                           numerator_result=0,
                                           denominator_result=0,
                                           result=0,
                                           score=0,
                                           identifier_result="{}_{}_{}".format(distribution_kpi_fk,
                                                                               each_cat_to_save_zero,
                                                                               template_fk),
                                           should_enter=True
                                           )

    def calculate_and_save_prod_presence_per_category(self, valid_scif, assortment_product_fks,
                                                      prod_presence_kpi_fk, distribution_kpi_name,
                                                      template_fk, distribution_kpi_fk):
        # all assortment products are only in own manufacturers context;
        # but we have the products and hence no need to filter out denominator
        Log.info("Calculate {} - SKU for {}".format(distribution_kpi_name, self.session_uid))
        categories_in_scif = valid_scif.category_fk.unique()
        categories_in_assortment = self.all_products[
            self.all_products.product_fk.isin(assortment_product_fks.tolist())].category_fk.unique()
        categories_to_save_zero = list(set(categories_in_assortment) - set(categories_in_scif))
        scene_category_group = valid_scif.groupby('category_fk')
        for category_fk, each_scif_data in scene_category_group:
            # EXTRA is NOT based on own products
            # total_own_products_in_scene_for_cat = each_scif_data[each_scif_data['manufacturer_fk'] ==
            # self.own_man_fk][
            #     "item_id"].unique()
            total_products_in_scene_for_cat = each_scif_data["item_id"].unique()
            curr_category_products_in_assortment_df = self.all_products[
                (self.all_products.product_fk.isin(assortment_product_fks))
                & (self.all_products.category_fk == category_fk)]
            curr_category_products_in_assortment = curr_category_products_in_assortment_df['product_fk'].unique()
            present_products = np.intersect1d(total_products_in_scene_for_cat, curr_category_products_in_assortment)
            extra_products = np.setdiff1d(total_products_in_scene_for_cat, present_products)
            oos_products = np.setdiff1d(curr_category_products_in_assortment, present_products)
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
                                                   denominator_id=category_fk,
                                                   context_id=template_fk,
                                                   result=assortment_code,
                                                   score=assortment_code,
                                                   identifier_result="{}_{}_{}_{}_{}".format(
                                                       each_fk,
                                                       prod_presence_kpi_fk,
                                                       distribution_kpi_fk,
                                                       template_fk,
                                                       category_fk
                                                   ),
                                                   identifier_parent="{}_{}_{}".format(distribution_kpi_fk,
                                                                                       category_fk,
                                                                                       template_fk
                                                                                       ),
                                                   should_enter=True
                                                   )
        # save OOS for products whose category is not present in session.
        for each_cat in categories_to_save_zero:
            curr_category_products_in_assortment_df = self.all_products[
                (self.all_products.product_fk.isin(assortment_product_fks))
                & (self.all_products.category_fk == each_cat)]
            curr_category_products_in_assortment = curr_category_products_in_assortment_df['product_fk'].unique()
            Log.info('Save OOS for products {prods} category {cat} because they were not found in session.'.format(
                prods=curr_category_products_in_assortment, cat=each_cat))
            for each_fk in curr_category_products_in_assortment:
                self.common.write_to_db_result(fk=prod_presence_kpi_fk,
                                               numerator_id=each_fk,
                                               denominator_id=each_cat,
                                               context_id=template_fk,
                                               result=OOS_CODE,
                                               score=OOS_CODE,
                                               identifier_result="{}_{}_{}_{}_{}".format(
                                                   each_fk,
                                                   prod_presence_kpi_fk,
                                                   distribution_kpi_fk,
                                                   template_fk,
                                                   each_cat
                                               ),
                                               identifier_parent="{}_{}_{}".format(distribution_kpi_fk,
                                                                                   each_cat,
                                                                                   template_fk
                                                                                   ),
                                               should_enter=True
                                               )

    def get_policies(self, kpi_fk):
        query = """ select a.kpi_fk, p.policy_name, p.policy, atag.assortment_group_fk,
                        atp.assortment_fk, atp.product_fk, atp.start_date, atp.end_date
                    from pservice.assortment_to_product atp 
                        join pservice.assortment_to_assortment_group atag on atp.assortment_fk = atag.assortment_fk 
                        join pservice.assortment a on a.pk = atag.assortment_group_fk
                        join pservice.policy p on p.pk = a.store_policy_group_fk
                    where a.kpi_fk={kpi_fk}
                    AND '{sess_date}' between atp.start_date AND atp.end_date;
                    """
        policies = pd.read_sql_query(query.format(kpi_fk=kpi_fk,
                                                  sess_date=self.session_info.iloc[0].visit_date),
                                     self.rds_conn.db)
        return policies

    def calculate_fsos_kpis(self):
        kpi_sheet = self.kpi_template.parse(KPI_NAMES_SHEET)
        kpi_sheet[KPI_FAMILY_COL] = kpi_sheet[KPI_FAMILY_COL].fillna(method='ffill')
        kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
        # get exclude include details from external targets
        kpi_include_exclude = self.external_targets[self.external_targets['kpi_type'] == 'FSOS_ALL']
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
                # gather include exclude
                include_exclude_data_dict = kpi_include_exclude.loc[:, ['include_brand_fks', 'include_category_fks',
                                                                        'template_fks', 'empty_exclude',
                                                                        'irrelevant_exclude',
                                                                        'others_exclude', 'stacking_exclude'
                                                                        ]].dropna(axis='columns').to_dict('records')[0]
                dataframe_to_process = self.get_sanitized_match_prod_scene(include_exclude_data_dict)
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
            scif_with_den_context = dataframe_to_process[
                [PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                 PARAM_DB_MAP[kpi['context'].iloc[0]]['key']]].drop_duplicates()
            df_with_den_context = dataframe_to_process.query(query_string)[[
                PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                PARAM_DB_MAP[kpi['context'].iloc[0]]['key']
            ]].drop_duplicates()
            _denominators_df_to_save_zero = pd.merge(scif_with_den_context, df_with_den_context,
                                                     how='outer', suffixes=('', '_y'), indicator=True)
            denominators_df_to_save_zero = _denominators_df_to_save_zero[
                _denominators_df_to_save_zero['_merge'] == 'left_only'][scif_with_den_context.columns]
            identifier_parent = None
            numerator_fk = self.own_man_fk
            result = numerator_result = 0  # SAVE ALL RESULTS AS ZERO
            denominators_df_to_save_zero.dropna(inplace=True)
            denominators_df_to_save_zero = denominators_df_to_save_zero.astype('int64')
            for idx, each_row in denominators_df_to_save_zero.iterrows():
                # get parent details
                param_id_map = dict(each_row.fillna('0'))
                cat_fk = param_id_map.get('category_fk', '')
                if not is_nan(kpi[KPI_PARENT_COL].iloc[0]):
                    kpi_parent = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi[KPI_PARENT_COL].iloc[0])
                                                      & (self.kpi_static_data['delete_time'].isnull())]
                    kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
                    kpi_parent_detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi_parent[KPI_TYPE_COL].values[0]]
                    parent_denominator_id = get_parameter_id(key_value=PARAM_DB_MAP[
                                                                    kpi_parent_detail['denominator'].iloc[0]]['key'],
                                                             param_id_map=param_id_map)
                    if parent_denominator_id is None:
                        parent_denominator_id = self.store_id
                    parent_context_id = get_parameter_id(key_value=PARAM_DB_MAP[
                                                                    kpi_parent_detail['context'].iloc[0]]['key'],
                                                         param_id_map=param_id_map)
                    if parent_context_id is None:
                        parent_context_id = self.store_id
                    identifier_parent = "{}_{}_{}_{}_{}_{}".format(
                        kpi_parent_detail['kpi_name'].iloc[0],
                        kpi_parent['pk'].iloc[0],
                        # parent_numerator_id,
                        int(parent_denominator_id),
                        int(parent_context_id),
                        cat_fk,
                        self.session_uid
                    )
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
                                               identifier_result="{}_{}_{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   each_den_fk,
                                                   context_id,
                                                   cat_fk,
                                                   self.session_uid
                                               ),
                                               identifier_parent=identifier_parent,
                                               should_enter=True,
                                               )
        stop_res_calc = True
        for group_id_tup, group_data in grouped_data_frame:
            if type(group_id_tup) not in [tuple, list]:
                # convert to a tuple
                group_id_tup = group_id_tup,
            param_id_map = dict(zip(groupers, group_id_tup))
            numerator_id = param_id_map.get(PARAM_DB_MAP[kpi['numerator'].iloc[0]]['key'])
            denominator_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi['denominator'].iloc[0]]['key'],
                                              param_id_map=param_id_map)
            if denominator_id is None:
                denominator_id = self.store_id
            context_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi['context'].iloc[0]]['key'],
                                          param_id_map=param_id_map)
            if context_id is None:
                context_id = self.store_id
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
            context_denominator_df = dataframe_to_process
            if len(groupers) > 1:
                context_denominator_df = dataframe_to_process.query('{key} == {value}'.format(
                    key=groupers[1],
                    value=get_parameter_id(key_value=groupers[1], param_id_map=param_id_map)))
                if 'category_fk' in groupers and 'template_fk' in groupers:
                    context_denominator_df = context_denominator_df.query('{key} == {value}'.format(
                        key='template_fk',
                        value=param_id_map.get('template_fk')))
            result = len(group_data) / float(len(context_denominator_df))
            cat_fk = param_id_map.get('category_fk', '')
            if not is_nan(kpi[KPI_PARENT_COL].iloc[0]):
                kpi_parent = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == kpi[KPI_PARENT_COL].iloc[0])
                                                  & (self.kpi_static_data['delete_time'].isnull())]
                kpi_details = self.kpi_template.parse(KPI_DETAILS_SHEET)
                kpi_parent_detail = kpi_details[kpi_details[KPI_NAME_COL] == kpi_parent[KPI_TYPE_COL].values[0]]
                parent_denominator_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi_parent_detail['denominator'].iloc[0]]['key'],
                                                         param_id_map=param_id_map)
                if parent_denominator_id is None:
                    parent_denominator_id = self.store_id
                parent_context_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi_parent_detail['context'].iloc[0]]['key'],
                                                     param_id_map=param_id_map)
                if parent_context_id is None:
                    parent_context_id = self.store_id
                self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                               numerator_id=numerator_id,
                                               denominator_id=denominator_id,
                                               context_id=context_id,
                                               result=result,
                                               numerator_result=len(group_data),
                                               denominator_result=len(context_denominator_df),
                                               identifier_result="{}_{}_{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   denominator_id,
                                                   context_id,
                                                   cat_fk,
                                                   self.session_uid
                                               ),
                                               identifier_parent="{}_{}_{}_{}_{}_{}".format(
                                                   kpi_parent_detail['kpi_name'].iloc[0],
                                                   kpi_parent['pk'].iloc[0],
                                                   # parent_numerator_id,
                                                   parent_denominator_id,
                                                   parent_context_id,
                                                   cat_fk,
                                                   self.session_uid
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
                                               denominator_result=len(context_denominator_df),
                                               identifier_result="{}_{}_{}_{}_{}_{}".format(
                                                   kpi['kpi_name'].iloc[0],
                                                   kpi['pk'].iloc[0],
                                                   # numerator_id,
                                                   denominator_id,
                                                   context_id,
                                                   cat_fk,
                                                   self.session_uid
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
            context_id = get_parameter_id(key_value=PARAM_DB_MAP[kpi['context'].iloc[0]]['key'],
                                          param_id_map=param_id_map)
            if context_id is None:
                context_id = self.store_id
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
        empty_exclude = bool(int(include_exclude_data_dict.get('empty_exclude', 0)))
        irrelevant_exclude = bool(int(include_exclude_data_dict.get('irrelevant_exclude', 0)))
        others_exclude = bool(int(include_exclude_data_dict.get('others_exclude', 0)))
        stacking_exclude = bool(int(include_exclude_data_dict.get('stacking_exclude', 0)))

        # list
        scene_types_to_include = include_exclude_data_dict.get('template_fks', False)
        categories_to_include = include_exclude_data_dict.get('include_category_fks', False)
        brands_to_include = include_exclude_data_dict.get('include_brand_fks', False)
        ean_codes_to_exclude = include_exclude_data_dict.get('exclude_sku_fks', False)
        # Start removing items
        if scene_types_to_include and not is_nan(scene_types_to_include):
            # list of scene types to include is present, otherwise all included
            Log.info("Include only template/scene type fks: {}".format(scene_types_to_include))
            if type(scene_types_to_include) != list:
                scene_types_to_include = [scene_types_to_include]
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['template_fk'].isin(scene_types_to_include)]
        if stacking_exclude and not is_nan(stacking_exclude):
            # exclude stacking if the flag is set
            Log.info("Exclude stacking; exclude other than in layer 1 or negative stacking [menu]")
            sanitized_products_in_scene = sanitized_products_in_scene.loc[
                sanitized_products_in_scene['stacking_layer'] <= 1]
        if categories_to_include and not is_nan(categories_to_include):
            # list of categories to exclude is present, otherwise all included
            Log.info("Inlcude only categories: {}".format(categories_to_include))
            if type(categories_to_include) != list:
                categories_to_include = [categories_to_include]
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['category_fk'].isin(categories_to_include)]
        if brands_to_include and not is_nan(brands_to_include):
            # list of brands to exclude is present, otherwise all included
            Log.info("Include only brands: {}".format(brands_to_include))
            if type(brands_to_include) != list:
                brands_to_include = [brands_to_include]
            sanitized_products_in_scene = sanitized_products_in_scene[
                sanitized_products_in_scene['brand_fk'].isin(brands_to_include)]
        if ean_codes_to_exclude and not is_nan(ean_codes_to_exclude):
            # list of ean_codes to exclude is present, otherwise all included
            Log.info("Exclude ean codes {}".format(ean_codes_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[sanitized_products_in_scene['product_ean_code'].str.upper().isin(
                    [x.upper() if type(x) in [unicode, str] else x for x in ean_codes_to_exclude]
                )].index,
                inplace=True
            )
        product_ids_to_exclude = []
        if irrelevant_exclude:
            # add product ids to exclude with irrelevant
            product_ids_to_exclude.extend(self.irrelevant_prod_ids)
        if others_exclude:
            # add product ids to exclude with others
            product_ids_to_exclude.extend(self.other_prod_ids)
        if empty_exclude:
            # add product ids to exclude with empty
            product_ids_to_exclude.extend(self.empty_prod_ids)
        if product_ids_to_exclude and not is_nan(ean_codes_to_exclude):
            Log.info("Exclude product ids {}".format(product_ids_to_exclude))
            sanitized_products_in_scene.drop(
                sanitized_products_in_scene[
                    sanitized_products_in_scene['product_fk'].isin(product_ids_to_exclude)].index,
                inplace=True
            )
        return sanitized_products_in_scene

    def calculate_fsos_sku(self):
        # get exclude include details from external targets
        kpi_include_exclude = self.external_targets[self.external_targets['kpi_type'] == FSOS_AT_SKU]
        kpi = self.kpi_static_data[(self.kpi_static_data[KPI_TYPE_COL] == 'FSOS_ALL')
                                   & (self.kpi_static_data['delete_time'].isnull())]
        if kpi.empty:
            Log.warning("*** KPI Name:{name} not found in DB for session {sess} ***".format(
                name=FSOS_AT_SKU,
                sess=self.session_uid
            ))
            return False
        else:
            Log.info("KPI Name:{name} found in DB for session {sess}".format(
                name=FSOS_AT_SKU,
                sess=self.session_uid
            ))
            # gather include exclude
            include_exclude_data_dict = kpi_include_exclude.loc[:, ['include_brand_fks', 'include_category_fks',
                                                                    'template_fks', 'empty_exclude',
                                                                    'irrelevant_exclude',
                                                                    'others_exclude', 'stacking_exclude'
                                                                    ]].dropna(axis='columns').to_dict(
                'records')[0]
            dataframe_to_process = self.get_sanitized_match_prod_scene(include_exclude_data_dict)
            if dataframe_to_process.empty:
                Log.info("No data to calculate KPI Name:{name} for session {sess}. Check store and targets.".format(
                    name=FSOS_AT_SKU,
                    sess=self.session_uid
                ))
            for group_ids, group_data in dataframe_to_process.groupby(['template_fk', 'product_fk']):
                template_fk, product_fk = group_ids
                Log.info("Product: {prod_pk}, Session: {sess}, Presence: {num}, Outof: {den}, Result: {res}".format(
                    prod_pk=product_fk,
                    sess=self.session_uid,
                    num=len(group_data),
                    den=len(dataframe_to_process),
                    res=len(group_data) / float(len(dataframe_to_process)),
                ))
                self.common.write_to_db_result(fk=kpi['pk'].iloc[0],
                                               numerator_id=product_fk,
                                               denominator_id=self.store_id,
                                               context_id=template_fk,
                                               numerator_result=len(group_data),
                                               denominator_result=len(dataframe_to_process),
                                               result=round(
                                                   (len(group_data)/float(len(dataframe_to_process))) * 100, 2),
                                               )


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
