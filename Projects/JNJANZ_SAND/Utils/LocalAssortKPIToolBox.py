import pandas as pd
import numpy as np
import json as json
import math
import KPIUtils.Utils.Templates.TemplateUploader as assTemplate

from datetime import datetime
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils.GlobalProjects.JNJ.Utils.GeneralToolBox import JNJGENERALToolBox
from KPIUtils.GlobalProjects.JNJ.Utils.Fetcher import JNJQueries
from KPIUtils.Calculations.Assortment import Assortment
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
import KPIUtils_v2.Utils.Parsers.ParseInputKPI as Parser

__author__ = 'prasanna'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_NEW_TABLE = 'report.kpi_level_2_results'
SUB_CATEGORY = 'sub_category'


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


class JNJToolBox:

    NUMERATOR = 'numerator'
    DENOMINATOR = 'denominator'
    SP_LOCATION_KPI = 'secondary placement location quality'
    SP_LOCATION_QUALITY_KPI = 'secondary placement location visibility quality'
    LVL3_HEADERS = ['assortment_group_fk', 'assortment_fk', 'target', 'product_fk', 'in_store',
                    'kpi_fk_lvl1', 'kpi_fk_lvl2', 'kpi_fk_lvl3', 'group_target_date',
                    'assortment_super_group_fk', 'super_group_target']
    LVL2_HEADERS = ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target',
                    'passes', 'total', 'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date',
                    'super_group_target']
    LVL1_HEADERS = ['assortment_super_group_fk', 'assortment_group_fk', 'super_group_target',
                    'passes', 'total', 'kpi_fk_lvl1']
    ASSORTMENT_FK = 'assortment_fk'
    ASSORTMENT_GROUP_FK = 'assortment_group_fk'
    ASSORTMENT_SUPER_GROUP_FK = 'assortment_super_group_fk'

    # local_msl availability
    LOCAL_MSL_AVAILABILITY = 'local_msl'
    LOCAL_MSL_AVAILABILITY_SKU = 'local_msl - SKU'

    # jnjanz local msl/oos KPIs

    OOS_BY_LOCAL_ASSORT_STORE_KPI = 'OOS_BY_LOCAL_ASSORT_STORE'
    OOS_BY_LOCAL_ASSORT_PRODUCT = 'OOS_BY_LOCAL_ASSORT_PRODUCT'
    OOS_BY_LOCAL_ASSORT_CATEGORY = 'OOS_BY_LOCAL_ASSORT_CATEGORY'
    OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY = 'OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY'
    OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT = 'OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT'
    MSL_BY_LOCAL_ASSORT = 'MSL_BY_LOCAL_ASSORT'
    MSL_BY_LOCAL_ASSORT_PRODUCT = 'MSL_BY_LOCAL_ASSORT_PRODUCT'
    MSL_BY_LOCAL_ASSORT_CATEGORY = 'MSL_BY_LOCAL_ASSORT_CATEGORY'
    MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY = 'MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY'
    MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT = 'MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT'

    # msl availability
    MSL_AVAILABILITY = 'MSL'
    MSL_AVAILABILITY_SKU = 'MSL - SKU'

    JNJ = 'JOHNSON & JOHNSON'
    TYPE_SKU = 'SKU'
    TYPE_OTHER = 'Other'

    SUCCESSFUL = [1, 4]
    OTHER = 'Other'

    YES = 'Yes'
    NO = 'No'

    OOS = 'OOS'
    DISTRIBUTED = 'DISTRIBUTED'
    EXTRA = 'EXTRA'

    def __init__(self, data_provider, output, common, exclusive_template):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products_i_d = self.data_provider[Data.ALL_PRODUCTS_INCLUDING_DELETED]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.templates = self.data_provider[Data.ALL_TEMPLATES]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = JNJGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.common = common
        self.New_kpi_static_data = common.get_new_kpi_static_data()
        self.kpi_results_new_tables_queries = []
        self.all_products = self.ps_data_provider.get_sub_category(self.all_products, 'sub_category_local_name')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.current_date = datetime.now()
        self.labels = self.ps_data_provider.get_labels()
        self.products_in_ass = []
        self.products_to_ass = pd.DataFrame(columns=assTemplate.COLUMNS_ASSORTMENT_DEFINITION_SHEET)
        self.assortment_policy = pd.DataFrame(columns=assTemplate.COLUMNS_STORE_ATTRIBUTES_TO_ASSORT)
        self.ass_deleted_prod = pd.DataFrame(columns=[assTemplate.COLUMN_GRANULAR_GROUP, assTemplate.COLUMN_EAN_CODE])
        self.session_category_info = pd.DataFrame()
        self.session_products = pd.DataFrame()
        self.assortment = Assortment(self.data_provider, self.output, self.ps_data_provider)
        self.products_to_remove = []
        self.ignore_from_top = 1
        self.start_shelf = 3
        self.products_for_ass_new = pd.DataFrame(columns=['session_id', 'product_fk'])
        self.prev_session_products_new_ass = pd.DataFrame()
        self.session_category_new_ass = pd.DataFrame()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.kpi_result_values = self.get_kpi_result_values_df()
        self.parser = Parser
        self.exclusive_template = exclusive_template
        self.result_values = self.ps_data_provider.get_result_values()

    def get_kpi_result_values_df(self):
        query = JNJQueries.get_kpi_result_values()
        query_result = pd.read_sql_query(query, self.rds_conn.db)
        return query_result

    def get_session_products(self, session):
        return self.session_products[self.session_products['session_id'] == session]

    def result_value_pk(self, result):
            """
            converts string result to its pk (in static.kpi_result_value)
            :param result: str
            :return: int
            """
            pk = self.result_values[self.result_values['value'] == result]["pk"].iloc[0]
            return pk

    @staticmethod
    def split_and_strip(value):
        return map(lambda x: x.strip(), value.split(';'))

    def reset_scif_and_matches(self):
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS].copy()
        self.match_product_in_scene = self.data_provider[Data.MATCHES].copy()

    def filter_scif_matches_for_kpi(self, kpi_name):
        if not self.exclusive_template.empty:
            template_filters = {}
            kpi_filters_df = self.exclusive_template[self.exclusive_template['KPI'] == kpi_name]
            if kpi_filters_df.empty:
                return
            if not kpi_filters_df.empty:
                if kpi_filters_df['Exclude1'].values[0]:
                    template_filters.update({kpi_filters_df['Exclude1'].values[0]: (
                        self.split_and_strip(kpi_filters_df['Value1'].values[0]), 0)})
                if kpi_filters_df['Exclude2'].values[0]:
                    template_filters.update({kpi_filters_df['Exclude2'].values[0]: (
                        self.split_and_strip(kpi_filters_df['Value2'].values[0]), 0)})
                if 'Exclude3' in kpi_filters_df.columns.values:
                    if kpi_filters_df['Exclude3'].values[0]:
                        template_filters.update({kpi_filters_df['Exclude3'].values[0]: (
                            self.split_and_strip(kpi_filters_df['Value3'].values[0]), 0)})
                if 'Exclude4' in kpi_filters_df.columns.values:
                    if kpi_filters_df['Exclude4'].values[0]:
                        template_filters.update({template_filters['Exclude4'].values[0]: (
                            self.split_and_strip(template_filters['Value4'].values[0]), 0)})

                filters = self.get_filters_for_scif_and_matches(template_filters)
                self.scif = self.scif[
                    self.tools.get_filter_condition(self.scif,**filters)
                ]
                self.match_product_in_scene = self.match_product_in_scene[
                    self.tools.get_filter_condition(self.match_product_in_scene,**filters)
                ]

    def get_filters_for_scif_and_matches(self, template_filters):
        product_keys = filter(lambda x: x in self.data_provider[Data.ALL_PRODUCTS].columns.values.tolist(),
                              template_filters.keys())
        scene_keys = filter(lambda x: x in self.data_provider[Data.ALL_TEMPLATES].columns.values.tolist(),
                            template_filters.keys())
        product_filters = {}
        scene_filters = {}
        filters_all = {}
        for key in product_keys:
            product_filters.update({key: template_filters[key]})
        for key in scene_keys:
            scene_filters.update({key: template_filters[key]})

        if product_filters:
            product_fks = self.get_fk_from_filters(product_filters)
            filters_all.update({'product_fk': product_fks})
        if scene_filters:
            scene_fks = self.get_scene_fk_from_filters(scene_filters)
            filters_all.update({'scene_fk': scene_fks})
        return filters_all

    def get_fk_from_filters(self, filters):
        all_products = self.data_provider.all_products
        product_fk_list = all_products[self.tools.get_filter_condition(all_products, **filters)]
        product_fk_list = product_fk_list['product_fk'].unique().tolist()
        return product_fk_list

    def get_scene_fk_from_filters(self, filters):
        scif_data = self.data_provider[Data.SCENE_ITEM_FACTS]
        scene_fk_list = scif_data[self.tools.get_filter_condition(scif_data, **filters)]
        scene_fk_list = scene_fk_list['scene_fk'].unique().tolist()
        return scene_fk_list

    def get_own_manufacturer_skus_in_scif(self):
        # Filter scif by own_manufacturer & product_type = 'SKU'
        return self.scif[(self.scif.manufacturer_fk == self.own_manuf_fk)
                         & (self.scif.product_type == "SKU")
                         & (self.scif["facings"] > 0)]['item_id'].unique().tolist()

    def fetch_local_assortment_products(self):
        # TODO Fix with real assortment

        lvl3_assortment = self.assortment.get_lvl3_relevant_ass()
        local_msl_ass_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                              self.LOCAL_MSL_AVAILABILITY]['pk'].drop_duplicates().values[0]
        local_msl_ass_sku_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                  self.LOCAL_MSL_AVAILABILITY_SKU]['pk'].drop_duplicates().values[0]
        if lvl3_assortment.empty:
            return
        lvl3_assortment = lvl3_assortment[lvl3_assortment['kpi_fk_lvl3'] == local_msl_ass_sku_fk]
        if lvl3_assortment.empty:
            return

        assortments = lvl3_assortment['assortment_group_fk'].unique()
        products_in_ass = []
        for assortment in assortments:
            current_assortment = lvl3_assortment[lvl3_assortment['assortment_group_fk'] == assortment]
            current_assortment_product_fks =  list(
                current_assortment[~current_assortment['product_fk'].isna()]['product_fk'].unique()
            )
            products_in_ass.extend( current_assortment_product_fks )

        #ignore None if anty
        products_in_ass = [ p for p in products_in_ass if not ((p == None) or p == 'None')]

        return products_in_ass, lvl3_assortment

    @kpi_runtime()
    def local_assortment_hierarchy_per_store_calc(self):
        Log.debug("starting local_assortment calc")
        self.products_in_ass, lvl3_assortment = self.fetch_local_assortment_products()
        self.products_in_ass = np.unique(self.products_in_ass)
        if len(self.products_in_ass) == 0:
            Log.warning("No assortment products were found the requested session")
            return
        self.local_assortment_hierarchy_per_category_and_subcategory()

        oos_per_product_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                          self.OOS_BY_LOCAL_ASSORT_PRODUCT]['pk'].values[0]
        msl_per_product_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                          self.MSL_BY_LOCAL_ASSORT_PRODUCT]['pk'].values[0]
        products_in_session = self.scif['item_id'].drop_duplicates().values

        for sku in self.products_in_ass:
            if sku in products_in_session:
                result = self.result_value_pk(self.DISTRIBUTED)
                result_num = 1
            else:
                result = self.result_value_pk(self.OOS)
                result_num = 0
                # Saving OOS
                self.common.write_to_db_result(fk=oos_per_product_kpi_fk, numerator_id=sku, numerator_result=result,
                                               result=result, denominator_id=self.own_manuf_fk,
                                               denominator_result=1,
                                               score=result, identifier_parent="OOS_Local_store",
                                               should_enter=True)

            # Saving MSL
            self.common.write_to_db_result(fk=msl_per_product_kpi_fk, numerator_id=sku, numerator_result=result_num,
                                           result=result, denominator_id=self.own_manuf_fk, denominator_result=1,
                                           score=result, identifier_parent="MSL_Local_store", should_enter=True)

        # Saving MSL - Extra
        # Add the Extra Products found in Session from same manufacturer into MSL
        own_manufacturer_skus = self.get_own_manufacturer_skus_in_scif()
        extra_products_in_scene = set(products_in_session) - set(self.products_in_ass)
        for sku in extra_products_in_scene:
            if sku in own_manufacturer_skus:
                result = self.result_value_pk(self.EXTRA)  # Extra
                result_num = 1
                self.common.write_to_db_result(fk=msl_per_product_kpi_fk, numerator_id=sku, numerator_result=result_num,
                                               result=result, denominator_id=self.own_manuf_fk, denominator_result=1,
                                               score=result, identifier_parent="MSL_Local_store", should_enter=True)

        oos_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                              self.OOS_BY_LOCAL_ASSORT_STORE_KPI]['pk'].values[0]
        msl_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                              self.MSL_BY_LOCAL_ASSORT]['pk'].values[0]
        denominator = len(self.products_in_ass)

        # Saving OOS
        oos_numerator = len(list(set(self.products_in_ass) - set(products_in_session)))
        oos_res = round((oos_numerator / float(denominator)), 4) if denominator != 0 else 0
        self.common.write_to_db_result(fk=oos_kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                       numerator_result=oos_numerator,
                                       result=oos_res, denominator_result=denominator, score=oos_res,
                                       identifier_result="OOS_Local_store")

        # Saving MSL
        msl_numerator = len(list(set(self.products_in_ass) & set(products_in_session)))
        msl_res = round((msl_numerator / float(denominator)), 4) if denominator != 0 else 0
        self.common.write_to_db_result(fk=msl_kpi_fk, numerator_id=self.own_manuf_fk, denominator_id=self.store_id,
                                       numerator_result=msl_numerator,
                                       result=msl_res, denominator_result=denominator, score=msl_res,
                                       identifier_result="MSL_Local_store")
        Log.debug("finishing oos_per_store_calc")
        return

    def local_assortment_hierarchy_per_category_and_subcategory(self):
        Log.debug("starting oos_per_category_per_sub_category_per_product")
        products_in_session = self.scif['product_fk'].drop_duplicates().values

        # OOS KPIs
        oos_cat_subcat_sku_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                             self.OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT]['pk'].values[0]
        oos_cat_subcat_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                         self.OOS_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY]['pk'].values[0]
        oos_cat_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                  self.OOS_BY_LOCAL_ASSORT_CATEGORY]['pk'].values[0]
        # MSL KPIs
        msl_cat_subcat_sku_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                             self.MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY_PRODUCT]['pk'].values[0]
        msl_cat_subcat_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                         self.MSL_BY_LOCAL_ASSORT_CATEGORY_SUB_CATEGORY]['pk'].values[0]
        msl_cat_kpi_fk = self.New_kpi_static_data[self.New_kpi_static_data['client_name'] ==
                                                  self.MSL_BY_LOCAL_ASSORT_CATEGORY]['pk'].values[0]
        categories = self.all_products[self.all_products['product_fk'].isin(self.products_in_ass)] \
            ['category_fk'].drop_duplicates().values
        for category in categories:
            products_in_cat = self.all_products[self.all_products['category_fk'] == category][
                'product_fk'].drop_duplicates().values
            relevant_for_ass = list(set(self.products_in_ass) & set(products_in_cat))
            denominator = len(relevant_for_ass)

            # Saving OOS
            oos_numerator = len(list(set(relevant_for_ass) - set(products_in_session)))
            oos_res = round((oos_numerator / float(denominator)), 4) if denominator != 0 else 0
            self.common.write_to_db_result(fk=oos_cat_kpi_fk, numerator_id=self.own_manuf_fk,
                                           numerator_result=oos_numerator,
                                           result=oos_res, denominator_id=category,
                                           denominator_result=denominator, score=oos_res,
                                           identifier_result="OOS_Local_cat_" + str(int(category)))

            # Saving MSL
            msl_numerator = len(list(set(relevant_for_ass) & set(products_in_session)))
            msl_res = round((msl_numerator / float(denominator)), 4) if denominator != 0 else 0
            self.common.write_to_db_result(fk=msl_cat_kpi_fk, numerator_id=self.own_manuf_fk,
                                           numerator_result=msl_numerator,
                                           result=msl_res, denominator_id=category,
                                           denominator_result=denominator, score=msl_res,
                                           identifier_result="MSL_Local_cat_" + str(int(category)))

            sub_categories = self.all_products[(self.all_products['product_fk'].isin(self.products_in_ass) &
                                                (self.all_products['category_fk'] ==
                                                 category))]['sub_category_fk'].drop_duplicates().values
            for sub_category in sub_categories:
                products_in_sub_cat = self.all_products[(self.all_products['sub_category_fk'] == sub_category) &
                                                        (self.all_products['category_fk'] == category)][
                    'product_fk'].drop_duplicates().values
                relevant_for_ass = list(set(self.products_in_ass) & set(products_in_sub_cat))
                denominator = len(relevant_for_ass)

                # Saving OOS
                oos_numerator = len(list(set(relevant_for_ass) - set(products_in_session)))
                oos_res = round((oos_numerator / float(denominator)), 4) if denominator != 0 else 0
                self.common.write_to_db_result(fk=oos_cat_subcat_kpi_fk, numerator_id=self.own_manuf_fk,
                                               numerator_result=oos_numerator, result=oos_res,
                                               denominator_id=sub_category,
                                               denominator_result=denominator, score=oos_res,
                                               identifier_result="OOS_Local_subcat_" + str(int(sub_category)),
                                               identifier_parent="OOS_Local_cat_" + str(int(category)),
                                               should_enter=True)

                # Saving MSL
                msl_numerator = len(list(set(relevant_for_ass) & set(products_in_session)))
                msl_res = round((msl_numerator / float(denominator)), 4) if denominator != 0 else 0
                self.common.write_to_db_result(fk=msl_cat_subcat_kpi_fk, numerator_id=self.own_manuf_fk,
                                               numerator_result=msl_numerator, result=msl_res,
                                               denominator_id=sub_category,
                                               denominator_result=denominator, score=msl_res,
                                               identifier_result="MSL_Local_subcat_" + str(int(sub_category)),
                                               identifier_parent="MSL_Local_cat_" + str(int(category)),
                                               should_enter=True)

                for sku in relevant_for_ass:
                    if sku in products_in_session:
                        result = self.result_value_pk(self.DISTRIBUTED)
                        result_num = 1
                    else:
                        result = self.result_value_pk(self.OOS)
                        result_num = 0
                        # Saving OOS
                        self.common.write_to_db_result(fk=oos_cat_subcat_sku_kpi_fk, result=result, score=result,
                                                       numerator_id=sku, numerator_result=result,
                                                       denominator_id=sub_category,
                                                       denominator_result=1,
                                                       identifier_parent="OOS_Local_subcat_" + str(int(sub_category)),
                                                       should_enter=True)

                    # Saving MSL
                    self.common.write_to_db_result(fk=msl_cat_subcat_sku_kpi_fk, result=result, score=result,
                                                   numerator_id=sku, numerator_result=result_num,
                                                   denominator_id=sub_category,
                                                   denominator_result=1,
                                                   identifier_parent="MSL_Local_subcat_" + str(int(sub_category)),
                                                   should_enter=True)


        Log.debug("finishing assortment_per_category")
        return

    def main_calculation(self):
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
                Log.warning('Unable to calculate local_msl assortment KPIs: SCIF  is empty')
                return 0
            self.reset_scif_and_matches()
            self.filter_scif_matches_for_kpi("local_msl")
            self.local_assortment_hierarchy_per_store_calc()
        except Exception as e:
            Log.error("Error: {}".format(e))
        return 0
