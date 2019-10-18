from __builtin__ import unicode

import math
import ast
import os
import pandas as pd
import numpy as np
from datetime import datetime


from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from KPIUtils_v2.Utils.Consts.GlobalConsts import BasicConsts, HelperConsts, ProductTypeConsts
from KPIUtils_v2.Utils.Consts.DataProvider import SessionInfoConsts, ProductsConsts, ScifConsts, MatchesConsts
from KPIUtils_v2.Utils.Consts.PS import AssortmentProductConsts
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from Projects.CCJP.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log

__author__ = 'satya'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, set_up_file):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.result_values = self.ps_data.get_result_values()
        self.shelf_placement_file = None
        self.tools = GENERALToolBox(self.data_provider)
        self.sub_category = self.ps_data.get_sub_category(self.all_products)
        self.New_kpi_static_data = self.common.get_new_kpi_static_data()
        self.current_date = datetime.now()
        self.store_fk = self.data_provider[SessionInfoConsts.STORE_FK]
        self.assortment = Assortment(self.data_provider, self.output)
        self.set_up_data = {
                            ("Facings SOS", Consts.KPI_TYPE_COLUMN): Consts.NO_INFO,
                            ("Availability", Consts.KPI_TYPE_COLUMN): Consts.NO_INFO
                            }
        self.manufacturer_fk = None if self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0] is None else \
            int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
        self.assort_lvl3 = None
        self.last_session_uid = self.ps_data.get_last_session()
        self.last_results = self.ps_data.get_last_status(self.last_session_uid, 3)

        set_up_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..',  'Data', set_up_file)
        temp_file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', "template.xlsx")

        self.poc_template = pd.read_excel(temp_file_name, sheet_name='poc_config', keep_default_na=False)
        set_up_template = pd.read_excel(set_up_file_name, sheet_name='Functional KPIs', keep_default_na=False)
        self.set_up_file = set_up_template
        self.store = Consts.STORE

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        red_score_dict = []
        unique_sku_sos = []

        assortment_store_dict = self.availability_store_function()
        if assortment_store_dict is None:
            assortment_store_dict = []

        assortment_category_dict = self.availability_category_function()
        if assortment_category_dict is None:
            assortment_category_dict = []

        facings_sos_whole_store_dict = self.facings_sos_whole_store_function()
        if facings_sos_whole_store_dict is None:
            facings_sos_whole_store_dict = []

        facings_sos_by_category_dict = self.facings_sos_by_category_function()
        if facings_sos_by_category_dict is None:
            facings_sos_by_category_dict = []

        kpi_names = self.get_kpi_params()

        point_of_store_dict = []
        for row_num, row_data in kpi_names.iterrows():
            kpi_name = row_data[Consts.KPI_TYPE_COLUMN]
            if kpi_name == "CCJP_POC_COUNT_BY_STORE_AREA":
                point_of_store_dict = self.point_of_connection()
                if point_of_store_dict is None:
                    point_of_store_dict = []

        # Added additional loop because poc count is required for red score calculation.
        # Entering the poc kpi in the excel sheet before red score kpi is not working always.

        for row_num, row_data in kpi_names.iterrows():
            kpi_name = row_data[Consts.KPI_TYPE_COLUMN]
            if kpi_name == "CCJP_RED_SCORE":
                red_score_dict = self.calculate_red_score(facings_sos_whole_store_dict,
                                                          point_of_store_dict,
                                                          assortment_store_dict, row_data)
            elif kpi_name == 'CCJP_UNIQUE_DIST_OWN_MANU':
                unique_sku_sos = self.calculate_unique_sku_sos(row_data)

        self.common.save_json_to_new_tables(assortment_store_dict)
        self.common.save_json_to_new_tables(assortment_category_dict)

        if facings_sos_whole_store_dict is None:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.common.save_json_to_new_tables(facings_sos_whole_store_dict)

        if facings_sos_by_category_dict is None:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.common.save_json_to_new_tables(facings_sos_by_category_dict)

        if point_of_store_dict is None:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.common.save_json_to_new_tables(point_of_store_dict)

        if red_score_dict is None:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.common.save_json_to_new_tables(red_score_dict)

        if unique_sku_sos is None:
            Log.warning('Scene item facts is empty for this session')
        else:
            self.common.save_json_to_new_tables(unique_sku_sos)

        self.common.commit_results_data()
        score = 0
        return score

    def assortment_calculation(self, assort_lvl3, denominator_fk, availability_type):
        """
           :param denominator_fk  : store_fk/category_fk/sub_category_fk
           :param assort_lvl3  : df of  level 3 assortment calculation
           :param availability_type : "Store"/"Category"/"SubCategory"
           :return results_df that contains:
               - result in SKU level :  distribution 1/2/3  oos 1/2
               - result : stock rate for oos and distribution

        """
        dict_list = []

        oos_fk = self.common.get_kpi_fk_by_kpi_type(
            Consts.OOS_PREFIX + Consts.AVAILABILITY_DICT[('OOS', availability_type)][0])
        dist_fk = self.common.get_kpi_fk_by_kpi_type(
            Consts.DISTRIBUTION_PREFIX + Consts.AVAILABILITY_DICT[('DIST', availability_type)][0])
        category_fk = None
        if availability_type is Consts.SUB_CATEGORY:
            category_fk = assort_lvl3['category_fk'].iloc[0]

        if not assort_lvl3.empty:
            if availability_type is not Consts.SUB_CATEGORY_PARENT:
                oos_sku_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.OOS_SKU_PREFIX + Consts.AVAILABILITY_DICT[('OOS', availability_type)][1])
                dist_sku_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.DISTRIBUTION_SKU_PREFIX + Consts.AVAILABILITY_DICT[('DIST', availability_type)][1])
                dist_origin_kpi = self.common.get_kpi_fk_by_kpi_name(Consts.DISTRIBUTION)

                for result in assort_lvl3.itertuples():
                    if (math.isnan(result.in_store)) | (result.kpi_fk_lvl3 != dist_origin_kpi):
                        score = self.result_value_pk(Consts.EXTRA)
                        result_num = 1
                    else:
                        score = self.result_value_pk(Consts.OOS) \
                            if result.in_store == 0 else self.result_value_pk(Consts.DISTRIBUTED)
                        result_num = result.in_store
                    last_status = self.get_last_status(dist_sku_fk, result.product_fk)
                    dict_list.append(
                        self.build_dictionary_for_db_insert_v2(
                            fk=dist_sku_fk, numerator_id=result.product_fk, numerator_result=result_num,
                            result=score, score=last_status, denominator_id=denominator_fk, denominator_result=1,
                            identifier_parent=self.common.get_dictionary(kpi_parent=dist_fk,
                                                                         type=availability_type,
                                                                         type_fk=denominator_fk)))

                    # OOS
                    if score == self.result_value_pk(Consts.OOS):
                        dict_list.append(self.build_dictionary_for_db_insert_v2(
                            fk=oos_sku_fk, numerator_id=result.product_fk, numerator_result=score, result=score,
                            score=score
                            , denominator_id=denominator_fk, denominator_result=1, identifier_parent=
                            self.common.get_dictionary(kpi_parent=oos_fk, type=availability_type,
                                                       type_fk=denominator_fk)))

            assort_lvl3 = assort_lvl3[~assort_lvl3['in_store'].isnull()]
            if assort_lvl3.empty:
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=0,
                    result=0, score=0, denominator_id=denominator_fk,
                    denominator_result=0,
                    identifier_result=self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type,
                                                                 type_fk=denominator_fk)))
                return dict_list
            oos_numerator = len(assort_lvl3[assort_lvl3['in_store'] == 0])
            denominator = len(assort_lvl3)
            oos_res = np.divide(float(oos_numerator), float(denominator)) * 100
            if availability_type is Consts.SUB_CATEGORY:
                oos_parent_fk = self.common.get_kpi_fk_by_kpi_type(
                    Consts.OOS_PREFIX + Consts.AVAILABILITY_DICT[('OOS', Consts.SUB_CATEGORY_PARENT)][0])
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=oos_fk,
                    numerator_id=self.manufacturer_fk,
                    numerator_result=oos_numerator,
                    result=oos_res,
                    score=oos_res,
                    denominator_result=denominator,
                    denominator_id=denominator_fk,
                    identifier_result=self.common.get_dictionary(kpi_parent=oos_fk,
                                                                 type=availability_type,
                                                                 type_fk=denominator_fk),
                    identifier_parent=self.common.get_dictionary(kpi_parent=oos_parent_fk,
                                                                 type=Consts.SUB_CATEGORY_PARENT,
                                                                 type_fk=category_fk)))
            else:
                dict_list.append(self.build_dictionary_for_db_insert_v2(
                    fk=oos_fk,
                    numerator_id=self.manufacturer_fk,
                    numerator_result=oos_numerator,
                    result=oos_res,
                    score=oos_res,
                    denominator_result=denominator,
                    denominator_id=denominator_fk,
                    identifier_result=self.common.get_dictionary(kpi_parent=oos_fk,
                                                                 type=availability_type,
                                                                 type_fk=denominator_fk)))

            lvl2_result = self.assortment.calculate_lvl2_assortment(assort_lvl3)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                if not pd.isnull(result.target) and not pd.isnull(
                        result.group_target_date) and result.group_target_date <= self.current_date:
                    denominator_res = result.target
                res = np.divide(float(result.passes), float(denominator_res))  # * 100
                if res >= 100:
                    score = 100
                else:
                    score = 0
                res = 100 if res > 100 else res
                if availability_type is Consts.SUB_CATEGORY:
                    dist_parent_fk = self.common.get_kpi_fk_by_kpi_type(
                        Consts.DISTRIBUTION_PREFIX + Consts.AVAILABILITY_DICT[('DIST', Consts.SUB_CATEGORY_PARENT)][0])
                    dict_list.append(self.build_dictionary_for_db_insert_v2(
                        fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=result.passes,
                        result=res, score=score, denominator_id=denominator_fk,
                        denominator_result=result.total, identifier_result=
                        self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type,
                                                   type_fk=denominator_fk),
                        identifier_parent=
                        self.common.get_dictionary(kpi_parent=dist_parent_fk, type=Consts.SUB_CATEGORY_PARENT,
                                                   type_fk= category_fk)))
                else:
                    dict_list.append(self.build_dictionary_for_db_insert_v2(
                        fk=dist_fk, numerator_id=self.manufacturer_fk, numerator_result=result.passes,
                        result=res, score=score, denominator_id=denominator_fk,
                        denominator_result=result.total, identifier_result=
                        self.common.get_dictionary(kpi_parent=dist_fk, type=availability_type, type_fk=denominator_fk)))

        return dict_list

    def extract_data_set_up_file(self, kpi_type, set_up_data=None, kpi_dict=None):
        """
        The function receives kpi type : "FSOS", "LSOS", "availability"
        Function translate set up  file template to a dictionary that defines many parameters
        """
        if set_up_data is None:
            set_up_data = self.set_up_data
        if kpi_dict is None:
            kpi_dict = Consts.KPI_DICT
        if set_up_data[(kpi_dict[kpi_type], Consts.KPI_TYPE_COLUMN)] == Consts.INFO:
            return
        else:
            set_up_data[(kpi_dict[kpi_type], Consts.KPI_TYPE_COLUMN)] = Consts.INFO

        # filtering the template info to match the kpi
        set_up_info = self.set_up_file[self.set_up_file[Consts.KPI_TYPE_COLUMN] == kpi_dict[kpi_type]]

        for col in Consts.SET_UP_COLUMNS_MULTIPLE_VALUES:
            if set_up_info[col].empty or set_up_info[col].iloc[0].encode(HelperConsts.UTF8).strip() == '':
                set_up_data[(col, kpi_type)] = []
            else:
                set_up_data[(col, kpi_type)] = [x.strip() for x in set_up_info[col].iloc[0].encode(HelperConsts.UTF8).split(",")]
        for col in Consts.SET_UP_COLUMNS_BOOLEAN_VALUES:
            if set_up_info[col].empty or set_up_info[col].iloc[0].encode(HelperConsts.UTF8).strip() == '':
                set_up_data[(col, kpi_type)] = True
            else:
                set_up_data[(col, kpi_type)] = True if set_up_info[col].iloc[0].encode(HelperConsts.UTF8).strip().lower() == \
                                                       Consts.INCLUDE else False

    def availability_calculation(self, availability_type):
        """
            :param availability_type : "Store" , "Category", "SubCategory"
            Function initialize assortment_level_3 (class attribute) if not initialized before
            and calculate availability results according to availability type
        """

        self.extract_data_set_up_file("Availability")

        if self.assort_lvl3 is None:
            self.assortment_lvl3_adjustments1()
            if self.assort_lvl3 is None or self.assort_lvl3.empty:
                return
        dict_list = []

        if availability_type == Consts.STORE:
            return self.assortment_calculation(self.assort_lvl3, self.store_fk, Consts.STORE)

        if availability_type == Consts.CATEGORY:
            categories = self.all_products['category_fk'].dropna().unique()
            for cat in categories:
                assort_lvl3 = self.assort_lvl3[self.assort_lvl3['category_fk'] == cat]
                if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
                    self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
                dict_list.extend(self.assortment_calculation(assort_lvl3, cat, Consts.CATEGORY))

        # if availability_type == Consts.SUB_CATEGORY:
        #     categories = self.assort_lvl3['category_fk'].dropna().unique()
        #     for cat in categories:
        #         assort_lvl3 = self.assort_lvl3[self.assort_lvl3['category_fk'] == cat]
        #         if assort_lvl3.empty:
        #             continue
        #         if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
        #             self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
        #         dict_list.extend(self.assortment_calculation(assort_lvl3, cat, Consts.SUB_CATEGORY_PARENT))
        #         sub_categories = assort_lvl3['sub_category_fk'].dropna().unique()
        #         for sub_cat in sub_categories:
        #             assort_lvl3_sub_cat = assort_lvl3[assort_lvl3['sub_category_fk'] == sub_cat]
        #             if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
        #                  self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
        #             dict_list.extend(self.assortment_calculation(assort_lvl3_sub_cat, sub_cat, Consts.SUB_CATEGORY))

        return dict_list

    def get_assortment_provider(self):
        return self.assortment

    def result_value_pk(self, result):
            """
            converts string result to its pk (in static.kpi_result_value)
            :param result: str
            :return: int
            """
            pk = self.result_values[self.result_values['value'] == result][BasicConsts.PK].iloc[0]
            return pk

    def build_dictionary_for_db_insert_v2(self, fk=None, kpi_name=None, numerator_id=0, numerator_result=0, result=0,
                                          denominator_id=0, denominator_result=0, score=0,
                                          denominator_result_after_actions=None, context_id=None,
                                          target=None, identifier_parent=None, identifier_result=None):
        try:
            insert_params = dict()
            if not fk:
                if not kpi_name:
                    return
                else:
                    insert_params['fk'] = self.common.get_kpi_fk_by_kpi_name_new_tables(kpi_name)
            else:
                insert_params['fk'] = fk
            insert_params[SessionResultsConsts.NUMERATOR_ID] = numerator_id
            insert_params[SessionResultsConsts.NUMERATOR_RESULT] = numerator_result
            insert_params[SessionResultsConsts.DENOMINATOR_ID] = denominator_id
            insert_params[SessionResultsConsts.DENOMINATOR_RESULT] = denominator_result
            insert_params[SessionResultsConsts.RESULT] = result
            insert_params[SessionResultsConsts.SCORE] = score
            if target:
                insert_params[SessionResultsConsts.RESULT] = target
            if denominator_result_after_actions:
                insert_params[SessionResultsConsts.DENOMINATOR_RESULT_AFTER_ACTIONS] = denominator_result_after_actions
            if context_id:
                insert_params[SessionResultsConsts.CONTEXT_ID] = context_id
            if identifier_parent:
                insert_params['identifier_parent'] = identifier_parent
                insert_params['should_enter'] = True
            if identifier_result:
                insert_params['identifier_result'] = identifier_result
            return insert_params
        except IndexError:
            Log.error('error in build_dictionary_for_db_insert')
            return None

    def availability_store_function(self):

        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.availability_calculation(Consts.STORE)
        except Exception as e:
            Log.error('{}'.format(e))

    def availability_category_function(self):

        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.availability_calculation(Consts.CATEGORY)
        except Exception as e:
            Log.error('{}'.format(e))

    def availability_subcategory_function(self):
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.availability_calculation(Consts.SUB_CATEGORY)
        except Exception as e:
            Log.error('{}'.format(e))

    @staticmethod
    def get_store_area_query():
        return """
        SELECT
        template_fk,
        temp.name template_name,
        store_task_area_group_item_fk store_area_group_item_fk,
        sttagi.name store_area_group_item_name,
        count(1) store_area_item_count
        FROM 
        probedata.scene sce, 
        probedata.scene_store_task_area_group_items scetagi,
        static.template temp,
        static.store_task_area_group_items sttagi 
        WHERE 1=1 
        AND sce.status = 6
        AND sce.delete_time is null
        AND sce.session_uid ='{}'
        AND sce.pk = scetagi.scene_fk
        AND sce.template_fk = temp.pk 
        AND scetagi.store_task_area_group_item_fk = sttagi.pk
        AND temp.name in ({})
        GROUP BY 
        template_fk,
        temp.name,
        store_task_area_group_item_fk,
        sttagi.name"""

    def get_store_area_data(self, scene_types):
        query = self.get_store_area_query().format(self.session_uid, scene_types)
        store_area_data = pd.read_sql_query(query, self.rds_conn.db)
        return store_area_data

    @staticmethod
    def get_store_area_score_query():
        return """
           SELECT
           (select pk from static.template where name in ({2})) template_fk,
           count(distinct(sttagi.name))  count,
           {0} target,
           count(distinct(sttagi.name)) / {0} result
           FROM 
           probedata.scene sce, 
           probedata.scene_store_task_area_group_items scetagi,
           static.template temp,
           static.store_task_area_group_items sttagi 
           WHERE 1=1 
           AND sce.status = 6
           AND sce.delete_time is null
           AND sce.session_uid ='{1}'
           AND sce.pk = scetagi.scene_fk
           AND sce.template_fk = temp.pk 
           AND scetagi.store_task_area_group_item_fk = sttagi.pk
           AND temp.name in ({2})
           AND sttagi.name in ({3})"""

    def get_store_area_score_data(self, scene_types, store_location, target):
        query = self.get_store_area_score_query().format(target, self.session_uid, scene_types, store_location)
        store_area_score_data = pd.read_sql_query(query, self.rds_conn.db)
        return store_area_score_data

    def get_assortment_data_provider(self):
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.get_assortment_provider()
        except Exception as e:
            Log.error('{}'.format(e))

    def assortment_lvl3_adjustments1(self):

        """
        The function takes data frame of level 3 assortment and manipulate the information :
         remove substation products from assortment
         adding products in scif for the manufacture
        """
        lvl3_result, filtered_scif = self.get_assortment_filtered(self.set_up_data, 'Availability')
        if lvl3_result.empty:
            return
        lvl3_result = pd.merge(self.all_products[[ProductsConsts.PRODUCT_FK, ProductsConsts.PRODUCT_EAN_CODE,
                                                  ProductsConsts.SUBSTITUTION_PRODUCT_FK,
                                                  ProductsConsts.SUB_CATEGORY_FK, ProductsConsts.CATEGORY_FK]],
                               lvl3_result, how='left', on=AssortmentProductConsts.PRODUCT_FK)
        lvl3_result = lvl3_result[lvl3_result[ProductsConsts.SUBSTITUTION_PRODUCT_FK].isnull()]
        dist_kpi = self.common.get_kpi_fk_by_kpi_name(Consts.DISTRIBUTION)
        lvl3_result = lvl3_result[(lvl3_result['kpi_fk_lvl3'] == dist_kpi) | (lvl3_result['kpi_fk_lvl3'].isnull())]
        if filtered_scif is not None:
            scif = filtered_scif[(filtered_scif[ProductsConsts.MANUFACTURER_FK] == self.manufacturer_fk) &
                                 filtered_scif[ScifConsts.FACINGS] > 0]
            lvl3_result = pd.merge(lvl3_result, scif, how='left', on=AssortmentProductConsts.PRODUCT_FK)
            lvl3_result = lvl3_result[(~lvl3_result['kpi_fk_lvl3'].isnull()) | (~lvl3_result[BasicConsts.PK].isnull())]
            lvl3_result.drop_duplicates(AssortmentProductConsts.PRODUCT_FK,
                                        inplace=True)  # when in 2 different scenes I see the same product
            lvl3_result.rename(index=str, columns={"category_fk_x": ProductsConsts.CATEGORY_FK,
                                                   "sub_category_fk_x": ProductsConsts.SUB_CATEGORY_FK}, inplace=True)
        else:
            lvl3_result = lvl3_result[(~lvl3_result['kpi_fk_lvl3'].isnull())]
        self.assort_lvl3 = lvl3_result

    def get_assortment_filtered(self, filter_dict, kpi):
        filtered_scif = self.scif
        filtered_scif = self.tests_by_template(kpi, filtered_scif, filter_dict)
        self.assortment.scif = self.scif.drop(self.scif.index,
                                              inplace=False) if filtered_scif is None else filtered_scif
        # lvl3_result = self.assortment.calculate_lvl3_assortment(filter_dict[(Consts.INCLUDE_STACKING, kpi)])
        lvl3_result = self.assortment.calculate_lvl3_assortment()
        self.assortment.scif = self.scif
        return lvl3_result, filtered_scif

    def tests_by_template(self, kpi_type, df, set_up_data=None):

        """
        :param  kpi_type : "FSOS", "LSOS", "availability"
        :param       df : data frame that is merge of scif and other tables
        :param       set_up_data: dictionary of kpi type and parameters to filter the df by

        :returns   filtered df by template definition , if df null return None and print warning
        """
        if set_up_data is None:
            set_up_data = self.set_up_data
        df = df[df[ScifConsts.STATUS] != 2]  # 2 =  not a good pic of product
        if set_up_data[(Consts.CHANNEL, kpi_type)]:
            if self.data_provider.store_type not in (set_up_data[(Consts.CHANNEL, kpi_type)]):
                Log.warning('no calculation for this channel type {0}'.format(set_up_data[(Consts.CHANNEL, kpi_type)]))
                return
        if set_up_data[(Consts.SCENE_TYPE, kpi_type)]:  # template_name
            df = df[df[ScifConsts.TEMPLATE_NAME].isin(set_up_data[(Consts.SCENE_TYPE, kpi_type)])]
            if df.empty:
                Log.warning('There is no relevant data for this template name {0}'.format(set_up_data[(
                    Consts.SCENE_TYPE, kpi_type)]))
                return

        if set_up_data[(Consts.CATEGORY_INCLUDE, kpi_type)]:  # category_name
            df = df[df[ProductsConsts.CATEGORY].isin(set_up_data[(Consts.CATEGORY_INCLUDE, kpi_type)])]
            if df.empty:
                Log.warning('There is no relevant data for categories: {0}'.format(set_up_data[(
                    Consts.SCENE_TYPE, kpi_type)]))
                return

        if set_up_data[(Consts.SUB_CATEGORY_INCLUDE, kpi_type)]:  # sub_category_name
            df = df[df[ProductsConsts.SUB_CATEGORY].isin(set_up_data[(Consts.SUB_CATEGORY_INCLUDE, kpi_type)])]
            if df.empty:
                Log.warning('There is no relevant data for sub-categoris: {0}'.format(set_up_data[(
                    Consts.SUB_CATEGORY_INCLUDE, kpi_type)]))
                return

        if not set_up_data[(Consts.INCLUDE_STACKING, kpi_type)]:
            df = df[df[MatchesConsts.STACKING_LAYER] == 1]
            if df.empty:
                Log.warning('There is no relevant data - all products are stacking')
                return

        if not set_up_data[(Consts.INCLUDE_OTHERS, kpi_type)]:
            df = df[df[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.OTHER]
            if df.empty:
                Log.warning('There is no relevant data - all products from type OTHER')
                return
        if not set_up_data[(Consts.INCLUDE_IRRELEVANT, kpi_type)]:
            df = df[df[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.IRRELEVANT]
            if df.empty:
                Log.warning('There is no relevant data - all products from type Irrelevant')
                return
        if not set_up_data[(Consts.INCLUDE_EMPTY, kpi_type)]:
            df = df[df[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.EMPTY]
            if df.empty:
                Log.warning('There is no relevant data - all products from type Empty')
                return

        if not set_up_data[(Consts.INCLUDE_POSM, kpi_type)]:
            df = df[df[ProductsConsts.PRODUCT_TYPE] != ProductTypeConsts.POS]
            if df.empty:
                Log.warning('There is no relevant data - all products from type POSM')
                return

        if set_up_data[(Consts.BRANDS_INCLUDE, kpi_type)]:
            df = df[df[ProductsConsts.BRAND_NAME].isin(set_up_data[(Consts.BRANDS_INCLUDE, kpi_type)])]
            if df.empty:
                Log.warning('There is no relevant data for this brands {0}'.format(set_up_data[(Consts.BRANDS_INCLUDE, kpi_type)]))
                return
        if set_up_data[(Consts.SKUS_EXCLUDE, kpi_type)]:
            df = df[~df[ProductsConsts.PRODUCT_EAN_CODE].isin(set_up_data[(Consts.SKUS_EXCLUDE, kpi_type)])]
            if df.empty:
                Log.warning('There is no relevant data for this products {0}'.format(set_up_data[(Consts.SKUS_EXCLUDE, kpi_type)]))
                return

        return df

    def get_last_status(self, kpi_pk, numerator, denominator=None):
        """
              :param kpi_pk
              :param numerator
              :param denominator

              :return result ,from last_results df( data frame that contains results from last completed session from
              the same store , that has the same  kpi_level_2_fk = kpi_pk and numerator_id=numerator and
              denominator_id=denominator
        """
        if denominator is None:
            results = self.last_results[(self.last_results[SessionResultsConsts.KPI_LEVEL_2_FK] == kpi_pk) &
                                        (self.last_results[SessionResultsConsts.NUMERATOR_ID] == numerator)]
        else:
            results = self.last_results[(self.last_results[SessionResultsConsts.KPI_LEVEL_2_FK] == kpi_pk) & (
                    self.last_results[SessionResultsConsts.NUMERATOR_ID] == numerator) &
                                        (self.last_results[SessionResultsConsts.DENOMINATOR_ID] == denominator)]
        if results.empty:
            return None
        return results[SessionResultsConsts.RESULT].iloc[0]

    def facings_sos_whole_store_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.main_sos_calculation(Consts.STORE_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def facings_sos_by_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.main_sos_calculation(Consts.CATEGORY_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def facings_by_sub_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.main_sos_calculation(Consts.SUB_CATEGORY_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def main_sos_calculation(self, kpi_type, sos_type):
        """
         The function
         :param kpi_type: "In_Whole_Store" /"By_Category"/"By_SubCategory"
         :param sos_type : "FSOS"/"LSOS"
         :returns results_df
        """

        if self.manufacturer_fk is None:
            Log.warning('Own manufacturer fk is empty')
            return
        results_df = []
        sos_policy = Consts.SOS_FACINGS if sos_type == Consts.SOS_FACINGS else MatchesConsts.WIDTH_MM_ADVANCE
        df = pd.merge(self.match_product_in_scene,
                      self.all_products[Consts.PRODUCTS_COLUMNS], how='left', on=[ProductsConsts.PRODUCT_FK])
        df = pd.merge(self.scif[Consts.SCIF_COLUMNS],
                      df, how='right', right_on=[ScifConsts.SCENE_FK, ScifConsts.PRODUCT_FK],
                      left_on=[ScifConsts.SCENE_ID, ScifConsts.PRODUCT_FK])

        if df.empty:
            Log.warning('match_product_in_scene is empty ')
            return
        self.extract_data_set_up_file(sos_type)
        df = self.tests_by_template(sos_type, df)
        if df is None or df.empty:
            return

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(sos_type + Consts.OWN_MANUFACTURER + kpi_type)  # adding type

        if kpi_type == Consts.STORE_KPI_SUFFIX:
            identifier_level_1 = self.common.get_dictionary(manufacturer_fk=self.manufacturer_fk, sos_type=sos_type)
            filters_num = {ProductsConsts.MANUFACTURER_FK: [self.manufacturer_fk]}
            results = self.calculate_sos(df, filters_num, {}, sos_policy)
            results_df.append(self.create_db_result(kpi_fk, self.manufacturer_fk, self.store_fk, results,
                                                    identifier_result=identifier_level_1))
            results_df.extend(self.sos_manufacturer_level(df, identifier_level_1, {}, sos_policy,
                                                          self.manufacturer_fk,
                                                          sos_type, kpi_type))
        else:
            results_df.extend(self.sos_category(df, kpi_fk, kpi_type, sos_type, sos_policy))
        return results_df

    def calculate_sos(self, df, filters_num, filters_den, sos_policy):
        """
              The function
              :param df: contains  products + product_width +product facings
              :param filters_num : dictionary of {parameter_name : parameter value} for choosing which products to sum
              in numerator
              :param filters_den : dictionary of {parameter_name : parameter value} for choosing which products to sum
              in denominator
              :param sos_policy : string with values:'GSK_FSOS',MatchesConsts.WIDTH_MM_ADVANCE

              :returns array of 3 parameters [numerator_results-int , denominator_result -int , results = float ,
              front_facings = how many front facings found in numerator]

        """
        numerator_result = df[self.tools.get_filter_condition(df, **filters_num)]
        denominator_result = df[self.tools.get_filter_condition(df, **filters_den)]
        front_facing = float(numerator_result[numerator_result[MatchesConsts.FRONT_FACING] == 'Y']
                             [MatchesConsts.FRONT_FACING].count()) / float(100)
        if sos_policy == MatchesConsts.WIDTH_MM_ADVANCE:
            numerator_result = numerator_result[sos_policy].sum()
            denominator_result = denominator_result[sos_policy].sum()
        else:
            numerator_result = numerator_result.shape[0]
            denominator_result = denominator_result.shape[0]

        result = round(float(numerator_result) / float(denominator_result), 4) \
            if denominator_result != 0 and denominator_result != 0 else 0

        return [numerator_result, denominator_result, result, front_facing]

    def sos_category(self, df, kpi_fk, kpi_type, sos_type, sos_policy):
        """
        The function calculate sos result in category level and sub_category level
        (since sub_category is under category in drill down).
              :param df: filtered  df by template
              :param kpi_fk: kpi fk in static.kpi_level_2
              :param kpi_type: "In_Whole_Store" /"By_Category"/"By_SubCategory"
              :param sos_type : "GSK_FSOS"/"GSK_LSOS"
              :param sos_policy : "GSK_FSOS" / MatchesConsts.WIDTH_MM_ADVANCE
              :returns results_df
        """
        results_df = []
        categories = df[ProductsConsts.CATEGORY_FK].dropna().unique()
        for category in categories:
            filters_num = {ProductsConsts.MANUFACTURER_FK: [self.manufacturer_fk], ProductsConsts.CATEGORY_FK: [category]}
            filters_den = {ProductsConsts.CATEGORY_FK: [category]}
            results = self.calculate_sos(df, filters_num, filters_den, sos_policy)

            if kpi_type == Consts.CATEGORY_KPI_SUFFIX:
                identifier_level_1 = self.common.get_dictionary(category_fk=[category], sos_type=sos_type)

                results_df.append(self.create_db_result(kpi_fk, self.manufacturer_fk, category, results,
                                                        identifier_result=identifier_level_1))
                cat_df = df[self.tools.get_filter_condition(df, **filters_den)]
                results_df.extend(self.sos_manufacturer_level(cat_df, identifier_level_1, filters_den,
                                                              sos_policy, category, sos_type, kpi_type))
            else:
                identifier_level_1 = self.common.get_dictionary(category_fk=[category],
                                                                sos_type=sos_type + Consts.SUFFIX_SUB_CAT)
                kpi_category_fk = self.common.get_kpi_fk_by_kpi_type(
                    sos_type + Consts.OWN_MANUFACTURER + kpi_type + Consts.SUFFIX_SUB_CAT)

                results_df.append(self.create_db_result(kpi_category_fk, self.manufacturer_fk, category, results
                                                        , identifier_result=identifier_level_1))

                sub_categories = df[df[ProductsConsts.CATEGORY_FK] == category][ProductsConsts.SUB_CATEGORY_FK].dropna().unique()
                for sub_cat in sub_categories:
                    filters_num = {ProductsConsts.MANUFACTURER_FK: [self.manufacturer_fk], ProductsConsts.SUB_CATEGORY_FK: [sub_cat],
                                   ProductsConsts.CATEGORY_FK: [category]}
                    filters_den = {ProductsConsts.SUB_CATEGORY_FK: [sub_cat], ProductsConsts.CATEGORY_FK: [category]}
                    identifier_level_2 = self.common.get_dictionary(sub_category_fk=[sub_cat],
                                                                    sos_type=sos_type + Consts.SUFFIX_SUB_CAT,
                                                                    category_fk=[category])
                    results = self.calculate_sos(df, filters_num, filters_den, sos_policy)

                    results_df.append(
                        self.create_db_result(kpi_fk, self.manufacturer_fk, sub_cat, results,
                                              identifier_parent=identifier_level_1, identifier_result=
                                              identifier_level_2))
                    sub_cat_df = df[self.tools.get_filter_condition(df, **filters_den)]
                    results_df.extend(self.sos_manufacturer_level(sub_cat_df, identifier_level_2, filters_den,
                                                                  sos_policy, sub_cat, sos_type, kpi_type))
        return results_df

    @staticmethod
    def get_kpi_score(lst_results, kpi_fk, weight):
        result = 0
        for dict_result in lst_results:
            if dict_result['fk'] == kpi_fk:
                dict_result["weight"] = weight
                dict_result["score"] = float(dict_result['result']) * weight
                return dict_result['score']
        return result

    def calculate_red_score(self, facings_sos_whole_store_dict, point_of_store_dict, assortment_store_dict, kpi_data):
        dict_list = []

        sovi_kpi_fk = self.common.get_kpi_fk_by_kpi_type("CCJP_FSOS_Own_Manufacturer_In_Whole_Store")
        dist_kpi_fk = self.common.get_kpi_fk_by_kpi_type("CCJP_Dst_Manufacturer_in_Whole_Store")
        poc_kpi_fk = self.common.get_kpi_fk_by_kpi_type("CCJP_POC_SCORE_BY_TARGET")

        weights = ast.literal_eval(str(kpi_data['additional_kpi_attributes']))

        sovi_weightage = weights['sovi_weightage']
        poc_weightage = weights['poc_weightage']
        dist_weightage = weights['dist_weightage']

        kpi_name = kpi_data[Consts.KPI_TYPE_COLUMN]

        sovi_score = self.get_kpi_score(facings_sos_whole_store_dict, sovi_kpi_fk, sovi_weightage)
        dist_score = self.get_kpi_score(assortment_store_dict, dist_kpi_fk, dist_weightage)
        poc_score = self.get_kpi_score(point_of_store_dict, poc_kpi_fk, poc_weightage)

        red_score_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        red_score = sovi_score + dist_score + poc_score
        dict_list.append(self.build_dictionary_for_db_insert_v2(fk=red_score_kpi_fk,
                                                                numerator_id=self.manufacturer_fk,
                                                                numerator_result=red_score,
                                                                result=red_score,
                                                                denominator_id=self.store_id,
                                                                denominator_result=0,
                                                                score=red_score))

        return dict_list

    def sos_manufacturer_level(self, df, identifier_parent, parent_filter, sos_policy,
                               kpi_denominator, sos_type, kpi_type):

        """
        The function calculate sos result in manufacturer level by store/category/sub_category depended on
        filter_own_manufacturer_level(filter df by the relevant filters )
              :param df: filtered  df by template
              :param identifier_parent: dictionary -identifier set in upper level
              :param parent_filter: dictionary - relevant filters of level above in calculation
              :param kpi_type: "In_Whole_Store" /"By_Category"/"By_SubCategory"
              :param kpi_denominator : category/sub category / store   pks . depended on kpi_type
              :param sos_type : "GSK_FSOS"/"GSK_LSOS"
              :param sos_policy : "GSK_FSOS" / MatchesConsts.WIDTH_MM_ADVANCE
              :returns results_df
        """

        results_df = []
        kpi_all_manu_pk = self.common.get_kpi_fk_by_kpi_type(sos_type + Consts.ALL_MANUFACTURER + kpi_type)
        all_manufacturers = df[ProductsConsts.MANUFACTURER_FK].dropna().unique()
        for manufacturer in all_manufacturers:
            identifier_result = self.common.get_dictionary(manufacturer_fk=[manufacturer],
                                                           identifier_level_1=identifier_parent)
            filters_all_manufacturer = dict(
                (k, v) for d in [parent_filter,  {ProductsConsts.MANUFACTURER_FK: [manufacturer]}]
                for k, v in d.items())

            results = self.calculate_sos(df, filters_all_manufacturer, parent_filter, sos_policy)
            results_df.append(self.create_db_result(kpi_all_manu_pk, manufacturer, kpi_denominator, results,
                                                    identifier_parent, identifier_result))
            brands = df[df[ProductsConsts.MANUFACTURER_FK] == manufacturer][ProductsConsts.BRAND_FK].dropna().unique()
            for brand in brands:
                results_df.extend(self.sos_brand_level_by_manufacturer(df, brand, manufacturer,
                                                                       identifier_result, kpi_type,
                                                                       sos_type, sos_policy, filters_all_manufacturer,
                                                                       parent_filter, kpi_denominator
                                                                       ))

        return results_df

    def sos_brand_level_by_manufacturer(self, df, brand, manufacturer, identifier_parent, kpi_type,
                                        sos_type, sos_policy, filters_sos_level_above, filter_sos_first_level,
                                        kpi_denominator):
        """
               The function calculate sos result in brand level by store/category/sub_category depended on
               kpi type (filter df by the relevant filters )
                     :param df: filtered  df by template
                     :param identifier_parent: dictionary -identifier set in upper level
                     :param brand pk
                     :param manufacturer pk
                     :param kpi_type: "In_Whole_Store" /"By_Category"/"By_SubCategory"
                     :param filters_sos_level_above : dictionary of filters of level above  in calculation
                      :param filter_sos_first_level : dictionary of filters of first level in calculation
                     :param kpi_denominator : category/sub category / store   pks . depended on kpi_type
                     :param sos_type : "GSK_FSOS"/"GSK_LSOS"
                     :param sos_policy : "GSK_FSOS" / MatchesConsts.WIDTH_MM_ADVANCE
                     :returns results_df of sos by brand level and by product level
        """
        results_df = []
        kpi_brand_pk = self.common.get_kpi_fk_by_kpi_type(sos_type + Consts.ALL_BRAND_KPI + kpi_type)
        identifier_level_3 = self.common.get_dictionary(brand_fk=[brand], manufacturer_fk=[manufacturer],
                                                        identifier_level_1=
                                                        identifier_parent)
        filters_num = {ProductsConsts.BRAND_FK: [brand]}
        filters_brand = dict((k, v) for d in [filters_sos_level_above, filters_num] for k, v in d.items())
        results = self.calculate_sos(df, filters_brand, filter_sos_first_level, sos_policy)
        results_df.append(self.create_db_result(kpi_brand_pk, brand, kpi_denominator, results, identifier_parent,
                                                identifier_level_3))
        products = df[(df[ProductsConsts.MANUFACTURER_FK] == manufacturer) &
                      (df[ProductsConsts.BRAND_FK] == brand)][ProductsConsts.PRODUCT_FK].unique()
        for prod in products:
            results_df.append(self.sos_product_level_by_brand_and_manufacturer(df, prod, kpi_type, sos_type,
                                                                               sos_policy, filters_brand,
                                                                               filter_sos_first_level,
                                                                               kpi_denominator, identifier_level_3))
        return results_df

    def sos_product_level_by_brand_and_manufacturer(self, df, product_fk, kpi_type, sos_type, sos_policy,
                                                    filters_sos_level_above, filter_sos_first_level,
                                                    kpi_denominator, identifier_parent):
        """
               The function calculate sos result in brand level by store/category/sub_category depended on
               kpi type (filter df by the relevant filters )
                     :param df: filtered  df by template
                     :param identifier_parent: dictionary -identifier set in upper level
                     :param product_fk pk
                     :param kpi_type: "In_Whole_Store" /"By_Category"/"By_SubCategory"
                     :param filters_sos_level_above : dictionary of filters of level above  in calculation
                      :param filter_sos_first_level : dictionary of filters of first level in calculation
                     :param kpi_denominator : category/sub category / store   pks . depended on kpi_type
                     :param sos_type : "GSK_FSOS"/"GSK_LSOS"
                     :param sos_policy : "GSK_FSOS" / MatchesConsts.WIDTH_MM_ADVANCE
                     :returns results_df of sos by brand level and by product level
        """
        kpi_product_pk = self.common.get_kpi_fk_by_kpi_type(sos_type + Consts.ALL_PRODUCT_KPI + kpi_type)
        filters_num = {ScifConsts.PRODUCT_FK: [product_fk]}
        filters_num = dict((k, v) for d in [filters_sos_level_above, filters_num] for k, v in d.items())
        results = self.calculate_sos(df, filters_num, filter_sos_first_level, sos_policy)
        return self.create_db_result(kpi_product_pk, product_fk, kpi_denominator, results, identifier_parent)

    def calculate_unique_sku_sos(self, kpi_data):
        dict_list = []

        if kpi_data.empty:
            Log.warning('CCJP_UNIQUE_DIST_OWN_MANU not found in template/db')

        kpi_unique_dist_fk = self.common.get_kpi_fk_by_kpi_type(kpi_data[Consts.KPI_TYPE_COLUMN])

        scene_types = [x.strip() for x in kpi_data[Consts.SCENE_TYPE].split(",")]
        df_unique_sku_all = self.scif[self.scif['template_name'].isin(scene_types)]

        if df_unique_sku_all.empty:
            Log.warning("{} scene_types not found in this session".format(scene_types))
            return

        if str(kpi_data[Consts.INCLUDE_STACKING]).lower() == Consts.INCLUDE:
            df_unique_sku_all = df_unique_sku_all[(self.scif['facings'] > 0)]
        else:
            df_unique_sku_all = df_unique_sku_all[(self.scif['facings_ign_stack'] > 0)]

        if str(kpi_data[Consts.INCLUDE_EMPTY]).lower() != Consts.INCLUDE:
            df_unique_sku_all = df_unique_sku_all[(df_unique_sku_all['product_type'] != Consts.EMPTY)]

        if str(kpi_data[Consts.INCLUDE_IRRELEVANT]).lower() != Consts.INCLUDE:
            df_unique_sku_all = df_unique_sku_all[df_unique_sku_all['product_type'] != Consts.IRRELEVANT]

        if str(kpi_data[Consts.INCLUDE_OTHERS]).lower() != Consts.INCLUDE:
            df_unique_sku_all = df_unique_sku_all[df_unique_sku_all['product_type'] != Consts.OTHER]

        if str(kpi_data[Consts.INCLUDE_POSM]).lower() != Consts.INCLUDE:
            df_unique_sku_all = df_unique_sku_all[df_unique_sku_all['product_type'] != Consts.POSM]

        df_unique_sku_own = df_unique_sku_all[df_unique_sku_all['manufacturer_fk'] == self.manufacturer_fk]

        numerator_id = self.manufacturer_fk
        numerator_result = len(df_unique_sku_own.index)

        denominator_id = self.store_id
        denominator_result = len(df_unique_sku_all.index)

        try:
            result = numerator_result / float(denominator_result)
        except:
            result = 0

        dict_list.append(self.build_dictionary_for_db_insert_v2(fk=kpi_unique_dist_fk,
                                                                numerator_id=numerator_id,
                                                                numerator_result=numerator_result,
                                                                result=result,
                                                                denominator_id=denominator_id,
                                                                denominator_result=denominator_result,
                                                                score=0))
        return dict_list

    def point_of_connection(self):

        if self.store_info.empty or self.poc_template.empty:
            return

        policy = self.get_poc_store_policy()

        if policy.empty:
            return

        store_locations = [x.strip() for x in policy['store_area_location'].split(',')]
        target = int(policy['target'])
        store_locations = ", ".join("'" + str(x) + "'" for x in store_locations)

        scene_types_count = [x.strip() for x in policy['scene_type_count'].split(',')]
        scene_types_count = ", ".join("'" + str(x) + "'" for x in scene_types_count)

        scene_types_score = [x.strip() for x in policy['scene_type_score'].split(',')]
        scene_types_score = ", ".join("'" + str(x) + "'" for x in scene_types_score)

        return self.point_of_connection_calc(scene_types_count, scene_types_score, store_locations, target)

    def point_of_connection_calc(self, scene_types_count, scene_types_score, store_locations, target):
        dict_list = []
        kpi_poc_count_fk = self.common.get_kpi_fk_by_kpi_type('CCJP_POC_COUNT_BY_STORE_AREA')
        store_area_data = self.get_store_area_data(scene_types_count)

        kpi_poc_score_fk = self.common.get_kpi_fk_by_kpi_type('CCJP_POC_SCORE_BY_TARGET')
        store_area_score_data = self.get_store_area_score_data(scene_types_score, store_locations, target)

        for row_num, row_data in store_area_data.iterrows():
            dict_list.append(self.build_dictionary_for_db_insert_v2(fk=kpi_poc_count_fk,
                                                                    numerator_id=row_data['store_area_group_item_fk'],
                                                                    numerator_result=row_data['store_area_item_count'],
                                                                    result=row_data['store_area_item_count'],
                                                                    denominator_id=row_data['template_fk'],
                                                                    denominator_result=0,
                                                                    score=row_data['store_area_item_count']))

        for row_num, row_data in store_area_score_data.iterrows():
            dict_list.append(self.build_dictionary_for_db_insert_v2(fk=kpi_poc_score_fk,
                                                                    numerator_id=row_data['template_fk'],
                                                                    numerator_result=row_data['count'],
                                                                    result=row_data['result'],
                                                                    denominator_id=self.store_id,
                                                                    denominator_result=row_data['target'],
                                                                    score=row_data['result']))

        return dict_list

    @staticmethod
    def create_db_result(kpi_fk, numerator_id, denominator_id, results, identifier_parent=None, identifier_result=None):
        """
        The function build db result for sos result
        :param kpi_fk: pk of kpi
        :param numerator_id
        :param denominator_id
        :param results: array of 3 parameters [int , int ,float]
        :param identifier_parent : dictionary of filters of level above  in calculation
        :param identifier_result : dictionary of filters of first level in calculation
        :returns dict in format of db result
        """
        return {'fk': kpi_fk,
                SessionResultsConsts.NUMERATOR_ID: numerator_id,
                SessionResultsConsts.DENOMINATOR_ID: denominator_id,
                SessionResultsConsts.DENOMINATOR_RESULT: results[1],
                SessionResultsConsts.NUMERATOR_RESULT: results[0],
                SessionResultsConsts.RESULT: results[2],
                SessionResultsConsts.SCORE: results[3],
                'identifier_parent': identifier_parent,
                'identifier_result': identifier_result,
                'should_enter': True}

    def get_poc_store_policy(self):
        store_policy = pd.DataFrame()
        for row_num, row_data in self.poc_template.iterrows():
            filter_params = {}
            for idx in range(1, 7):
                try:
                    if len(str(row_data["store_attr_" + str(idx) + "_name"]).strip()) != 0:
                        filter_params[row_data["store_attr_" + str(idx) + "_name"]] = \
                            row_data["store_attr_" + str(idx) + "_value"]
                except Exception as ex:
                    Log.info("Error:{} filter_params:{}".format(ex, filter_params))

            result = self.store_info.loc[
                (self.store_info[list(filter_params)] == pd.Series(filter_params)).all(axis=1)]

            if result.empty:
                continue
            else:
                # As per design - One store can have only one POC policy
                Log.info("store_policy:{} filter_params:{}".format(row_data['store_policy'], filter_params))
                return row_data

        return store_policy

    def get_kpi_params(self):
        list_kpis = []

        for row_num, row_data in self.set_up_file.iterrows():
            filter_params = {}
            for idx in range(1, 7):
                try:
                    if len(str(row_data["store_attr_" + str(idx) + "_name"]).strip()) != 0:
                        filter_params[row_data["store_attr_" + str(idx) + "_name"]] = \
                            row_data["store_attr_" + str(idx) + "_value"]
                except Exception as ex:
                    Log.info("Error:{} filter_params:{}".format(ex, filter_params))

            kpi_params = self.store_info.loc[
                (self.store_info[list(filter_params)] == pd.Series(filter_params)).all(axis=1)]

            if kpi_params.empty:
                Log.info("No KPIs")
                continue
            else:
                dict_kpi = row_data.to_dict()
                list_kpis.append(dict_kpi)

        df = pd.DataFrame(list_kpis)

        return df
