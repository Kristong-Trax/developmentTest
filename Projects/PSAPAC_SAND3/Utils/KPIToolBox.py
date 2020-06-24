from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ProductsConsts, ScifConsts, StoreInfoConsts
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
# from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from Projects.PSAPAC_SAND3.Utils.KPILocalGenerator import GSKLocalGenerator as GSKGenerator
from KPIUtils_v2.Utils.Consts.GlobalConsts import ProductTypeConsts, HelperConsts
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.PSAPAC_SAND3.Data.LocalConsts import Consts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import Const
import pandas as pd
import os
import numpy as np
import math

__author__ = 'limorc'


class PSAPAC_SAND3ToolBox:
    # Gsk Japan kpis

    # DEFAULT_TARGET = {ProductsConsts.BRAND_FK: [-1], 'shelves': ["1,2,3"], 'block_target': [80], 'brand_target': [100], 'position_target': [80]}

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
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

        self.gsk_generator = GSKGenerator(
            self.data_provider, self.output, self.common, self.set_up_template)
        self.blocking_generator = Block(self.data_provider)
        self.assortment = self.gsk_generator.get_assortment_data_provider()
        self.store_info = self.data_provider['store_info']
        self.store_fk = self.data_provider[StoreInfoConsts.STORE_FK]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets(key_fields=Consts.KEY_FIELDS,
                                                                      data_fields=Consts.DATA_FIELDS)
        self.own_manufacturer = self.get_manufacturer
        self.set_up_data = {
            (Consts.PLN_BLOCK, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
            (Consts.POSITION_SCORE, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
            (Consts.ECAPS_FILTER_IDENT, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
            (Consts.PLN_MSL, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
            ("GSK_PLN_LSOS_SCORE",Const.KPI_TYPE_COLUMN): Const.NO_INFO,
            (Consts.POSM, Const.KPI_TYPE_COLUMN): Const.NO_INFO
        }

    @property
    def get_manufacturer(self):
        return int(self.data_provider.own_manufacturer[self.data_provider.own_manufacturer['param_name'] ==
                                                       'manufacturer_id']['param_value'].iloc[0])

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.Global functions and local functions
        """
        # global kpis

        assortment_store_dict = self.gsk_generator.availability_store_function()
        self.common.save_json_to_new_tables(assortment_store_dict)

        assortment_category_dict = self.gsk_generator.availability_category_function()
        self.common.save_json_to_new_tables(assortment_category_dict)

        assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function()
        self.common.save_json_to_new_tables(assortment_subcategory_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        # # local kpis
        for kpi in Consts.KPI_DICT.keys():
            self.gsk_generator.tool_box.extract_data_set_up_file(kpi, self.set_up_data, Consts.KPI_DICT)

        results_ecaps = self.gsk_ecaps_kpis()
        self.common.save_json_to_new_tables(results_ecaps)

        self.get_store_target()  # choosing the policy
        if self.targets.empty:
            Log.warning('There is no target policy matching this store')
        else:
            results_compliance = self.gsk_compliance()
            self.common.save_json_to_new_tables(results_compliance)

        # New POSM Kpi
        results_pos = self.gsk_pos_kpis()
        self.common.save_json_to_new_tables(results_pos)

        self.common.commit_results_data()
        return

    def position_shelf(self, brand_fk, policy, df):
        """
        :param  brand_fk :
        :param  policy : dictionary that contains {
                                                'shelves':"1 ,2 ,4 ,5" (or any other string of numbers separate by ','),
                                                'position_target': 80 (or any other percentage you want the score to
                                                reach)
                                                }
        :param  df: data frame that contains columns MatchesConsts.SHELF_NUMBER , "brand kf"

        :returns   tuple of (result,score,numerator,denominator)
                   result = number of products from brand_fk in shelves / number of products from brand_fk ,
                   score  = if result reach position target 100  else 0 ,
                   numerator = number of products from brand_fk in shelves
                   denominator = number of products from brand_fk
        """
        if (Consts.SHELVES not in policy.keys()) or policy[Consts.SHELVES].empty:
            Log.warning('This sessions have external targets but doesnt have value for shelves position')
            return 0, 0, 0, 0, 0
        if isinstance(policy[Consts.SHELVES].iloc[0], list):
            shelf_from_bottom = [int(shelf) for shelf in policy[Consts.SHELVES].iloc[0]]
        else:
            shelf_from_bottom = [int(shelf) for shelf in policy[Consts.SHELVES].iloc[0].split(",")]

        threshold = policy[Consts.POSITION_TARGET].iloc[0]
        brand_df = df[df[ProductsConsts.BRAND_FK] == brand_fk]
        shelf_df = brand_df[brand_df[MatchesConsts.SHELF_NUMBER].isin(shelf_from_bottom)]
        numerator = shelf_df.shape[0]
        denominator = brand_df.shape[0]
        result = float(numerator) / float(denominator)
        score = 1 if (result*100) >= threshold else 0
        return result, score, numerator, denominator, threshold

    def lsos_score(self, brand, policy):
        """
        :param brand : pk of brand
        :param policy :  dictionary of  { 'brand_target' : lsos number you want to reach}
        This function uses the lsos_in whole_store global calculation.
        it takes the result of the parameter 'brand' according to the policy set target and results.
        :return result,score,target
                result : result of this brand lsos
                score :  result / brand_target ,
                target  :  branf_target

        """
        df = pd.merge(self.match_product_in_scene,
                      self.all_products[Const.PRODUCTS_COLUMNS], how='left', on=[MatchesConsts.PRODUCT_FK])
        df = pd.merge(self.scif[Const.SCIF_COLUMNS],
                      df, how='right', right_on=[ScifConsts.SCENE_FK, ScifConsts.PRODUCT_FK],
                      left_on=[ScifConsts.SCENE_ID, ScifConsts.PRODUCT_FK])

        if df.empty:
            Log.warning('match_product_in_scene is empty ')
            return 0, 0, 0
        df = self.gsk_generator.tool_box.tests_by_template('GSK_PLN_LSOS_SCORE', df, self.set_up_data)
        if df is None:
            Log.warning('match_product_in_scene is empty ')
            return 0, 0, 0
        result = self.gsk_generator.tool_box.calculate_sos(df, {ProductsConsts.BRAND_FK: brand}, {}, Const.LINEAR)[0]
        target = policy['brand_target'].iloc[0]
        score = float(result) / float(target)
        return result, score, target

    def brand_blocking(self, brand, policy):
        """
                :param brand : pk of brand
                :param policy :  dictionary of  { 'block_target' : number you want to reach}
                :return result : 1 if there is a block answer set_up_data conditions else 0
        """
        templates = self.set_up_data[(Const.SCENE_TYPE, Consts.PLN_BLOCK)]
        template_name = {
            ScifConsts.TEMPLATE_NAME: templates} if templates else None  # figure out which template name should I use
        ignore_empty = False
        # taking from params from set up  info
        stacking_param = False if not self.set_up_data[(
            Const.INCLUDE_STACKING, Consts.PLN_BLOCK)] else True  # false
        population_parameters = {ProductsConsts.BRAND_FK: [brand], ProductsConsts.PRODUCT_TYPE: [ProductTypeConsts.SKU]}

        if self.set_up_data[(Const.INCLUDE_OTHERS, Consts.PLN_BLOCK)]:
            population_parameters[ProductsConsts.PRODUCT_TYPE].append(Const.OTHER)
        if self.set_up_data[(Const.INCLUDE_IRRELEVANT, Consts.PLN_BLOCK)]:
            population_parameters[ProductsConsts.PRODUCT_TYPE].append(Const.IRRELEVANT)
        if self.set_up_data[(Const.INCLUDE_EMPTY, Consts.PLN_BLOCK)]:

            population_parameters[ProductsConsts.PRODUCT_TYPE].append(Const.EMPTY)
        else:
            ignore_empty = True

        if self.set_up_data[(Const.CATEGORY_INCLUDE, Consts.PLN_BLOCK)]:  # category_name
            population_parameters[ProductsConsts.CATEGORY] = self.set_up_data[(Const.CATEGORY_INCLUDE,
                                                                               Consts.PLN_BLOCK)]

        if self.set_up_data[(Const.SUB_CATEGORY_INCLUDE, Consts.PLN_BLOCK)]:  # sub_category_name
            population_parameters[ProductsConsts.SUB_CATEGORY] = self.set_up_data[(Const.SUB_CATEGORY_INCLUDE,
                                                                                   Consts.PLN_BLOCK)]

        # from Data file
        target = float(policy['block_target'].iloc[0]) / float(100)
        result = self.blocking_generator.network_x_block_together(location=template_name,
                                                                  population=population_parameters,
                                                                  additional={'minimum_block_ratio': target,
                                                                              'calculate_all_scenes': True,
                                                                              'ignore_empty': ignore_empty,
                                                                              'include_stacking': stacking_param,
                                                                              'check_vertical_horizontal': True,
                                                                              'minimum_facing_for_block': 1
                                                                              })
        result.sort_values('facing_percentage', ascending=False, inplace=True)
        score = 0 if result[result['is_block']].empty else 1
        numerator = 0 if result.empty else result['block_facings'].iloc[0]
        denominator = 0 if result.empty else result['total_facings'].iloc[0]

        return score, target, numerator, denominator

    def msl_assortment(self, kpi_fk, kpi_name):
        """
                        :param kpi_fk : name of level 3 assortment kpi
                        :param kpi_name: GSK_PLN_MSL_SCORE assortment , or   GSK_ECAPS assortment
                        :return kpi_results : data frame of assortment products of the kpi, product's availability,
                        product details.
                        filtered by set up
                """
        lvl3_assort, filter_scif = self.gsk_generator.tool_box.get_assortment_filtered(
            self.set_up_data, kpi_name)
        if lvl3_assort is None or lvl3_assort.empty:
            return None
        kpi_assortment_fk = self.common.get_kpi_fk_by_kpi_type(kpi_fk)
        kpi_results = lvl3_assort[lvl3_assort['kpi_fk_lvl3']
                                  == kpi_assortment_fk]  # general assortment
        kpi_results = pd.merge(kpi_results, self.all_products[Const.PRODUCTS_COLUMNS],
                               how='left', on=ProductsConsts.PRODUCT_FK)

        kpi_results = kpi_results[kpi_results[ProductsConsts.SUBSTITUTION_PRODUCT_FK].isnull()]
        return kpi_results

    def pln_ecaps_score(self, brand, assortment):
        """
                             :param brand : pk of desired brand
                             :param assortment : data frame of assortment products of the kpi, product's availability,
                                    product details. filtered by set up

                             besides result of lvl2_assortment function writing level 3 assortment product presence
                             results

                             :return  numerator : how many products available out of the granular groups
                                      denominator : how many products in assortment groups
                                      result :  (numerator/denominator)*100
                                      results :  array of dictionary, each dict contains the result details
        """
        identifier_parent = self.common.get_dictionary(brand_fk=brand,
                                                       kpi_fk=self.common.get_kpi_fk_by_kpi_type(Consts.ECAP_ALL_BRAND))
        results = []
        kpi_ecaps_product = self.common.get_kpi_fk_by_kpi_type(Consts.PRODUCT_PRESENCE)
        ecaps_assortment_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_ASSORTMENT_KPI)
        if assortment.empty:
            return 0, 0, 0, results
        brand_results = assortment[assortment[ProductsConsts.BRAND_FK]
                                   == brand]  # only assortment of desired brand
        for result in brand_results.itertuples():
            if (math.isnan(result.in_store)) | (result.kpi_fk_lvl3 != ecaps_assortment_fk):
                score = self.gsk_generator.tool_box.result_value_pk(Const.EXTRA)
                result_num = 1
            else:
                score = self.gsk_generator.tool_box.result_value_pk(Const.OOS) if result.in_store == 0 else \
                    self.gsk_generator.tool_box.result_value_pk(Const.DISTRIBUTED)
                result_num = result.in_store
            last_status = self.gsk_generator.tool_box.get_last_status(kpi_ecaps_product, result.product_fk)
            # score = result.in_store * 100
            results.append(
                {'fk': kpi_ecaps_product, SessionResultsConsts.NUMERATOR_ID: result.product_fk,
                 SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                 SessionResultsConsts.DENOMINATOR_RESULT: 1, SessionResultsConsts.NUMERATOR_RESULT: result_num,
                 SessionResultsConsts.RESULT: score,
                 SessionResultsConsts.SCORE: last_status,
                 'identifier_parent': identifier_parent, 'identifier_result': 1,
                 'should_enter': True})

        if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
            self.assortment.LVL2_HEADERS.extend(['total', 'passes'])
        lvl2 = self.assortment.calculate_lvl2_assortment(brand_results)
        if lvl2.empty:
            return 0, 0, 0, results  # in case of no assortment return 0
        result = round(np.divide(float(lvl2.iloc[0].passes), float(lvl2.iloc[0].total)), 4)
        return lvl2.iloc[0].passes, lvl2.iloc[0].total, result, results

    def pln_msl_summary(self, brand, assortment):
        """
                :param brand : pk of desired brand
                :param assortment : data frame of assortment products of the kpi, product's availability,
                                           product details. filtered by set up
                :return  numerator : how many products available out of the granular groups
                                             denominator : how many products in assortment groups
                                             result :  (numerator/denominator)*100
                                             results :  array of dictionary, each dict contains the result details
               """

        if assortment is None or assortment.empty:
            return 0, 0, 0, 0
        brand_results = assortment[assortment[ProductsConsts.BRAND_FK]
                                   == brand]  # only assortment of desired brand
        if 'total' not in self.assortment.LVL2_HEADERS or 'passes' not in self.assortment.LVL2_HEADERS:
            self.assortment.LVL2_HEADERS.extend(['total', 'passes'])

        lvl2 = self.assortment.calculate_lvl2_assortment(brand_results)
        if lvl2.empty:
            return 0, 0, 0, 0  # in case of no assortment return 0
        result = round(np.divide(float(lvl2.iloc[0].passes), float(lvl2.iloc[0].total)), 4)
        return lvl2.iloc[0].passes, lvl2.iloc[0].total, result, lvl2.iloc[0].assortment_group_fk

    def get_store_target(self):
        """
            Function checks which policies out of self.target are relevant to this store visit according to store
            attributes.
        """

        parameters_dict = {StoreInfoConsts.STORE_NUMBER_1: 'store_number'}
        for store_param, target_param in parameters_dict.items():
            if target_param in self.targets.columns:
                if self.store_info[store_param][0] is None:
                    if self.targets.empty or self.targets[self.targets[target_param] != ''].empty:
                        continue
                    else:
                        self.targets.drop(self.targets.index, inplace=True)
                self.targets = self.targets[
                    (self.targets[target_param] == self.store_info[store_param][0].encode(HelperConsts.UTF8)) |
                    (self.targets[target_param] == '')]

    def gsk_compliance(self):
        """
                    Function calculate compliance score for each brand based on : 
                    position score, brand-assortment score,
                    block score ,lsos score.
                    Also calculate  compliance summary score  - average of brands compliance scores
                """
        results_df = []
        df = self.scif
        # kpis
        kpi_block_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_BLOCK)
        kpi_position_fk = self.common.get_kpi_fk_by_kpi_type(Consts.POSITION_SCORE)
        kpi_lsos_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_LSOS)
        kpi_msl_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_MSL)
        kpi_compliance_brands_fk = self.common.get_kpi_fk_by_kpi_type(Consts.COMPLIANCE_ALL_BRANDS)
        kpi_compliance_summary_fk = self.common.get_kpi_fk_by_kpi_type(Consts.COMPLIANCE_SUMMARY)
        identifier_compliance_summary = self.common.get_dictionary(kpi_fk=kpi_compliance_summary_fk)

        # targets
        block_target = 0.25
        posit_target = 0.25
        lsos_target = 0.25
        msl_target = 0.25

        total_brand_score = 0
        counter_brands = 0

        # assortment_lvl3 msl df initialize
        self.gsk_generator.tool_box.extract_data_set_up_file(
            Consts.PLN_MSL, self.set_up_data, Consts.KPI_DICT)
        assortment_msl = self.msl_assortment(Const.DISTRIBUTION, Consts.PLN_MSL)

        # set data frame to find position shelf
        df_position_score = pd.merge(self.match_product_in_scene,
                                     self.all_products, on=ProductsConsts.PRODUCT_FK)
        df_position_score = pd.merge(self.scif[Const.SCIF_COLUMNS],
                                     df_position_score, how='right', right_on=[ScifConsts.SCENE_FK,
                                                                               ProductsConsts.PRODUCT_FK],
                                     left_on=[ScifConsts.SCENE_ID, ScifConsts.PRODUCT_FK])
        df_position_score = self.gsk_generator.tool_box.tests_by_template(Consts.POSITION_SCORE, df_position_score,
                                                                          self.set_up_data)

        if not self.set_up_data[(Const.INCLUDE_STACKING, Consts.POSITION_SCORE)]:
            df_position_score = df_position_score if df_position_score is None else df_position_score[
                df_position_score[MatchesConsts.STACKING_LAYER] == 1]

        # calculate all brands if template doesnt require specific brand else only for specific brands
        template_brands = self.set_up_data[(Const.BRANDS_INCLUDE, Consts.PLN_BLOCK)]
        brands = df[df[ProductsConsts.BRAND_NAME].isin(template_brands)][ProductsConsts.BRAND_FK].unique() if \
            template_brands else df[ProductsConsts.BRAND_FK].dropna().unique()

        for brand in brands:
            policy = self.targets[self.targets[ProductsConsts.BRAND_FK] == brand]
            if policy.empty:
                Log.warning('There is no target policy matching brand')  # adding brand name
                return results_df
            identifier_parent = self.common.get_dictionary(
                brand_fk=brand, kpi_fk=kpi_compliance_brands_fk)
            # msl_kpi
            msl_numerator, msl_denominator, msl_result, msl_assortment_group = self.pln_msl_summary(brand,
                                                                                                    assortment_msl)
            msl_score = msl_result * msl_target
            results_df.append({'fk': kpi_msl_fk, SessionResultsConsts.NUMERATOR_ID: brand,
                               SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                               SessionResultsConsts.DENOMINATOR_RESULT: msl_denominator,
                               SessionResultsConsts.NUMERATOR_RESULT: msl_numerator, SessionResultsConsts.RESULT:
                                   msl_result, SessionResultsConsts.SCORE: msl_score, SessionResultsConsts.TARGET:
                                   msl_target,
                               SessionResultsConsts.CONTEXT_ID:
                                   msl_assortment_group, 'identifier_parent': identifier_parent, 'should_enter': True})
            # lsos kpi
            lsos_numerator, lsos_result, lsos_denominator = self.lsos_score(brand, policy)
            lsos_result = 1 if lsos_result > 1 else lsos_result
            lsos_score = lsos_result * lsos_target
            results_df.append({'fk': kpi_lsos_fk, SessionResultsConsts.NUMERATOR_ID: brand,
                               SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                               SessionResultsConsts.DENOMINATOR_RESULT: lsos_denominator,
                               SessionResultsConsts.NUMERATOR_RESULT: lsos_numerator, SessionResultsConsts.RESULT:
                                   lsos_result,
                               SessionResultsConsts.SCORE: lsos_score, SessionResultsConsts.TARGET: lsos_target,
                               'identifier_parent': identifier_parent, SessionResultsConsts.WEIGHT: lsos_denominator,
                               'should_enter': True})
            # block_score
            block_result, block_benchmark, numerator_block, block_denominator = self.brand_blocking(brand, policy)
            block_score = round(block_result * block_target, 4)
            results_df.append({'fk': kpi_block_fk, SessionResultsConsts.NUMERATOR_ID: brand,
                               SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                               SessionResultsConsts.DENOMINATOR_RESULT: block_denominator,
                               SessionResultsConsts.NUMERATOR_RESULT: numerator_block, SessionResultsConsts.RESULT:
                                   block_result, SessionResultsConsts.SCORE: block_score,
                               SessionResultsConsts.TARGET: block_target,
                               'identifier_parent':
                                   identifier_parent, 'should_enter': True,
                               SessionResultsConsts.WEIGHT: (block_benchmark * 100)})

            # position score
            if df_position_score is not None:
                position_result, position_score, position_num, position_den, position_benchmark = self.position_shelf(
                    brand, policy, df_position_score)
            else:
                position_result, position_score, position_num, position_den, position_benchmark = 0, 0, 0, 0, 0
            position_score = round(position_score * posit_target, 4)
            results_df.append({'fk': kpi_position_fk, SessionResultsConsts.NUMERATOR_ID: brand,
                               SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                               SessionResultsConsts.DENOMINATOR_RESULT: position_den,
                               SessionResultsConsts.NUMERATOR_RESULT: position_num, SessionResultsConsts.RESULT:
                                   position_result, SessionResultsConsts.SCORE: position_score,
                               SessionResultsConsts.TARGET: posit_target,
                               'identifier_parent': identifier_parent, 'should_enter': True,
                               SessionResultsConsts.WEIGHT:
                                   position_benchmark})

            # compliance score per brand
            compliance_score = round(position_score + block_score + lsos_score + msl_score, 4)
            results_df.append(
                {'fk': kpi_compliance_brands_fk, SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer,
                 SessionResultsConsts.DENOMINATOR_ID: brand,
                 SessionResultsConsts.DENOMINATOR_RESULT: 1, SessionResultsConsts.NUMERATOR_RESULT: compliance_score,
                 SessionResultsConsts.RESULT:
                     compliance_score, SessionResultsConsts.SCORE: compliance_score,
                 'identifier_parent': identifier_compliance_summary, 'identifier_result': identifier_parent,
                 'should_enter': True})

            # counter and sum updates
            total_brand_score = round(total_brand_score + compliance_score, 4)
            counter_brands = counter_brands + 1
        if counter_brands == 0:
            return results_df
        # compliance summary
        average_brand_score = round(total_brand_score / counter_brands, 4)
        results_df.append(
            {'fk': kpi_compliance_summary_fk, SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer,
             SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
             SessionResultsConsts.DENOMINATOR_RESULT: counter_brands, SessionResultsConsts.NUMERATOR_RESULT:
                 total_brand_score, SessionResultsConsts.RESULT:
                 average_brand_score, SessionResultsConsts.SCORE: average_brand_score,
             'identifier_result': identifier_compliance_summary})

        return results_df

    def gsk_ecaps_kpis(self):
        """
                      Function calculate for each brand ecaps score, and for all brands together set ecaps summary score
                      :return
                             results_df :  array of dictionary, each dict contains kpi's result details
       """
        results_df = []
        kpi_ecaps_brands_fk = self.common.get_kpi_fk_by_kpi_type(Consts.ECAP_ALL_BRAND)
        kpi_ecaps_summary_fk = self.common.get_kpi_fk_by_kpi_type(Consts.ECAP_SUMMARY)
        identifier_ecaps_summary = self.common.get_dictionary(kpi_fk=kpi_ecaps_summary_fk)
        total_brand_score = 0
        assortment_display = self.msl_assortment(Consts.PLN_ASSORTMENT_KPI, Consts.ECAPS_FILTER_IDENT)

        if assortment_display is None or assortment_display.empty:
            return results_df
        template_brands = self.set_up_data[(Const.BRANDS_INCLUDE, Consts.ECAPS_FILTER_IDENT)]
        brands = assortment_display[assortment_display[ProductsConsts.BRAND_NAME].isin(template_brands)][
            ProductsConsts.BRAND_FK].unique() if \
            template_brands else assortment_display[ProductsConsts.BRAND_FK].dropna().unique()

        for brand in brands:
            numerator_res, denominator_res, result, product_presence_df = self.pln_ecaps_score(brand,
                                                                                               assortment_display)
            results_df.extend(product_presence_df)
            identifier_all_brand = self.common.get_dictionary(brand_fk=brand, kpi_fk=self.common.get_kpi_fk_by_kpi_type(
                Consts.ECAP_ALL_BRAND))
            results_df.append(
                {'fk': kpi_ecaps_brands_fk, SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer,
                 SessionResultsConsts.DENOMINATOR_ID: brand,
                 SessionResultsConsts.DENOMINATOR_RESULT: denominator_res, SessionResultsConsts.NUMERATOR_RESULT:
                     numerator_res, SessionResultsConsts.RESULT: result,
                 SessionResultsConsts.SCORE: result, 'identifier_parent': identifier_ecaps_summary, 'identifier_result':
                     identifier_all_brand, 'should_enter': True})

            total_brand_score = total_brand_score + result
        if len(brands) > 0:  # don't want to show result in case of there are no brands relevan to the template
            result_summary = round(total_brand_score / len(brands), 4)
            results_df.append(
                {'fk': kpi_ecaps_summary_fk, SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer,
                 SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                 SessionResultsConsts.DENOMINATOR_RESULT: len(brands), SessionResultsConsts.NUMERATOR_RESULT:
                     total_brand_score, SessionResultsConsts.RESULT:
                     result_summary, SessionResultsConsts.SCORE: result_summary,
                 'identifier_result': identifier_ecaps_summary})
        return results_df

    def gsk_pos_kpis(self):
        """
        Function calculate POSM Distribution
        Kpis  - GSK_POS_DISTRIBUTION_STORE,
                - posm distribution for own manufacturer(#No of available POS) by store (#No of POS in Assortment)
              - GSK_POS_DISTRIBUTION_BRAND
                - posm distribution for each brand (#No of available POS) by store (#No of POS by brand in Assortment)
                    - GSK_POS_DISTRIBUTION_SKU
                      - available POS 0/1
        :return
              - results :  array of dictionary, each dict contains kpi's result details
        """
        results = []
        # assortment_lvl3 msl df initialize
        # self.gsk_generator.tool_box.extract_data_set_up_file(
        #     Consts.PLN_MSL, self.set_up_data, Consts.KPI_DICT)
        # assortment_pos = self.msl_assortment(Const.DISTRIBUTION, Consts.PLN_MSL)

        self.gsk_generator.tool_box.extract_data_set_up_file(
            Consts.POSM, self.set_up_data, Consts.KPI_DICT
        )
        assortment_pos = self.msl_assortment(Consts.POSM_SKU, Consts.POSM)

        kpi_gsk_pos_distribution_store_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GSK_POS_DISTRIBUTION_STORE)
        kpi_gsk_pos_distribution_brand_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GSK_POS_DISTRIBUTION_BRAND)
        kpi_gsk_pos_distribution_sku_fk = self.common.get_kpi_fk_by_kpi_type(Consts.GSK_POS_DISTRIBUTION_SKU)

        if assortment_pos is None or assortment_pos.empty:
            Log.info("Assortment df is empty. GSK_POS_DISTRIBUTION Kpis are not calculated")
            return results

        # Calculate KPI : GSK_POS_DISTRIBUTION_STORE
        assortment_pos['in_store'] = assortment_pos['in_store'].astype('int')
        # TODO: Multiple-granular groups for a store's policy - Remove Duplicate product_fks if Any
        Log.info("Dropping duplicate product_fks accros multiple-granular groups")
        Log.info("Before : {}".format(len(assortment_pos)))
        assortment_pos = assortment_pos.drop_duplicates(subset=[ProductsConsts.PRODUCT_FK])
        Log.info("After : {}".format(len(assortment_pos)))

        # TODO: Make sure product type is "POS" only

        # Implement the logic to calculate the numerator & denominator
        numerator_res = len(assortment_pos[assortment_pos['in_store'] == 1])
        denominator_res = len(assortment_pos)

        result = round((numerator_res / float(denominator_res)), 4) if denominator_res != 0 else 0

        results.append(
            {'fk': kpi_gsk_pos_distribution_store_fk,
             SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer,
             SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
             SessionResultsConsts.NUMERATOR_RESULT: numerator_res,
             SessionResultsConsts.DENOMINATOR_RESULT: denominator_res,
             SessionResultsConsts.RESULT: result,
             SessionResultsConsts.SCORE: result,
             # 'identifier_parent': identifier_ecaps_summary,
             'identifier_result': "Gsk_Pos_Distribution_Store",
             'should_enter': True}
        )

        # Calculate KPI: GSK_POS_DISTRIBUTION_BRAND

        brands_group = assortment_pos.groupby([ProductsConsts.BRAND_FK])
        for brand, assortment_pos_by_brand in brands_group:
            numerator_res = len(assortment_pos_by_brand[assortment_pos_by_brand['in_store'] == 1])
            denominator_res = len(assortment_pos_by_brand)
            result = round((numerator_res / float(denominator_res)), 4) if denominator_res != 0 else 0
            # Implement the logic to calculate the numerator & denominator

            results.append(
                {'fk': kpi_gsk_pos_distribution_brand_fk,
                 SessionResultsConsts.NUMERATOR_ID: int(brand),
                 SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                 SessionResultsConsts.NUMERATOR_RESULT: numerator_res,
                 SessionResultsConsts.DENOMINATOR_RESULT: denominator_res,
                 SessionResultsConsts.RESULT: result,
                 SessionResultsConsts.SCORE: result,
                 'identifier_parent': "Gsk_Pos_Distribution_Store",
                 'identifier_result': "Gsk_Pos_Distribution_Brand_" + str(int(brand)),
                 'should_enter': True}
            )

            for idx, each_product in assortment_pos_by_brand.iterrows():
                product_fk = each_product[ProductsConsts.PRODUCT_FK]
                result = 1 if int(each_product['in_store']) == 1 else 0
                # Implement the logic to calculate the numerator & denominator
                DISTRIBUTED = 2
                OOS = 1
                result_status = DISTRIBUTED if result == 1 else OOS

                results.append(
                    {'fk': kpi_gsk_pos_distribution_sku_fk,
                     SessionResultsConsts.NUMERATOR_ID: product_fk,
                     SessionResultsConsts.DENOMINATOR_ID: self.store_fk,
                     SessionResultsConsts.NUMERATOR_RESULT: result,
                     SessionResultsConsts.DENOMINATOR_RESULT: 1,
                     SessionResultsConsts.RESULT: result_status,
                     SessionResultsConsts.SCORE: result_status,
                     'identifier_parent': "Gsk_Pos_Distribution_Brand_" + str(int(brand)),
                     'identifier_result': "Gsk_Pos_Distribution_SKU_" + str(int(product_fk)),
                     'should_enter': True}
                )

        return results
