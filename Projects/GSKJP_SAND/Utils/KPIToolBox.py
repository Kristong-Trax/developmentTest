
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
import numpy as np
import math
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from Trax.Utils.Logging.Logger import Log
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import Const
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class GSKJPToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    # Gsk Japan kpis

    PLN_BLOCK = 'GSK_PLN_BLOCK_SCORE'
    POSITION_SCORE = 'GSK_PLN_POSITION_SCORE'
    PRODUCT_PRESENCE = 'GSK_PLN_ECAPS_PRODUCT_PRESENCE'
    PLN_MSL = 'GSK_PLN_MSL_SCORE'
    PLN_LSOS = 'GSK_PLN_LSOS_SCORE'
    COMPLIANCE_ALL_BRANDS = 'GSK_PLN_COMPLIANCE_ALL_BRANDS'
    ECAP_SUMMARY = 'GSK_PLN_ECAPS_SUMMARY'
    COMPLIANCE_SUMMARY = 'GSK_PLN_COMPLIANCE_SUMMARY'
    ECAP_ALL_BRAND = 'GSK_PLN_ECAPS_ALL BRANDS'
    GLOBAL_LSOS_BRAND_BY_STORE = 'GSK_LSOS_All_Brand_In_Whole_Store'
    PLN_ASSORTMENT_KPI = 'PLN_ECAPS - SKU'
    ECAPS_FILTER_IDENT = 'GSK_ECAPS'

    KPI_DICT = {"GSK_PLN_BLOCK_SCORE": "GSK_PLN_BLOCK_SCORE", "GSK_ECAPS": "GSK_ECAPS",
                "GSK_PLN_MSL_SCORE": "GSK_PLN_MSL_SCORE",
                "GSK_PLN_POSITION_SCORE": "GSK_PLN_POSITION_SCORE", "GSK_PLN_LSOS_SCORE": "GSK_PLN_LSOS_SCORE"}

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

        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)
        self.blocking_generator = Block(self.data_provider)
        self.assortment = self.gsk_generator.get_assortment_data_provider()
        self.store_info = self.data_provider['store_info']
        self.store_fk = self.data_provider['store_fk']
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.own_manufacturer = self.get_manufacturer
        self.set_up_data = {(self.PLN_BLOCK, Const.KPI_TYPE_COLUMN): Const.NO_INFO,
                            (self.POSITION_SCORE, Const.KPI_TYPE_COLUMN):
                                Const.NO_INFO, (self.ECAPS_FILTER_IDENT, Const.KPI_TYPE_COLUMN):
                                Const.NO_INFO, (self.PLN_MSL, Const.KPI_TYPE_COLUMN):
                                Const.NO_INFO, ("GSK_PLN_LSOS_SCORE",
                                                Const.KPI_TYPE_COLUMN): Const.NO_INFO}

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
        for kpi in self.KPI_DICT.keys():
            self.gsk_generator.tool_box.extract_data_set_up_file(kpi, self.set_up_data, self.KPI_DICT)

        results_ecaps = self.gsk_ecaps_kpis()
        self.common.save_json_to_new_tables(results_ecaps)

        self.get_store_target()  # choosing the policy
        if self.targets.empty:
            Log.warning('There is no target policy matching this store')
        else:
            results_compliance = self.gsk_compliance()
            self.common.save_json_to_new_tables(results_compliance)
        self.common.commit_results_data()
        return

    def position_shelf(self, brand_fk, policy, df):
        """
        :param  brand_fk :
        :param  policy : dictionary that contains {
                                                'shelves':"1 ,2 ,4 ,5" (or any other string of numbers separate by ','),
                                                'position_target': 80 (or any other percentage you want the score to reach)
                                                }
        :param  df: data frame that contains columns "shelf_number" , "brand kf"

        :returns   tuple of (result,score,numerator,denominator)
                   result = number of products from brand_fk in shelves / number of products from brand_fk ,
                   score  = if result reach position target 100  else 0 ,
                   numerator = number of products from brand_fk in shelves
                   denominator = number of products from brand_fk
        """
        shelf_from_bottom = [int(shelf) for shelf in policy['shelves'].iloc[0].split(",")]
        threshold = policy['position_target'].iloc[0]
        brand_df = df[df['brand_fk'] == brand_fk]
        shelf_df = brand_df[brand_df['shelf_number'].isin(shelf_from_bottom)]
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
                      self.all_products[Const.PRODUCTS_COLUMNS], how='left', on=['product_fk'])
        df = pd.merge(self.scif[Const.SCIF_COLUMNS],
                      df, how='right', right_on=['scene_fk', 'product_fk'], left_on=['scene_id', 'product_fk'])

        if df.empty:
            Log.warning('match_product_in_scene is empty ')
            return 0, 0, 0
        df = self.gsk_generator.tool_box.tests_by_template('GSK_PLN_LSOS_SCORE', df, self.set_up_data)
        if df is None:
            Log.warning('match_product_in_scene is empty ')
            return 0, 0, 0
        result = self.gsk_generator.tool_box.calculate_sos(df, {'brand_fk': brand}, {}, Const.LINEAR)[0]
        target = policy['brand_target'].iloc[0]
        score = float(result) / float(target)
        return result, score, target

    def brand_blocking(self, brand, policy):
        """
                :param brand : pk of brand
                :param policy :  dictionary of  { 'block_target' : number you want to reach}
                :return result : 1 if there is a block answer set_up_data conditions else 0
        """
        templates = self.set_up_data[(Const.SCENE_TYPE, self.PLN_BLOCK)]
        template_name = {
            'template_name': templates} if templates else None  # figure out which template name should I use
        ignore_empty = False
        # taking from params from set up  info
        stacking_param = False if not self.set_up_data[(
            Const.INCLUDE_STACKING, self.PLN_BLOCK)] else True  # false
        population_parameters = {'brand_fk': [brand], 'product_type': ['SKU']}

        if self.set_up_data[(Const.INCLUDE_OTHERS, self.PLN_BLOCK)]:
            population_parameters['product_type'].append(Const.OTHER)
        if self.set_up_data[(Const.INCLUDE_IRRELEVANT, self.PLN_BLOCK)]:
            population_parameters['product_type'].append(Const.IRRELEVANT)
        if self.set_up_data[(Const.INCLUDE_EMPTY, self.PLN_BLOCK)]:

            population_parameters['product_type'].append(Const.EMPTY)
        else:
            ignore_empty = True

        if self.set_up_data[(Const.CATEGORY_INCLUDE, self.PLN_BLOCK)]:  # category_name
            population_parameters['category'] = self.set_up_data[(Const.CATEGORY_INCLUDE, self.PLN_BLOCK)]

        if self.set_up_data[(Const.SUB_CATEGORY_INCLUDE, self.PLN_BLOCK)]:  # sub_category_name
            population_parameters['sub_category'] = self.set_up_data[(Const.SUB_CATEGORY_INCLUDE, self.PLN_BLOCK)]

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

        # numerator = len(result['cluster'].values[0].node.keys())
        # numerator = 0
        # cluster = result['cluster'].iloc[0]
        # for node in cluster.nodes.data():
        #     print node
        # for dict in cluster._node.values():
        #     numerator = numerator + dict['group_attributes']['facings']

        # nodes_sum = 0
        # for node_clust in result['cluster'].values:
        #     nodes_sum = nodes_sum + node_clust.node.keys()
        # # numerator
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
                               how='left', on='product_fk')

        kpi_results = kpi_results[kpi_results['substitution_product_fk'].isnull()]
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
                                                       kpi_fk=self.common.get_kpi_fk_by_kpi_type(self.ECAP_ALL_BRAND))
        results = []
        kpi_ecaps_product = self.common.get_kpi_fk_by_kpi_type(self.PRODUCT_PRESENCE)
        ecaps_assortment_fk = self.common.get_kpi_fk_by_kpi_type(self.PLN_ASSORTMENT_KPI)
        if assortment.empty:
            return 0, 0, 0, results
        brand_results = assortment[assortment['brand_fk']
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
                {'fk': kpi_ecaps_product, 'numerator_id': result.product_fk, 'denominator_id': self.store_fk,
                 'denominator_result': 1, 'numerator_result': result_num, 'result': score,
                 'score': last_status,
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
        brand_results = assortment[assortment['brand_fk']
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

        parameters_dict = {'store_number_1': 'store_number'}
        for store_param, target_param in parameters_dict.items():
            if target_param in self.targets.columns:
                if self.store_info[store_param][0] is None:
                    if self.targets.empty or self.targets[self.targets[target_param] != ''].empty:
                        continue
                    else:
                        self.targets.drop(self.targets.index, inplace=True)
                self.targets = self.targets[
                    (self.targets[target_param] == self.store_info[store_param][0].encode('utf-8')) |
                    (self.targets[target_param] == '')]

    def gsk_compliance(self):
        """
                    Function calculate compliance score for each brand based on : position score, brand-assortment score,
                    block score ,lsos score.
                    Also calculate  compliance summary score  - average of brands compliance scores
                """
        results_df = []
        df = self.scif
        # kpis
        kpi_block_fk = self.common.get_kpi_fk_by_kpi_type(self.PLN_BLOCK)
        kpi_position_fk = self.common.get_kpi_fk_by_kpi_type(self.POSITION_SCORE)
        kpi_lsos_fk = self.common.get_kpi_fk_by_kpi_type(self.PLN_LSOS)
        kpi_msl_fk = self.common.get_kpi_fk_by_kpi_type(self.PLN_MSL)
        kpi_compliance_brands_fk = self.common.get_kpi_fk_by_kpi_type(self.COMPLIANCE_ALL_BRANDS)
        kpi_compliance_summary_fk = self.common.get_kpi_fk_by_kpi_type(self.COMPLIANCE_SUMMARY)
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
            self.PLN_MSL, self.set_up_data, self.KPI_DICT)
        assortment_msl = self.msl_assortment(Const.DISTRIBUTION, self.PLN_MSL)

        # set data frame to find position shelf
        df_position_score = pd.merge(self.match_product_in_scene,
                                     self.all_products, on="product_fk")
        df_position_score = pd.merge(self.scif[Const.SCIF_COLUMNS],
                                     df_position_score, how='right', right_on=['scene_fk', 'product_fk'],
                                     left_on=['scene_id', 'product_fk'])
        df_position_score = self.gsk_generator.tool_box.tests_by_template(self.POSITION_SCORE, df_position_score,
                                                                          self.set_up_data)
        # df_block = self.gsk_generator.tool_box.tests_by_template(self.PLN_BLOCK, df,
        #                                                                   self.set_up_data)
        if not self.set_up_data[(Const.INCLUDE_STACKING, self.POSITION_SCORE)]:
            df_position_score = df_position_score if df_position_score is None else df_position_score[
                df_position_score['stacking_layer'] == 1]

        # calculate all brands if template doesnt require specific brand else only for specific brands
        template_brands = self.set_up_data[(Const.BRANDS_INCLUDE, self.PLN_BLOCK)]
        brands = df[df['brand_name'].isin(template_brands)]['brand_fk'].unique() if \
            template_brands else df['brand_fk'].dropna().unique()

        for brand in brands:
            policy = self.targets[self.targets['brand_fk'] == brand]
            if policy.empty:
                Log.warning('There is no target policy matching brand')  # adding brand name
                return results_df
            identifier_parent = self.common.get_dictionary(
                brand_fk=brand, kpi_fk=kpi_compliance_brands_fk)
            # msl_kpi
            msl_numerator, msl_denominator, msl_result, msl_assortment_group = self.pln_msl_summary(brand, assortment_msl)
            msl_score = msl_result * msl_target
            results_df.append({'fk': kpi_msl_fk, 'numerator_id': brand, 'denominator_id': self.store_fk,
                               'denominator_result': msl_denominator, 'numerator_result': msl_numerator, 'result':
                                   msl_result, 'score': msl_score, 'target': (msl_target * 100), 'context_id':msl_assortment_group,
                               'identifier_parent': identifier_parent,
                               'should_enter': True})
            # lsos kpi
            lsos_numerator, lsos_result, lsos_denominator = self.lsos_score(brand, policy)
            lsos_result = 1 if lsos_result > 1 else lsos_result
            lsos_score = lsos_result * lsos_target
            results_df.append({'fk': kpi_lsos_fk, 'numerator_id': brand, 'denominator_id': self.store_fk,
                               'denominator_result': lsos_denominator, 'numerator_result': lsos_numerator, 'result':
                                   lsos_result, 'score': lsos_score, 'target': (lsos_target * 100),
                               'identifier_parent': identifier_parent, 'weight': lsos_denominator,
                               'should_enter': True})
            # block_score
            # block_result, block_benchmark, block_numerator, block_denominator = self.brand_blocking(brand, policy,df_block)
            block_result, block_benchmark, numerator_block, block_denominator = self.brand_blocking(brand, policy)
            block_score = round(block_result * block_target, 4)
            results_df.append({'fk': kpi_block_fk, 'numerator_id': brand, 'denominator_id': self.store_fk,
                               'denominator_result': block_denominator, 'numerator_result': numerator_block, 'result':
                                   block_result, 'score': block_score, 'target': (block_target * 100),
                               'identifier_parent':
                                   identifier_parent, 'should_enter': True, 'weight': (block_benchmark * 100)})

            # position score
            if df_position_score is not None:
                position_result, position_score, position_num, position_den, position_benchmark = self.position_shelf(
                    brand, policy, df_position_score)
            else:
                position_result, position_score, position_num, position_den, position_benchmark = 0, 0, 0, 0, 0
            position_score = round(position_score * posit_target, 4)
            results_df.append({'fk': kpi_position_fk, 'numerator_id': brand, 'denominator_id': self.store_fk,
                               'denominator_result': position_den, 'numerator_result': position_num, 'result':
                                   position_result, 'score': position_score, 'target': posit_target,
                               'identifier_parent': identifier_parent, 'should_enter': True, 'weight':
                                   position_benchmark})

            # compliance score per brand
            compliance_score = round(position_score + block_score + lsos_score + msl_score, 4)
            results_df.append(
                {'fk': kpi_compliance_brands_fk, 'numerator_id': self.own_manufacturer, 'denominator_id': brand,
                 'denominator_result': 1, 'numerator_result': compliance_score, 'result':
                     compliance_score, 'score': compliance_score,
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
            {'fk': kpi_compliance_summary_fk, 'numerator_id': self.own_manufacturer, 'denominator_id': self.store_fk,
             'denominator_result': counter_brands, 'numerator_result': total_brand_score, 'result':
                 average_brand_score, 'score': average_brand_score,
             'identifier_result': identifier_compliance_summary})

        return results_df

    def gsk_ecaps_kpis(self):
        """
                      Function calculate for each brand ecaps score, and for all brands together set ecaps summary score
                      :return
                             results_df :  array of dictionary, each dict contains kpi's result details
       """
        results_df = []
        kpi_ecaps_brands_fk = self.common.get_kpi_fk_by_kpi_type(self.ECAP_ALL_BRAND)
        kpi_ecaps_summary_fk = self.common.get_kpi_fk_by_kpi_type(self.ECAP_SUMMARY)
        identifier_ecaps_summary = self.common.get_dictionary(kpi_fk=kpi_ecaps_summary_fk)
        total_brand_score = 0
        assortment_display = self.msl_assortment(self.PLN_ASSORTMENT_KPI, self.ECAPS_FILTER_IDENT)

        if assortment_display is None or assortment_display.empty:
            return results_df
        template_brands = self.set_up_data[(Const.BRANDS_INCLUDE, self.ECAPS_FILTER_IDENT)]
        brands = assortment_display[assortment_display['brand_name'].isin(template_brands)]['brand_fk'].unique() if \
            template_brands else assortment_display['brand_fk'].dropna().unique()

        for brand in brands:
            numerator_res, denominator_res, result, product_presence_df = self.pln_ecaps_score(brand,
                                                                                               assortment_display)
            results_df.extend(product_presence_df)
            identifier_all_brand = self.common.get_dictionary(brand_fk=brand, kpi_fk=self.common.get_kpi_fk_by_kpi_type(
                self.ECAP_ALL_BRAND))
            results_df.append(
                {'fk': kpi_ecaps_brands_fk, 'numerator_id': self.own_manufacturer, 'denominator_id': brand,
                 'denominator_result': denominator_res, 'numerator_result': numerator_res, 'result': result,
                 'score': result, 'identifier_parent': identifier_ecaps_summary, 'identifier_result':
                     identifier_all_brand, 'should_enter': True})

            total_brand_score = total_brand_score + result
        if len(brands) > 0:  # don't want to show result in case of there are no brands relevan to the template
            result_summary = round(total_brand_score / len(brands), 4)
            results_df.append(
                {'fk': kpi_ecaps_summary_fk, 'numerator_id': self.own_manufacturer, 'denominator_id': self.store_fk,
                 'denominator_result': len(brands), 'numerator_result': total_brand_score, 'result':
                     result_summary, 'score': result_summary,
                 'identifier_result': identifier_ecaps_summary})
        return results_df
