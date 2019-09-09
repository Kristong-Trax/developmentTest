import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from Projects.GSKSG_SAND2.Data.LocalConsts import Consts
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import Const
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Consts import GlobalConsts, DataProvider as DataProviderConsts, DB

__author__ = 'limorc'


class GSKSGToolBox:

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        # self.common_old_tables = Common_old(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        # self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        # self.kpi_static_data = self.common.get_kpi_static_data()
        # self.old_kpi_static_data = self.common_old_tables.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        # self.calculations = {'SOS': self.calculate_sos, 'MSL': self.calculate_MSL, 'Sequence': self.calculate_sequence,
        #                      'Presence': self.calculate_presence, 'Facings': self.calculate_facings,
        #                      'No Facings': self.calculate_no_facings, 'Survey': self.calculate_survey}
        # self.sequence = Sequence(data_provider)
        # self.availability = Availability(data_provider)
        # self.sos = SOS(data_provider, self.output)
        # self.survey = Survey(data_provider, self.output)
        # self.toolbox = GENERALToolBox(self.data_provider)

        # adding data to templates
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)
        self.targets = self.ps_data_provider.get_kpi_external_targets()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        # """

        # assortment_store_dict = self.gsk_generator.availability_store_function()
        # self.common.save_json_to_new_tables(assortment_store_dict)
        assortment_category_dict = self.gsk_generator.availability_category_function()
        self.common.save_json_to_new_tables(assortment_category_dict)
        fsos_category_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        self.common.save_json_to_new_tables(fsos_category_dict)
        orange_score_dict = self.orange_score_category(assortment_category_dict, fsos_category_dict)
        self.common.save_json_to_new_tables(orange_score_dict)
        #
        # assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function()
        # self.common.save_json_to_new_tables(assortment_subcategory_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)

        self.common.commit_results_data()

        score = 0
        return score

    def msl_compliance_score(self, category, categories_results_json, category_targets,parent_result_identifier):
        results_list = []
        msl_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.MSL_ORANGE_SCORE)
        if category not in categories_results_json:
            dst_result = 0
        else:
            dst_result = categories_results_json[category]
        weight = category_targets['msl_weight'].iloc[0]
        result = dst_result * weight
        results_list.append({'fk': msl_kpi_fk, 'numerator_id': category, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': result, 'result': result,
                             'target': weight, 'score': result,
                             'identifier_parent': parent_result_identifier, 'should_enter': True})
        return result, results_list

    def fsos_compliance_score(self, category, categories_results_json, category_targets, parent_result_identifier):
        """
               This function return json of keys- categories and values -  kpi result for category
               :param category: pk of category
               :param categories_results_json: type of the desired kpi
               :return category json :  number-category_fk,number-result
           """
        results_list =[]
        fsos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FSOS_ORANGE_SCORE)
        dst_result = categories_results_json[category]
        benchmark = category_targets['fsos_benchmark'].iloc[0]
        weight = category_targets['fsos_weight'].iloc[0]
        result = weight if dst_result >= benchmark else 0
        results_list.append({'fk': fsos_kpi_fk, 'numerator_id': category, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': result, 'result': result,
                                                 'target': weight, 'score': result,
                                                 'identifier_parent': parent_result_identifier, 'should_enter': True})
        return result, results_list

    def extract_json_results_by_kpi(self, general_kpi_results, kpi_type):
        """
            This function return json of keys- categories and values -  kpi result for category
            :param general_kpi_results: list of json's , each json is the db results
            :param kpi_type: type of the desired kpi
            :return category json :  number-category_fk,number-result
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type)
        categories_results_json = self.extract_json_results(kpi_fk, general_kpi_results)
        return categories_results_json

    @staticmethod
    def extract_json_results(kpi_fk, general_kpi_results):
        """
        This function created json of keys- categories and values -  kpi result for category
        :param kpi_fk: pk of the kpi you want to extract results from.
        :param general_kpi_results: list of json's , each json is the db results
        :return category json :  number-category_fk,number-result
        """
        category_json = {}
        for row in general_kpi_results:
            if row['fk'] == kpi_fk:
                category_json[row[DB.SessionResultsConsts.DENOMINATOR_ID]] = row[DB.SessionResultsConsts.RESULT]
        return category_json

    def store_target(self):
        """
        This function filters the external targets df , to the only df with policy that answer current session's store
        attributes.
        It search which store attributes defined the targets policy.
        In addition it gives the targets flexibility to send "changed variables" , external targets need to save
        store param+_key and store_val + _value , than this function search the store param to look for and which value
        it need to have for this policy.
        """
        target_columns = self.targets.columns
        store_att = ['store_name', 'store_number', 'att']
        store_columns = [col for col in target_columns if len([att for att in store_att if att in col]) > 0]
        for col in store_columns:
            if self.targets.empty:
                return
            if 'key' in col:
                value = col.replace('_key', '') + '_value'
                if value not in store_columns:
                    continue
                self.target_test(col, value)
                store_columns.remove(value)
            else:
                if 'value' in col:
                    continue
                self.target_test(col)

    def target_test(self, store_param, store_param_val=None):
        """
        :param store_param: string , store attribute . by this attribute will compare between targets policy and
        current session
        :param store_param_val: string , if not None the store attribute value the policy have
               This function filters the targets to the only targets with a attributes that answer the current session's
                store attributes
        """
        store_param_val = store_param_val if store_param_val is not None else store_param
        store_param = [store_param] if store_param_val is None else self.targets[store_param].unique()
        for param in store_param:
            if param is None:
                continue
            if self.store_info[param][0] is None:
                if self.targets.empty:
                    return
                else:
                    self.targets.drop(self.targets.index, inplace=True)

            # targets contains, params equal to store info or , param value empty or none
            self.targets = self.targets[
                (self.targets[store_param_val] == self.store_info[param][0].encode(GlobalConsts.HelperConsts.UTF8)) |
                (self.targets[store_param_val] == '') | (not self.targets[store_param_val].any())]

    def display_distribution(self, display_name, category_fk, category_targets, parent_identifier, kpi_name, parent_kpi_name):

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name+Consts.COMPLIANCE_KPI)
        results_list = []
        display_products = self.scif[self.scif['product_type'] == 'POS']
        display_name = "{}_name".format(display_name.lower())
        display_names = category_targets[display_name].iloc[0]
        identifier_result = self.common.get_dictionary(category_fk=category_fk, kpi_fk=kpi_fk)
        kpi_result = 0
        if isinstance(display_names, str) or isinstance(display_names, unicode):
            display_array = []
            if len(display_names) > 0:
                display_array.append(display_names)
            display_names = display_array
        # check if name in
        for display in display_names:
            current_display_prod = display_products[display_products['product_name'].str.contains(display)]
            display_sku_level = self.display_sku_results(current_display_prod, category_fk, kpi_name)
            kpi_result += len(current_display_prod)
            results_list.extend(display_sku_level)

        weight = category_targets['{}_weight'.format(parent_kpi_name)].iloc[0]
        benchmark = category_targets['{}_benchmark'.format(parent_kpi_name)].iloc[0]
        kpi_score = weight if kpi_result >= benchmark else 0
        results_list.append({'fk': kpi_fk, 'numerator_id':   category_fk, 'denominator_id': self.store_id,
                             'denominator_result': 1, 'numerator_result': kpi_score, 'result': kpi_score,
                             'score': kpi_score, 'identifier_parent': parent_identifier, 'identifier_result':
                                 identifier_result, 'target': weight, 'should_enter': True})
        return kpi_score, results_list

    def display_sku_results(self, display_data, category_fk, kpi_name):
        results_list = []

        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name+Consts.SKU_LEVEL_LIST)
        parent_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name + Consts.COMPLIANCE_KPI)
        identifier_parent = self.common.get_dictionary(category_fk=category_fk, kpi_fk=parent_kpi_fk)

        display_names = display_data['item_id'].unique()
        for display in display_names:
            count = float(display_data[display_data['item_id'] == display]['facings'].sum()) / float(100)
            results_list.append({'fk': kpi_fk, 'numerator_id':   display, 'denominator_id': category_fk,
                                 'denominator_result': 1, 'numerator_result': count, 'result': count,
                                 'score': count, 'identifier_parent': identifier_parent, 'should_enter': True})
        return results_list

    def assortment(self):
        lvl3_assort, filter_scif = self.gsk_generator.tool_box.get_assortment_filtered(self.gsk_generator.tool_box.
                                                                                       set_up_data, "availability")
        return lvl3_assort, filter_scif

    def msl_assortment(self, kpi_name):
        """
                        :param kpi_name : name of level 3 assortment kpi
                        :return kpi_results : data frame of assortment products of the kpi, product's availability,
                        product details.(reduce assortment products that are not available)
                        filtered by set up
                """
        lvl3_assort, filtered_scif = self.assortment()
        if lvl3_assort is None or lvl3_assort.empty:
            return None
        kpi_assortment_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        kpi_results = lvl3_assort[lvl3_assort['kpi_fk_lvl3']
                                  == kpi_assortment_fk]  # general assortment
        kpi_results = pd.merge(kpi_results, self.all_products[Const.PRODUCTS_COLUMNS],
                               how='left', on='product_fk')
        kpi_results = kpi_results[kpi_results['in_store'] == 1]
        kpi_results = kpi_results[kpi_results['substitution_product_fk'].isnull()]
        shelf_data = pd.merge(self.match_product_in_scene[['scene_fk', 'product_fk', 'shelf_number']],
                              filtered_scif[['scene_id', 'product_fk']], how='right', left_on=
                              ['scene_fk', 'product_fk'], right_on=['scene_id', 'product_fk'])

        kpi_results = pd.merge(shelf_data, kpi_results, how='right', on=['product_fk'])
        return kpi_results

    def shelf_compliance(self, category, assortment_df, category_targets, identifier_parent):
        results_list = []
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.SHELF_COMPLIANCE)
        assortment_cat = assortment_df[assortment_df['category_fk'] == category]
        shelf_weight = category_targets['shelf_weight'].iloc[0]
        benchmark = category_targets['shelf_benchmark'].iloc[0]
        shelves = [int(shelf) for shelf in category_targets['shelf_number'].iloc[0].split(",")]
        shelf_df = assortment_cat[assortment_cat['shelf_number'].isin(shelves)]
        numerator = shelf_df.shape[0]
        denominator = assortment_cat.shape[0]
        result = float(numerator) / float(denominator)
        score = shelf_weight if result >= benchmark else 0
        results_list.append({'fk': kpi_fk, 'numerator_id': category, 'denominator_id':
            self.store_id, 'denominator_result': denominator, 'numerator_result': numerator, 'result': score,
                             'target': shelf_weight, 'score': score,
                             'identifier_parent': identifier_parent, 'should_enter': True})
        return score, results_list, shelf_weight

    def planogram(self, category_fk, assortment, category_targets, parent_result_identifier):
        results_list = []
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_CATEGORY)
        identifier_result = self.common.get_dictionary(category_fk=category_fk, kpi_fk=kpi_fk)

        shelf_compliance_score, shelf_compliance_result, shelf_weight = self.shelf_compliance(category_fk, assortment,
                                                                                              category_targets,
                                                                                              identifier_result)
        results_list.extend(shelf_compliance_result)
        sequence_kpi, sequence_weight = 0, self.sequence(category_targets)
        planogram_score = shelf_compliance_score + sequence_kpi
        planogram_weight = shelf_weight + sequence_weight
        results_list.append({'fk': kpi_fk, 'numerator_id': category_fk, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': planogram_score, 'result': planogram_score,
                             'target': planogram_weight, 'score': planogram_score,
                             'identifier_parent': parent_result_identifier, 'identifier_result': identifier_result
                             ,'should_enter': True})
        return planogram_score, results_list

    def sequence(self, category_targets):

        seq_1_target = 0 if category_targets['seq_1_weight'].empty else category_targets['seq_1_weight'].iloc[0]
        seq_2_target = 0 if category_targets['seq_2_weight'].empty else category_targets['seq_2_weight'].iloc[0]
        seq_3_target = 0 if category_targets['seq_3_weight'].empty else category_targets['seq_3_weight'].iloc[0]

        sequence_weight = seq_1_target + seq_2_target + seq_3_target

        return sequence_weight

    def secondary_display(self, category_fk, category_targets, identifier_parent):
        # display compliance
        results_list = []
        parent_kpi_name = 'display'
        weight = category_targets['display_weight'].iloc[0]
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.DISPLAY_SUMMARY)
        result_identifier = self.common.get_dictionary(category_fk=category_fk, kpi_fk=total_kpi_fk)

        dispenser_score, dispenser_res = self.display_distribution(Consts.DISPENSER_TARGET, category_fk,
                                                                   category_targets, result_identifier,
                                                                   Consts.DISPENSERS, parent_kpi_name)
        counter_top_score, counter_top_res = self.display_distribution(Consts.COUNTER_TOP_TARGET, category_fk,
                                                                       category_targets, result_identifier,
                                                                       Consts.COUNTERTOP, parent_kpi_name)
        standee_score, standee_res = self.display_distribution(Consts.STANDEE_TARGET, category_fk, category_targets,
                                                               result_identifier, Consts.STANDEE, parent_kpi_name)
        results_list.extend(dispenser_res)
        results_list.extend(counter_top_res)
        results_list.extend(standee_res)

        display_score = weight if (dispenser_score == weight) or (counter_top_score == weight) or (standee_score ==
                                                                                                   weight) else 0
        results_list.append({'fk': total_kpi_fk, 'numerator_id':   category_fk, 'denominator_id': self.store_id,
                             'denominator_result': 1, 'numerator_result': display_score, 'result': display_score,
                             'target': weight, 'score': display_score, 'identifier_parent': identifier_parent,
                             'identifier_result': result_identifier,'should_enter': True})

        return results_list, display_score

    def promo_activation(self, category_fk, category_targets, identifier_parent):
        # display compliance
        # Promotion activation
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PROMO_SUMMARY)
        result_identifier = self.common.get_dictionary(category_fk=category_fk, kpi_fk=total_kpi_fk)
        results_list = []
        parent_kpi_name = 'promo'
        weight = category_targets['promo_weight'].iloc[0]

        hang_shell_score, hang_shell_res = self.display_distribution('Hang_Sell', category_fk, category_targets,
                                                                     result_identifier, Consts.HANGSELL,
                                                                     parent_kpi_name)
        top_shelf_score, top_shelf_res = self.display_distribution('Top_Shelf', category_fk, category_targets,
                                                                   result_identifier, Consts.TOP_SHELF, parent_kpi_name)
        results_list.extend(hang_shell_res)
        results_list.extend(top_shelf_res)
        promo_score = weight if (hang_shell_score == weight) or (top_shelf_score == weight) else 0

        results_list.append({'fk': total_kpi_fk, 'numerator_id': category_fk, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': promo_score, 'result': promo_score,
                                                 'target': weight,
                                                 'score': promo_score, 'identifier_parent': identifier_parent,
                                                 'identifier_result': result_identifier, 'should_enter': True})

        return results_list, promo_score

    def orange_score_category(self, assortment_category_dict, fsos_category_dict):
        results_list = []
        self.store_target()
        if self.targets.empty:
            return

        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.ORANGE_SCORE_COMPLIANCE)
        fsos_results = self.extract_json_results_by_kpi(fsos_category_dict, Consts.GLOBAL_FSOS_BY_CATEGORY)
        msl_results = self.extract_json_results_by_kpi(assortment_category_dict, Consts.GLOBAL_DST_BY_CATEGORY)
        categories = self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK].unique()

        assortment = self.msl_assortment('Distribution - SKU')
        for cat in categories:
            orange_score_result_identifier = self.common.get_dictionary(category_fk=cat, kpi_fk=total_kpi_fk)

            cat_targets = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == cat]

            msl_score, msl_results = self.msl_compliance_score(cat, msl_results, cat_targets,
                                                               orange_score_result_identifier)

            fsos_score, fsos_results = self.fsos_compliance_score(cat, fsos_results, cat_targets,
                                                                  orange_score_result_identifier)

            planogram_score, planogram_results = self.planogram(cat, assortment, cat_targets,
                                                                orange_score_result_identifier)

            secondary_display_res, secondary_score = self.secondary_display(cat, cat_targets,
                                                                            orange_score_result_identifier)

            promo_activation_res, promo_score = self.promo_activation(cat, cat_targets,
                                                                      orange_score_result_identifier)

            compliance_category_score = promo_score + secondary_score + fsos_score + msl_score + planogram_score
            results_list.extend(msl_results+fsos_results+planogram_results+secondary_display_res+promo_activation_res)
            results_list.append({'fk': total_kpi_fk, 'numerator_id': cat, 'denominator_id': self.store_id,
                                 'denominator_result': 1, 'numerator_result': compliance_category_score, 'result':
                                     compliance_category_score, 'score': compliance_category_score,
                                 'identifier_result': orange_score_result_identifier})
        return results_list
