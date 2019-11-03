import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.GSKSG_SAND2.Data.LocalConsts import Consts
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import Const
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Consts import GlobalConsts, DataProvider as DataProviderConsts, DB
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts, TemplatesConsts
from KPIUtils_v2.Utils.Consts.DB import SessionResultsConsts
from KPIUtils_v2.Calculations.SequenceCalculationsV2 import Sequence
from KPIUtils_v2.Calculations.CalculationsUtils.Constants import AdditionalAttr

__author__ = 'limorc'


class GSKSGToolBox:
    KPI_DICT = {"planogram": "planogram", "secondary_display": "secondary_display",
                "promo": "promo"}

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.manufacturer_fk = None if self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0] is None else \
            int(self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0])
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)

        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.sequence = Sequence(self.data_provider)
        self.set_up_data = {('planogram', Const.KPI_TYPE_COLUMN): Const.NO_INFO,
                            ('secondary_display', Const.KPI_TYPE_COLUMN):
                                Const.NO_INFO, ('promo', Const.KPI_TYPE_COLUMN):
                                Const.NO_INFO}

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        # # global kpis in store_level
        assortment_store_dict = self.gsk_generator.availability_store_function()
        self.common.save_json_to_new_tables(assortment_store_dict)
        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function()
        self.common.save_json_to_new_tables(facings_sos_dict)
        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        # global kpis in category level & kpi results are used for orange score kpi
        assortment_category_dict = self.gsk_generator.availability_category_function()
        self.common.save_json_to_new_tables(assortment_category_dict)
        fsos_category_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        self.common.save_json_to_new_tables(fsos_category_dict)

        # updating the set up dictionary for all local kpis
        for kpi in self.KPI_DICT.keys():
            self.gsk_generator.tool_box.extract_data_set_up_file(kpi, self.set_up_data, self.KPI_DICT)

        orange_score_dict = self.orange_score_category(assortment_category_dict, fsos_category_dict)

        self.common.save_json_to_new_tables(orange_score_dict)
        self.common.commit_results_data()

        score = 0
        return score

    def msl_compliance_score(self, category, categories_results_json, cat_targets, parent_result_identifier):
        category_targets = self._filter_out_sequence_targets(cat_targets)
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

    def fsos_compliance_score(self, category, categories_results_json, cat_targets, parent_result_identifier):
        """
               This function return json of keys- categories and values -  kpi result for category
               :param cat_targets-targets df for the specific category
               :param category: pk of category
               :param categories_results_json: type of the desired kpi
               :return category json :  number-category_fk,number-result
           """
        category_targets = self._filter_out_sequence_targets(cat_targets)
        results_list = []
        fsos_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.FSOS_ORANGE_SCORE)
        dst_result = categories_results_json[category] if category in categories_results_json.keys() else 0
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
            This function return json of keys and values. keys= categories & values = kpi result for category
            :param general_kpi_results: list of json's , each json is a db result
            :param kpi_type: type of the desired kpi
            :return category json :  number-category_fk,number-result
        """
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type)
        if general_kpi_results is None:
            return {}
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

            self.targets['target_match'] = self.targets[store_param_val].apply(self.checking_param, store_info_col=param)
            self.targets = self.targets[self.targets['target_match']]

    def checking_param(self, df_param, store_info_col):
        # x is  self.targets[store_param_val]
        if isinstance(df_param, list):
            if self.store_info[store_info_col][0].encode(GlobalConsts.HelperConsts.UTF8) in df_param:
                return True
        if isinstance(df_param, unicode):
            if self.store_info[store_info_col][0].encode(GlobalConsts.HelperConsts.UTF8) == df_param or df_param == '':
                return True
        if isinstance(df_param, type(None)):
            return True
        return False

    def display_distribution(self, display_name, category_fk, cat_targets, parent_identifier, kpi_name,
                             parent_kpi_name, scif_df):

        """
          This Function sum facings of posm that it name contains substring (decided by external_targets )
          if sum facings is equal/bigger than benchmark that gets weight.
            :param display_name display name (in external targets this key contains relevant substrings)
            :param category_fk
            :param cat_targets-targets df for the specific category
            :param parent_identifier  - result identifier for this kpi parent
            :param kpi_name - kpi name
            :param parent_kpi_name - this parent kpi name
            :param scif_df - scif filtered by promo activation settings

        """
        category_targets = self._filter_out_sequence_targets(cat_targets)
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name + Consts.COMPLIANCE_KPI)
        results_list = []
        identifier_result = self.common.get_dictionary(category_fk=category_fk, kpi_fk=kpi_fk)
        weight = category_targets['{}_weight'.format(parent_kpi_name)].iloc[0]

        if scif_df is None:
            results_list.append({'fk': kpi_fk, 'numerator_id': category_fk, 'denominator_id': self.store_id,
                                 'denominator_result': 1, 'numerator_result': 0, 'result': 0,
                                 'score': 0, 'identifier_parent': parent_identifier, 'identifier_result':
                                     identifier_result, 'target': weight, 'should_enter': True})
            return 0, results_list

        display_products = scif_df[(scif_df['product_type'] == 'POS') & (scif_df['category_fk'] == category_fk)]

        display_name = "{}_name".format(display_name.lower())
        display_names = category_targets[display_name].iloc[0]
        kpi_result = 0

        # check's if display names (received from external targets) are string or array of strings
        if isinstance(display_names, str) or isinstance(display_names, unicode):
            display_array = []
            if len(display_names) > 0:
                display_array.append(display_names)
            display_names = display_array

        # for each display name , search POSM that contains display name (= sub string)
        for display in display_names:
            current_display_prod = display_products[display_products['product_name'].str.contains(display)]
            display_sku_level = self.display_sku_results(current_display_prod, category_fk, kpi_name)
            kpi_result += current_display_prod['facings'].sum()
            results_list.extend(display_sku_level)

        benchmark = category_targets['{}_benchmark'.format(parent_kpi_name)].iloc[0]
        kpi_score = weight if kpi_result >= benchmark else 0
        results_list.append({'fk': kpi_fk, 'numerator_id': category_fk, 'denominator_id': self.store_id,
                             'denominator_result': 1, 'numerator_result': kpi_score, 'result': kpi_score,
                             'score': kpi_score, 'identifier_parent': parent_identifier, 'identifier_result':
                                 identifier_result, 'target': weight, 'should_enter': True})
        return kpi_score, results_list

    def display_sku_results(self, display_data, category_fk, kpi_name):
        """
          This Function create for each posm in display data  db result with score of  posm facings.
            :param category_fk
            :param display_data-targets df for the specific category
            :param kpi_name - kpi name
        """
        results_list = []
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name + Consts.SKU_LEVEL_LIST)
        parent_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name + Consts.COMPLIANCE_KPI)
        identifier_parent = self.common.get_dictionary(category_fk=category_fk, kpi_fk=parent_kpi_fk)

        display_names = display_data['item_id'].unique()
        for display in display_names:
            count = float(display_data[display_data['item_id'] == display]['facings'].sum()) / float(100)
            results_list.append({'fk': kpi_fk, 'numerator_id': display, 'denominator_id': category_fk,
                                 'denominator_result': 1, 'numerator_result': count, 'result': count,
                                 'score': count, 'identifier_parent': identifier_parent, 'should_enter': True})
        return results_list

    def assortment(self):
        """
          This Function get relevant assortment based on filtered scif
        """
        lvl3_assort, filter_scif = self.gsk_generator.tool_box.get_assortment_filtered(self.set_up_data, "planogram")
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
        # only distributed products
        kpi_results = kpi_results[kpi_results['in_store'] == 1]

        # filtering substitied products
        kpi_results = kpi_results[kpi_results['substitution_product_fk'].isnull()]

        shelf_data = pd.merge(self.match_product_in_scene[['scene_fk', 'product_fk', 'shelf_number']],
                              filtered_scif[['scene_id', 'product_fk']], how='right', left_on=
                              ['scene_fk', 'product_fk'], right_on=['scene_id', 'product_fk'])  # why is this happening?

        # merge assortment results with match_product_in_scene for shelf_number parameter
        kpi_results = pd.merge(shelf_data, kpi_results, how='right', on=['product_fk'])  # also problematic
        return kpi_results

    def shelf_compliance(self, category, assortment_df, cat_targets, identifier_parent):
        """
            This function calculate how many assortment products available on specific shelves
            :param category
            :param cat_targets : targets df for the specific category
            :param assortment_df :relevant assortment based on filtered scif
            :param identifier_parent - result identifier for shelf compliance kpi parent .

        """
        category_targets = self._filter_out_sequence_targets(cat_targets)
        results_list = []
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.SHELF_COMPLIANCE)
        if assortment_df is not None:
            assortment_cat = assortment_df[assortment_df['category_fk'] == category]
            shelf_weight = category_targets['shelf_weight'].iloc[0]
            benchmark = category_targets['shelf_benchmark'].iloc[0]
            shelves = [int(shelf) for shelf in category_targets['shelf_number'].iloc[0].split(",")]
            shelf_df = assortment_cat[assortment_cat['shelf_number'].isin(shelves)]
            numerator = len(shelf_df['product_fk'].unique())
            denominator = len(assortment_cat['product_fk'].unique())
            result = float(numerator) / float(denominator) if numerator and denominator != 0 else 0
            score = shelf_weight if result >= benchmark else 0
        else:
            denominator, numerator, score, shelf_weight = 0, 0, 0, 0
            result = float(numerator) / float(denominator) if numerator and denominator != 0 else 0
            shelf_weight = category_targets['shelf_weight'].iloc[0]
        results_list.append({'fk': kpi_fk, 'numerator_id': category, 'denominator_id':
        self.store_id, 'denominator_result': denominator, 'numerator_result': numerator, 'result': result,
                         'target': shelf_weight, 'score': score,
                         'identifier_parent': identifier_parent, 'should_enter': True})
        return score, results_list, shelf_weight

    def planogram(self, category_fk, assortment, category_targets, identifier_parent):
        """
          This function sum sequence kpi and  shelf compliance
            :param category_fk
            :param category_targets : targets df for the specific category
            :param assortment :relevant assortment based on filtered scif
            :param identifier_parent : result identifier for planogram kpi parent .

        """
        results_list = []
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PLN_CATEGORY)
        identifier_result = self.common.get_dictionary(category_fk=category_fk, kpi_fk=kpi_fk)

        shelf_compliance_score, shelf_compliance_result, shelf_weight = self.shelf_compliance(category_fk, assortment,
                                                                                              category_targets,
                                                                                              identifier_result)
        results_list.extend(shelf_compliance_result)
        sequence_kpi, sequence_weight = self._calculate_sequence(category_targets, category_fk, identifier_result)
        planogram_score = shelf_compliance_score + sequence_kpi
        planogram_weight = shelf_weight + sequence_weight
        results_list.append({'fk': kpi_fk, 'numerator_id': category_fk, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': planogram_score,
                             'result': planogram_score,
                             'target': planogram_weight, 'score': planogram_score,
                             'identifier_parent': identifier_parent, 'identifier_result': identifier_result
                                , 'should_enter': True})
        return planogram_score, results_list

    def _calculate_sequence(self, targets, cat_fk, planogram_identifier):
        """
        This method calculated the sequence KPIs using the external targets' data and sequence calculation algorithm.
        """
        sequence_kpi_fk, sequence_sku_kpi_fk = self._get_sequence_kpi_fks()
        sequence_targets = targets.loc[targets['kpi_fk'] == sequence_kpi_fk]
        passed_sequences_score, total_weight, total_passed_counter = 0, 0, 0
        for i, sequence in sequence_targets.iterrows():
            population, location, sequence_attributes = self._prepare_data_for_sequence_calculation(sequence)
            sequence_result = self.sequence.calculate_sequence(population, location, sequence_attributes)
            score, weight = self._save_sequence_results_to_db(sequence_kpi_fk, sequence_sku_kpi_fk, sequence,
                                                              sequence_result)
            passed_sequences_score += score
            total_weight += weight
            total_passed_counter += 1 if score else 0
        self._save_sequence_main_level_to_db(sequence_kpi_fk, planogram_identifier, cat_fk, total_passed_counter,
                                             len(targets), total_weight)
        return passed_sequences_score, total_weight*10

    @staticmethod
    def _prepare_data_for_sequence_calculation(sequence_params):
        """
        This method gets the relevant targets per sequence and returns the sequence params for calculation.
        """
        population = {ProductsConsts.PRODUCT_FK: sequence_params[ProductsConsts.PRODUCT_FK]}
        location = {TemplatesConsts.TEMPLATE_GROUP: sequence_params[TemplatesConsts.TEMPLATE_GROUP]}
        additional_attributes = {AdditionalAttr.STRICT_MODE: sequence_params['strict_mode'],
                                 AdditionalAttr.INCLUDE_STACKING: sequence_params['include_stacking'],
                                 AdditionalAttr.CHECK_ALL_SEQUENCES: True}
        return population, location, additional_attributes

    def _extract_target_params(self, sequence_params):
        """
        This method extract the relevant category_fk and result value from the sequence parameters.
        """
        numerator_id = sequence_params[ProductsConsts.CATEGORY_FK]
        result_value = self.ps_data_provider.get_pks_of_result(sequence_params['sequence_name'])
        return numerator_id, result_value

    def _save_sequence_main_level_to_db(self, kpi_fk, planogram_identifier, cat_fk, num_res, den_res, weight):
        """
        This method saves the top sequence level to DB.
        """
        score = num_res / float(den_res) if den_res else 0
        result = weight if score >= Consts.GSK_BENCHMARK else 0
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=cat_fk, numerator_result=num_res,
                                       result=result, denominator_id=self.store_id, denominator_result=den_res,
                                       score=score, weight=weight, target=Consts.GSK_BENCHMARK,  should_enter=True,
                                       identifier_result=kpi_fk, identifier_parent=planogram_identifier)

    def _save_sequence_results_to_db(self, kpi_fk, parent_kpi_fk, sequence_params, sequence_results):
        """
        This method handles the saving of the SKU level sequence KPI.
        :param kpi_fk: Sequence SKU kpi fk.
        :param parent_kpi_fk: Total sequence score kpi fk.
        :param sequence_params: A dictionary with sequence params for the external targets.
        :param sequence_results: A DataFrame with the results that were received by the sequence algorithm.
        :return: The score that was saved (0 or 100 * weight).
        """
        category_fk, result_value = self._extract_target_params(sequence_params)
        num_of_sequences = len(sequence_results)
        target, weight = sequence_params[SessionResultsConsts.TARGET], sequence_params[SessionResultsConsts.WEIGHT]
        score = 100 * weight if len(sequence_results) >= target else 0
        self.common.write_to_db_result(fk=kpi_fk, numerator_id=category_fk, numerator_result=num_of_sequences,
                                       result=result_value, denominator_id=self.store_id, denominator_result=999,
                                       score=score, weight=weight, parent_fk=parent_kpi_fk, target=target,
                                       should_enter=True, identifier_parent=parent_kpi_fk,
                                       identifier_result=(kpi_fk, category_fk))
        return score, weight

    def _get_sequence_kpi_fks(self):
        """This method fetches the relevant sequence kpi fks"""
        sequence_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.SEQUENCE_KPI)
        sequence_sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.SEQUENCE_SKU_KPI)
        return sequence_kpi_fk, sequence_sku_kpi_fk

    def secondary_display(self, category_fk, cat_targets, identifier_parent, scif_df):
        """
            This function calculate secondary score -  0  or full weight if at least
            one of it's child kpis equal to weight.
            :param category_fk
            :param cat_targets : targets df for the specific category
            :param identifier_parent : result identifier for promo activation kpi parent .
            :param scif_df : scif filtered by promo activation settings

        """
        category_targets = self._filter_out_sequence_targets(cat_targets)
        results_list = []
        parent_kpi_name = 'display'
        weight = category_targets['display_weight'].iloc[0]
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.DISPLAY_SUMMARY)
        result_identifier = self.common.get_dictionary(category_fk=category_fk, kpi_fk=total_kpi_fk)

        dispenser_score, dispenser_res = self.display_distribution(Consts.DISPENSER_TARGET, category_fk,
                                                                   category_targets, result_identifier,
                                                                   Consts.DISPENSERS, parent_kpi_name, scif_df)
        counter_top_score, counter_top_res = self.display_distribution(Consts.COUNTER_TOP_TARGET, category_fk,
                                                                       category_targets, result_identifier,
                                                                       Consts.COUNTERTOP, parent_kpi_name, scif_df)
        standee_score, standee_res = self.display_distribution(Consts.STANDEE_TARGET, category_fk, category_targets,
                                                               result_identifier, Consts.STANDEE, parent_kpi_name,
                                                               scif_df)
        results_list.extend(dispenser_res)
        results_list.extend(counter_top_res)
        results_list.extend(standee_res)

        display_score = weight if (dispenser_score == weight) or (counter_top_score == weight) or (standee_score ==
                                                                                                   weight) else 0
        results_list.append({'fk': total_kpi_fk, 'numerator_id': category_fk, 'denominator_id': self.store_id,
                             'denominator_result': 1, 'numerator_result': display_score, 'result': display_score,
                             'target': weight, 'score': display_score, 'identifier_parent': identifier_parent,
                             'identifier_result': result_identifier, 'should_enter': True})

        return results_list, display_score

    def promo_activation(self, category_fk, cat_targets, identifier_parent, scif_df):
        """
            This function calculate promo activation score -  0  or full weight if at least
            one of it's child kpis equal to weight.
            :param category_fk
            :param cat_targets : targets df for the specific category
            :param identifier_parent : result identifier for promo activation kpi parent .
            :param scif_df : scif filtered by promo activation settings

        """
        category_targets = self._filter_out_sequence_targets(cat_targets)
        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.PROMO_SUMMARY)
        result_identifier = self.common.get_dictionary(category_fk=category_fk, kpi_fk=total_kpi_fk)
        results_list = []
        parent_kpi_name = 'promo'
        weight = category_targets['promo_weight'].iloc[0]

        hang_shell_score, hang_shell_res = self.display_distribution(Consts.HANGSELL, category_fk, category_targets,
                                                                     result_identifier, Consts.HANGSELL_KPI,
                                                                     parent_kpi_name, scif_df)
        top_shelf_score, top_shelf_res = self.display_distribution(Consts.TOP_SHELF, category_fk, category_targets,
                                                                   result_identifier, Consts.TOP_SHELF_KPI,
                                                                   parent_kpi_name,
                                                                   scif_df)
        results_list.extend(hang_shell_res)
        results_list.extend(top_shelf_res)
        promo_score = weight if (hang_shell_score == weight) or (top_shelf_score == weight) else 0

        results_list.append({'fk': total_kpi_fk, 'numerator_id': category_fk, 'denominator_id':
            self.store_id, 'denominator_result': 1, 'numerator_result': promo_score, 'result': promo_score,
                             'target': weight,
                             'score': promo_score, 'identifier_parent': identifier_parent,
                             'identifier_result': result_identifier, 'should_enter': True})

        return results_list, promo_score

    @staticmethod
    def _filter_out_sequence_targets(targets):
        """ This is a temporary function that filters out the Sequence targets.
        This is in order to support the current structure of the calculation.
        It will be removed in the future"""
        filtered_targets = targets.loc[~(targets.kpi_fk == 2475)]
        return filtered_targets

    def orange_score_category(self, assortment_category_res, fsos_category_res):
        """
        This function calculate orange score kpi by category. Settings are based on external targets and set up file.
        :param assortment_category_res :  array  of assortment results
        :param fsos_category_res : array  of facing sos by store results
        """
        results_list = []
        self.store_target()
        if self.targets.empty:
            return

        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Consts.ORANGE_SCORE_COMPLIANCE)
        fsos_json_global_results = self.extract_json_results_by_kpi(fsos_category_res, Consts.GLOBAL_FSOS_BY_CATEGORY)
        msl_json_global_results = self.extract_json_results_by_kpi(assortment_category_res, Consts.GLOBAL_DST_BY_CATEGORY)

        # scif after filtering it by set up file for each kpi
        scif_secondary = self.gsk_generator.tool_box.tests_by_template('secondary_display', self.scif,
                                                                       self.set_up_data)
        scif_promo = self.gsk_generator.tool_box.tests_by_template('promo', self.scif,
                                                                   self.set_up_data)
        categories = self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK].unique()
        assortment = self.msl_assortment('Distribution - SKU')

        for cat in categories:
            orange_score_result_identifier = self.common.get_dictionary(category_fk=cat, kpi_fk=total_kpi_fk)

            cat_targets = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == cat]

            msl_score, msl_results = self.msl_compliance_score(cat, msl_json_global_results, cat_targets,
                                                               orange_score_result_identifier)

            fsos_score, fsos_results = self.fsos_compliance_score(cat, fsos_json_global_results, cat_targets,
                                                                  orange_score_result_identifier)

            planogram_score, planogram_results = self.planogram(cat, assortment, cat_targets,
                                                                orange_score_result_identifier)

            secondary_display_res, secondary_score = self.secondary_display(cat, cat_targets,
                                                                            orange_score_result_identifier,
                                                                            scif_secondary)

            promo_activation_res, promo_score = self.promo_activation(cat, cat_targets,
                                                                      orange_score_result_identifier, scif_promo)

            compliance_category_score = promo_score + secondary_score + fsos_score + msl_score + planogram_score
            results_list.extend(
                msl_results + fsos_results + planogram_results + secondary_display_res + promo_activation_res)
            results_list.append({'fk': total_kpi_fk, 'numerator_id': self.manufacturer_fk, 'denominator_id': cat,
                                 'denominator_result': 1, 'numerator_result': compliance_category_score, 'result':
                                     compliance_category_score, 'score': compliance_category_score,
                                 'identifier_result': orange_score_result_identifier})
        return results_list
