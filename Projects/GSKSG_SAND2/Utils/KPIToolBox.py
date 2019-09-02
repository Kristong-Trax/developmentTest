from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from Projects.GSKSG_SAND2.Data.LocalConsts import Consts
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
        # assortment_category_dict,fsos_category_dict ='a','b'
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

    def msl_compliance_score(self, category, categories_results_json):
        weight = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == category]['msl_weight'].iloc[0]
        if category not in categories_results_json:
            return 0, weight
        dst_result = categories_results_json[category]
        result = dst_result * weight
        return result, weight

    def fsos_compliance_score(self, category, categories_results_json):
        """
               This function return json of keys- categories and values -  kpi result for category
               :param category: pk of category
               :param categories_results_json: type of the desired kpi
               :return category json :  number-category_fk,number-result
           """
        dst_result = categories_results_json[category]
        benchmark = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == category]['fsos_benchmark'].iloc[0]
        result = 0.1 if dst_result >= benchmark else 0
        return result, benchmark

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
        store_columns = [col for col in target_columns if [att for att in store_att if att in col]]
        for col in store_columns:
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
        store_param = self.targets[store_param] if store_param_val is None else [store_param]
        store_param = store_param.unique()
        for param in store_param:
            if self.store_info[param][0] is None:
                if self.targets.empty:
                    return
                else:
                    self.targets.drop(self.targets.index, inplace=True)
            self.targets = self.targets[
                (self.targets[store_param_val] == self.store_info[param][0].encode(GlobalConsts.HelperConsts.UTF8)) |
                (self.targets[store_param_val] == '')]

    def display_distribution(self, display_name):
        display_products = self.scif[self.scif['product_type'] == 'POSM']
        display_products = display_products[display_products['additional_attribute_1'] == display_name]
        display_sku_level = self.display_sku_results(display_products)

    def display_sku_results(self, display_data):
        results_list = []
        display_names = display_data['item_id'].unique()
        for display in display_names:
            count = display_data[display_data['item_id'] == display]['facings'].count()
           # self.db_write()
        return results_list

    def assortment(self):
        # self _msl_assortment() gskjp
        lvl3_assort, filter_scif = self.gsk_generator.tool_box.get_assortment_filtered(self.gsk_generator.tool_box.
                                                                                       set_up_data, Consts.DISTRIBUTION)
        #merge all products
        return lvl3_assort

    def shelf_compliance(self, category, assortment_df):
        # I need to extract assortment level3 and than for the specific  category. count how many are in a specific shelf
        policy = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == category]
        assortment_cat = assortment_df[assortment_df['category_fk'] == category]
        weight = policy['shelf_weight'].iloc[0]
        benchmark = policy['shelf_benchmark'].iloc[0]
        shelves = [int(shelf) for shelf in policy['shelf_number'].iloc[0].split(",")]
        shelf_df = assortment_cat[assortment_cat['shelf_number'].isin(shelves)]
        numerator = shelf_df.shape[0]
        denominator = assortment_cat.shape[0]
        result = float(numerator) / float(denominator)
        score = weight if result >= benchmark else 0
        return score, weight

    def orange_score_category(self, assortment_category_dict, fsos_category_dict):
        self.store_target()
        if self.targets.empty:
            return
        fsos_results = self.extract_json_results_by_kpi(fsos_category_dict, Consts.GLOBAL_FSOS_BY_CATEGORY)
        msl_results = self.extract_json_results_by_kpi(assortment_category_dict, Consts.GLOBAL_DST_BY_CATEGORY)
        categories = self.scif[DataProviderConsts.ProductsConsts.CATEGORY_FK].unique()
        assortment = self.assortment()
        for cat in categories:
            # msl_score = self.msl_compliance_score(cat, msl_results)
            # fsos_score = self.fsos_compliance_score(cat, fsos_results)
            # secondery_display_score = 0 #display distrbution
            # promotion_activation_score = 0 #display distrbution
            # pln_summary_category= 0 # (shelf_compliance ) +sequence compliance
            pln_compliance = self.shelf_compliance(cat, assortment)

        return []
