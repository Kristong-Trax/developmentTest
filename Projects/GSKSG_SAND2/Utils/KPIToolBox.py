from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from Projects.GSKSG_SAND2.Data.LocalConsts import Consts
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Utils.Consts import GlobalConsts, DataProvider as DataProviderConsts ,DB

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

        # assortment_category_dict = self.gsk_generator.availability_category_function()
        # self.common.save_json_to_new_tables(assortment_category_dict)
        # fsos_category_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        # self.common.save_json_to_new_tables(fsos_category_dict)
        assortment_category_dict,fsos_category_dict ='a','b'
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
        dst_result = categories_results_json[category]
        weight = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == category]['msl_weight'].iloc[0]
        result = dst_result * weight
        return result, weight

    def fsos_compliance_score(self, category, categories_results_json):
        dst_result = categories_results_json[category]
        benchmark = self.targets[self.targets[DataProviderConsts.ProductsConsts.CATEGORY_FK] == category]['fsos_benchmark'].iloc[0]
        result = 0.1 if dst_result >= benchmark else 0
        return result, benchmark

    def extract_json_results_by_kpi(self, assortment_category_dict, kpi_type):
        kpi_fk = self.common.get_kpi_fk_by_kpi_type('GSK_Dst_Manufacturer_By_Category')
        categories_results_json = self.extract_json_results(kpi_fk, assortment_category_dict)
        return categories_results_json

    @staticmethod
    def extract_json_results(kpi_fk, assortment_category_dict):
        category_json = {}
        for row in assortment_category_dict:
            if row['fk'] == kpi_fk:
                category_json[row[DB.SessionResultsConsts.DENOMINATOR_ID]] = row[DB.SessionResultsConsts.RESULT]
        return category_json

    def store_target(self):
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
        store_param_val = store_param_val if store_param_val is not None else store_param
        store_param = self.targets[store_param] if store_param_val is not None else store_param
        store_param.values.unique()
        for param in store_param:
            if self.store_info[param][0] is None:
                if self.targets.empty:
                    return
                else:
                    self.targets.drop(self.targets.index, inplace=True)
            self.targets = self.targets[
                (self.targets[store_param_val] == self.store_info[param][0].encode(GlobalConsts.HelperConsts.UTF8)) |
                (self.targets[store_param_val] == '')]

    def orange_score_category(self, assortment_category_dict, fsos_category_dict):
        self.store_target()
        if self.targets.empty:
            return
        fsos_results = self.extract_json_results_by_kpi(fsos_category_dict, Consts.GLOBAL_FSOS_BY_CATEGORY)
        msl_results = self.extract_json_results_by_kpi(assortment_category_dict, Consts.GLOBAL_DST_BY_CATEGORY)
        categories = self.scif[DataProviderConsts.ProductsConsts.CATEGORY_FK].unique()
        for cat in categories:
            msl_score = self.msl_compliance_score(cat, msl_results)
            fsos_score = self.fsos_compliance_score(cat, fsos_results)

        return []
