
import os
import pandas as pd

from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils.GlobalProjects.GSK.Data.LocalConsts import Consts as GlobalConsts


from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts
from Trax.Data.ProfessionalServices.PsConsts.DB import SessionResultsConsts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from Projects.GSKRU.Utils.LocalConsts import Consts as LocalConsts


__author__ = 'sergey'


class GSKRUToolBox:

    def __init__(self, data_provider, output):

        self.output = output
        self.data_provider = data_provider
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
        self.own_manufacturer_id = int(self.data_provider.own_manufacturer[
                                           self.data_provider.own_manufacturer['param_name'] ==
                                           'manufacturer_id']['param_value'].iloc[0])

        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)

        self.common = Common(self.data_provider)
        self.kpi_static_data = self.common.get_kpi_static_data()

        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_type = self.ps_data_provider.session_info.store_type
        self.store_format = self.ps_data_provider.session_info.additional_attribute_12.encode('utf-8')
        self.retailer_fk = self.ps_data_provider.session_info.retailer_fk

        self.set_up_template = None
        self.gsk_generator = None
        self.core_range_targets = {}

        self.set_up_data = LocalConsts.SET_UP_DATA
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs All Store',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        # Global KPIs

        # All Store KPIs
        assortment_store_dict = self.gsk_generator.availability_store_function(
            custom_suffix='_Stacking_Included')
        self.common.save_json_to_new_tables(assortment_store_dict)

        assortment_category_dict = self.gsk_generator.availability_category_function(
            custom_suffix='_Stacking_Included')
        self.common.save_json_to_new_tables(assortment_category_dict)

        assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function(
            custom_suffix='_Stacking_Included')
        self.common.save_json_to_new_tables(assortment_subcategory_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function(
            custom_suffix='_Stacking_Included',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function(
            custom_suffix='_Stacking_Included',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function(
            custom_suffix='_Stacking_Included',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        self.common.save_json_to_new_tables(linear_sos_dict)

        # Main Shelf KPIs
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Main Shelf',
                                             keep_default_na=False)
        self.gsk_generator.set_up_file = self.set_up_template
        self.gsk_generator.tool_box.set_up_file = self.gsk_generator.set_up_file
        self.gsk_generator.tool_box.set_up_data = LocalConsts.SET_UP_DATA.copy()
        # self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function(
            custom_suffix='_Stacking_Included_Main_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function(
            custom_suffix='_Stacking_Included_Main_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function(
            custom_suffix='_Stacking_Included_Main_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function(
            custom_suffix='_Main_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function(
            custom_suffix='_Main_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function(
            custom_suffix='_Main_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        # Secondary Shelf KPIs
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Secondary Shelf',
                                             keep_default_na=False)
        self.gsk_generator.set_up_file = self.set_up_template
        self.gsk_generator.tool_box.set_up_file = self.gsk_generator.set_up_file
        self.gsk_generator.tool_box.set_up_data = LocalConsts.SET_UP_DATA.copy()
        # self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function(
            custom_suffix='_Stacking_Included_Secondary_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function(
            custom_suffix='_Stacking_Included_Secondary_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function(
            custom_suffix='_Stacking_Included_Secondary_Shelf',
            fractional_facings_parameters=LocalConsts.FRACTIONAL_FACINGS_PARAMETERS)
        self.common.save_json_to_new_tables(facings_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function(
            custom_suffix='_Secondary_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function(
            custom_suffix='_Secondary_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function(
            custom_suffix='_Secondary_Shelf')
        self.common.save_json_to_new_tables(linear_sos_dict)

        # Local KPIs
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Local',
                                             keep_default_na=False)
        self.gsk_generator.set_up_file = self.set_up_template
        self.gsk_generator.tool_box.set_up_file = self.gsk_generator.set_up_file
        self.gsk_generator.tool_box.set_up_data = LocalConsts.SET_UP_DATA.copy()
        # self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

        # SOA
        soa_dict = self.gsk_soa_function()
        self.common.save_json_to_new_tables(soa_dict)

        # # Core Range Assortment
        # cra_dict = self.gsk_cra_function()
        # self.common.save_json_to_new_tables(cra_dict)

        self.common.commit_results_data()

        return

    def gsk_soa_function(self):

        results = []

        kpi_soa_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_KPI)
        kpi_soa_manufacturer_internal_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_MANUFACTURER_INTERNAL_TARGET_KPI)
        kpi_soa_manufacturer_external_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_MANUFACTURER_EXTERNAL_TARGET_KPI)
        kpi_soa_subcat_internal_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_SUBCAT_INTERNAL_TARGET_KPI)
        kpi_soa_subcat_external_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_SUBCAT_EXTERNAL_TARGET_KPI)

        identifier_internal = self.common.get_dictionary(manufacturer_fk=self.own_manufacturer_id,
                                                         kpi_fk=kpi_soa_manufacturer_internal_target_fk)
        identifier_external = self.common.get_dictionary(manufacturer_fk=self.own_manufacturer_id,
                                                         kpi_fk=kpi_soa_manufacturer_external_target_fk)

        targets = \
            self.ps_data_provider.get_kpi_external_targets(kpi_fks=[kpi_soa_fk],
                                                           key_filters={'additional_attribute_12': self.store_format,
                                                                        'retailer_fk': self.retailer_fk})

        if targets.empty:
            Log.warning('No SOA targets defined for this session')
        else:

            self.gsk_generator.tool_box. \
                extract_data_set_up_file(LocalConsts.SOA, self.set_up_data, LocalConsts.KPI_DICT)
            df = self.gsk_generator.tool_box.tests_by_template(LocalConsts.SOA, self.scif, self.set_up_data)
            df, facings_column = self.df_filter_by_stacking(df, LocalConsts.SOA)

            # Sub-Category
            for sub_category_fk in df[ScifConsts.SUB_CATEGORY_FK].unique().tolist():

                numerator_result = len(df[(df[ScifConsts.MANUFACTURER_FK] == self.own_manufacturer_id) &
                                          (df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk)][
                                           ScifConsts.PRODUCT_FK].unique().tolist())
                denominator_result = len(df[df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk][
                                             ScifConsts.PRODUCT_FK].unique().tolist())
                result = round(float(numerator_result) / float(denominator_result), 4) \
                    if numerator_result != 0 and denominator_result != 0 \
                    else 0

                target = targets[targets['sub_category_fk'] == sub_category_fk]['internal_target'].values
                target = float(target[0]) if len(target) > 0 else None
                target = target/100 if target else None
                if target:
                    # score = 1 if result >= target else 0
                    score = round(result/target, 4)
                else:
                    score = None
                results.append(
                    {'fk': kpi_soa_subcat_internal_target_fk,
                     SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                     SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                     SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                     SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                     SessionResultsConsts.CONTEXT_ID: sub_category_fk,
                     SessionResultsConsts.RESULT: result,
                     SessionResultsConsts.TARGET: target,
                     SessionResultsConsts.SCORE: score,
                     'identifier_parent': identifier_internal,
                     'should_enter': True})

                self.core_range_targets.update({sub_category_fk: target})

                target = targets[targets['sub_category_fk'] == sub_category_fk]['external_target'].values
                target = float(target[0]) if len(target) > 0 else None
                target = target/100 if target else None
                if target:
                    # score = 1 if result >= target else 0
                    score = round(result/target, 4)
                else:
                    score = None
                results.append(
                    {'fk': kpi_soa_subcat_external_target_fk,
                     SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                     SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                     SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                     SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                     SessionResultsConsts.CONTEXT_ID: sub_category_fk,
                     SessionResultsConsts.RESULT: result,
                     SessionResultsConsts.TARGET: target,
                     SessionResultsConsts.SCORE: score,
                     'identifier_parent': identifier_external,
                     'should_enter': True})

            # Manufacturer
            numerator_result = len(df[df[ScifConsts.MANUFACTURER_FK] == self.own_manufacturer_id][
                                       ScifConsts.PRODUCT_FK].unique().tolist())
            denominator_result = len(df[ScifConsts.PRODUCT_FK].unique().tolist())
            result = round(float(numerator_result) / float(denominator_result), 4) \
                if numerator_result != 0 and denominator_result != 0 \
                else 0

            target = targets[targets['sub_category_fk'].isnull()]['internal_target'].values
            target = float(target[0]) if len(target) > 0 else None
            target = target/100 if target else None
            if target:
                # score = 1 if result >= target else 0
                score = round(result/target, 4)
            else:
                score = None
            results.append(
                {'fk': kpi_soa_manufacturer_internal_target_fk,
                 SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                 SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                 SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                 SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                 SessionResultsConsts.RESULT: result,
                 SessionResultsConsts.TARGET: target,
                 SessionResultsConsts.SCORE: score,
                 'identifier_result': identifier_internal,
                 'should_enter': True})

            target = targets[targets['sub_category_fk'].isnull()]['external_target'].values
            target = float(target[0]) if len(target) > 0 else None
            target = target/100 if target else None
            if target:
                # score = 1 if result >= target else 0
                score = round(result/target, 4)
            else:
                score = None
            results.append(
                {'fk': kpi_soa_manufacturer_external_target_fk,
                 SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                 SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                 SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                 SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                 SessionResultsConsts.RESULT: result,
                 SessionResultsConsts.TARGET: target,
                 SessionResultsConsts.SCORE: score,
                 'identifier_result': identifier_external,
                 'should_enter': True})

        return results

    def gsk_cra_function(self):

        results = []

        kpi_cra_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.CRA_KPI)
        kpi_cra_manufacturer_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.CRA_MANUFACTURER_KPI)
        kpi_cra_subcat_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.CRA_SUBCAT_KPI)
        kpi_cra_subcat_by_product_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.CRA_SUBCAT_BY_PRODUCT_KPI)

        identifier_manufacturer = self.common.get_dictionary(manufacturer_fk=self.own_manufacturer_id,
                                                             kpi_fk=kpi_cra_manufacturer_fk)

        total_cra_size_target = 0
        total_cra_size_actual = 0

        targets = \
            self.ps_data_provider.get_kpi_external_targets(kpi_fks=[kpi_cra_fk],
                                                           key_filters={'additional_attribute_12': self.store_format,
                                                                        'retailer_fk': self.retailer_fk})

        if targets.empty:
            Log.warning('No CRA targets defined for this session')
        else:

            self.gsk_generator.tool_box. \
                extract_data_set_up_file(LocalConsts.CRA, self.set_up_data, LocalConsts.KPI_DICT)
            df = self.gsk_generator.tool_box.tests_by_template(LocalConsts.CRA, self.scif, self.set_up_data)
            df, facings_column = self.df_filter_by_stacking(df, LocalConsts.CRA)

            df = df[df[ScifConsts.SUB_CATEGORY_FK].notnull()][
                [ScifConsts.SUB_CATEGORY_FK, ScifConsts.PRODUCT_FK, facings_column]]\
                .groupby([ScifConsts.SUB_CATEGORY_FK, ScifConsts.PRODUCT_FK]).agg({facings_column: 'sum'})\
                .reset_index()
            df = df.merge(targets[['sub_category_fk', 'product_fk', 'priority']],
                          how='left',
                          left_on=[ScifConsts.SUB_CATEGORY_FK, ScifConsts.PRODUCT_FK],
                          right_on=['sub_category_fk', 'product_fk'])
            df['unique_product_id'] = \
                df.apply(lambda r:
                         'P' + str(r['priority']) if pd.notnull(r['priority']) else 'N' + str(r['product_fk']), axis=1)

            # Sub-Category
            target_subcat_fks = set(targets['sub_category_fk'].unique().tolist()) & set(self.core_range_targets.keys())
            for sub_category_fk in target_subcat_fks:

                identifier_subcat = self.common.get_dictionary(manufacturer_fk=self.own_manufacturer_id,
                                                               sub_category_fk=sub_category_fk,
                                                               kpi_fk=kpi_cra_subcat_fk)

                if sub_category_fk not in self.core_range_targets.keys():
                    numerator_result = denominator_result = result = score = 0
                else:
                    subcat_size = len(df[df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk][
                                          'unique_product_id'].unique().tolist())
                    core_range_target = self.core_range_targets[sub_category_fk]
                    cra_priority = round(subcat_size * core_range_target if core_range_target else 0)

                    cra_products_target = targets[(targets['sub_category_fk'] == sub_category_fk) &
                                                  (targets['priority'] <= cra_priority)][
                        ['product_fk', 'priority']]
                    cra_products_actual = df[(df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk) &
                                             (df['priority'] <= cra_priority)][
                        [ScifConsts.PRODUCT_FK, facings_column]]

                    cra_size_target = len(targets[(targets['sub_category_fk'] == sub_category_fk) &
                                                  (targets['priority'] <= cra_priority)]['priority'].unique().tolist())
                    cra_size_actual = len(df[(df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk) &
                                             (df['priority'] <= cra_priority)]['priority'].unique().tolist())

                    if cra_size_target == 0:
                        numerator_result = denominator_result = result = score = 0
                    else:

                        # Product
                        for i, product in cra_products_target.iterrows():

                            numerator_result = \
                                cra_products_actual[cra_products_actual[ScifConsts.PRODUCT_FK] ==
                                                    product['product_fk']][facings_column].sum()
                            denominator_result = product['priority']
                            result = 1 if numerator_result else 0
                            score = result

                            results.append(
                                {'fk': kpi_cra_subcat_by_product_fk,
                                 SessionResultsConsts.NUMERATOR_ID: product['product_fk'],
                                 SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                                 SessionResultsConsts.DENOMINATOR_ID: self.own_manufacturer_id,
                                 SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                                 SessionResultsConsts.CONTEXT_ID: sub_category_fk,
                                 SessionResultsConsts.RESULT: result,
                                 SessionResultsConsts.SCORE: score,
                                 'identifier_parent': identifier_subcat,
                                 'should_enter': True})

                        numerator_result = cra_size_actual
                        denominator_result = cra_size_target
                        result = round(float(numerator_result) / float(denominator_result), 4) \
                            if numerator_result != 0 and denominator_result != 0 \
                            else 0
                        score = result

                        total_cra_size_target += cra_size_target
                        total_cra_size_actual += cra_size_actual

                results.append(
                    {'fk': kpi_cra_subcat_fk,
                     SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                     SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                     SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                     SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                     SessionResultsConsts.CONTEXT_ID: sub_category_fk,
                     SessionResultsConsts.RESULT: result,
                     SessionResultsConsts.SCORE: score,
                     'identifier_parent': identifier_manufacturer,
                     'identifier_result': identifier_subcat,
                     'should_enter': True})

            # Manufacturer
            if target_subcat_fks:
                numerator_result = total_cra_size_actual
                denominator_result = total_cra_size_target
                result = round(float(total_cra_size_actual) / float(total_cra_size_target), 4) \
                    if numerator_result != 0 and denominator_result != 0 \
                    else 0
                score = result

                results.append(
                    {'fk': kpi_cra_manufacturer_fk,
                     SessionResultsConsts.NUMERATOR_ID: self.own_manufacturer_id,
                     SessionResultsConsts.NUMERATOR_RESULT: numerator_result,
                     SessionResultsConsts.DENOMINATOR_ID: self.store_id,
                     SessionResultsConsts.DENOMINATOR_RESULT: denominator_result,
                     SessionResultsConsts.RESULT: result,
                     SessionResultsConsts.SCORE: score,
                     'identifier_result': identifier_manufacturer,
                     'should_enter': True})

        return results

    def df_filter_by_stacking(self, df, kpi_type):
        include_stacking = self.set_up_data.get((GlobalConsts.INCLUDE_STACKING, kpi_type), True)
        facings_column = ScifConsts.FACINGS
        if not include_stacking:
            facings_column = ScifConsts.FACINGS_IGN_STACK
        df = df[df[facings_column] > 0]
        return df, facings_column
