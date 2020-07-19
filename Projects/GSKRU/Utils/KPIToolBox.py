
import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator

from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ProductsConsts, ScifConsts, StoreInfoConsts
from Trax.Data.ProfessionalServices.PsConsts.DB import SessionResultsConsts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from Projects.GSKRU.Utils.LocalConsts import Consts as LocalConsts


__author__ = 'sergey'


class GSKRUToolBox:

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
        self.set_up_template = None
        self.gsk_generator = None

        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.own_manufacturer_id = self.get_own_manufacturer_id()
        self.set_up_data = LocalConsts.SET_UP_DATA
        self.store_info = self.get_store_info()
        self.store_type = self.store_info['store_type'][0]
        self.retailer_name = self.store_info['retailer_name'][0]
        self.retailer_fk = self.store_info['retailer_fk'][0]

    def get_own_manufacturer_id(self):
        return int(self.data_provider.own_manufacturer[self.data_provider.own_manufacturer['param_name'] ==
                                                       'manufacturer_id']['param_value'].iloc[0])

    def get_store_info(self):
        query = """
                    SELECT 
                    s.pk as store_fk, 
                    s.store_type_fk, tp.name as store_type,
                    s.additional_attribute_17, 
                    s.additional_attribute_3, 
                    s.retailer_fk, r.name as retailer_name,
                    s.store_number_1,
                    s.address_line_1 as address_line_1,
                    s.business_unit_fk, 
                    s.additional_attribute_5, 
                    s.district_fk, 
                    s.test_store,
                    bu.name as business_unit,
                    br.name as branch_name,
                    re.name as region,
                    cn.name as country, 
                    dis.name as dist_name, 
                    dis.code as dis_code, 
                    st.name as state, 
                    st.code as state_code
                    FROM static.stores s
                    left join static.retailer r
                    on r.pk = s.retailer_fk
                    left join static.business_unit bu
                    on bu.pk = s.business_unit_fk
                    left join static.branch br 
                    on br.pk = s.branch_fk
                    left join static.regions re on re.pk = s.region_fk
                    left join static.countries cn ON re.country_fk = cn.pk
                    left join static.district dis on dis.pk = s.district_fk
                    left join static.state st on st.pk = s.state_fk
                    left join static.store_type tp on tp.pk=s.store_type_fk
                    where s.pk = '{}'
                """.format(self.store_id)
        store_data = pd.read_sql_query(query, self.rds_conn.db)
        return store_data

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        # Global KPIs

        # All Store KPIs
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs All Store',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

        assortment_store_dict = self.gsk_generator.availability_store_function(
            custom_suffix='_Include_stacking')
        self.common.save_json_to_new_tables(assortment_store_dict)

        assortment_category_dict = self.gsk_generator.availability_category_function(
            custom_suffix='_Include_stacking')
        self.common.save_json_to_new_tables(assortment_category_dict)

        assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function(
            custom_suffix='_Include_stacking')
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
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Main Shelf',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

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
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Secondary Shelf',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

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
        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'),
                                             sheet_name='Functional KPIs Local',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

        # SOA
        soa_dict = self.gsk_soa_function()
        self.common.save_json_to_new_tables(soa_dict)

        self.common.commit_results_data()

        return

    def gsk_soa_function(self):

        results = []

        targets = self.ps_data_provider.get_kpi_external_targets(kpi_operation_types=['SOA'],
                                                                 key_filters={'store_type': self.store_type,
                                                                              'retailer_fk': self.retailer_fk})

        kpi_soa_manufacturer_internal_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_MANUFACTURER_INTERNAL_TARGET_KPI)
        kpi_soa_manufacturer_external_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_MANUFACTURER_EXTERNAL_TARGET_KPI)
        kpi_soa_subcat_internal_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_SUBCAT_INTERNAL_TARGET_KPI)
        kpi_soa_subcat_external_target_fk = \
            self.common.get_kpi_fk_by_kpi_type(LocalConsts.SOA_SUBCAT_EXTERNAL_TARGET_KPI)

        identifier_internal = 'int'
        identifier_external = 'ext'

        self.gsk_generator.tool_box. \
            extract_data_set_up_file(LocalConsts.SOA, self.set_up_data, LocalConsts.KPI_DICT)
        df = self.gsk_generator.tool_box.tests_by_template(LocalConsts.SOA, self.scif, self.set_up_data)

        # Manufacturer
        numerator_result = len(df[df[ScifConsts.MANUFACTURER_FK] == self.own_manufacturer_id][
                                   ScifConsts.PRODUCT_FK].unique().tolist())
        denominator_result = len(df[ScifConsts.PRODUCT_FK].unique().tolist())
        result = round(float(numerator_result) / float(denominator_result), 4)

        target = targets[targets['sub_category_fk'].isnull()]['internal_target'].values
        target = float(target[0]) if target else None
        target = target/100 if target else None
        if target:
            score = 1 if result >= target else 0
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
        target = float(target[0]) if target else None
        target = target/100 if target else None
        if target:
            score = 1 if result >= target else 0
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

        # Sub-Category
        for sub_category_fk in df[ScifConsts.SUB_CATEGORY_FK].unique().tolist():

            numerator_result = len(df[(df[ScifConsts.MANUFACTURER_FK] == self.own_manufacturer_id) &
                                      (df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk)][
                                       ScifConsts.PRODUCT_FK].unique().tolist())
            denominator_result = len(df[df[ScifConsts.SUB_CATEGORY_FK] == sub_category_fk][
                                         ScifConsts.PRODUCT_FK].unique().tolist())
            result = round(float(numerator_result) / float(denominator_result), 4)

            target = targets[targets['sub_category_fk'] == sub_category_fk]['internal_target'].values
            target = float(target[0]) if target else None
            target = target/100 if target else None
            if target:
                score = 1 if result >= target else 0
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

            target = targets[targets['sub_category_fk'] == sub_category_fk]['external_target'].values
            target = float(target[0]) if target else None
            target = target/100 if target else None
            if target:
                score = 1 if result >= target else 0
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

        return results
