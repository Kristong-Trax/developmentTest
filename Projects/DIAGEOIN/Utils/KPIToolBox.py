import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames, Consts
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils.DIAGEO.ParseTemplates import parse_template
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.Queries import Queries
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers

__author__ = 'satya'


class DIAGEOINToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.commonV2 = CommonV2(self.data_provider)
        self.rds_conn = PSProjectConnector(self.data_provider.project_name, DbUsers.CalculationEng)
        self.output = output
        self.kpi_static_data = self.get_kpi_static_data()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_fk = 12
        self.store_info = self.get_store_info(self.store_id)
        self.assortment = self.get_assortment_info()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.common = Common(self.data_provider)
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.template_handler = TemplateHandler(self.data_provider.project_name)
        local_template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                                 'Data', 'Template.xlsx')
        self.bpa_store = parse_template(local_template_path, sheet_name='brand_presence_assortments_store')
        self.bpa = parse_template(local_template_path, 'brand_presence_assortments')

    def get_kpi_static_data(self):
        """
        This function extracts the static new KPI data (new tables) and saves it into one 'global' data frame.
        The data is taken from static.kpi_level_2.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_assortment_info(self):
        query = """SELECT pk, assortment_name FROM pservice.assortment"""

        return self.perform_query(query)

    def get_store_info(self, store_id):
        query = """SELECT st.name as store_name, st.pk as store_fk, st.store_number_1, st.sales_rep_name, st.store_type,
                           st.distribution_type, st.address_city,
                           st.customer_name,
                           st.manager_name,
                           st.comment as store_comment,
                           st.test_store,
                           sta.name state,
                           st.additional_attribute_1, st.additional_attribute_2, st.additional_attribute_3,
                           st.additional_attribute_4, st.additional_attribute_5, st.additional_attribute_6,
                           st.additional_attribute_7, st.additional_attribute_8, st.additional_attribute_9,
                           st.additional_attribute_10, st.additional_attribute_11, st.additional_attribute_12,
                           st.additional_attribute_13, st.additional_attribute_14, st.additional_attribute_15,
                           rg.name as region_name, rg.code as region_code, rg.remarks as region_remarks, st.region_fk,
                           st.branch_fk, st.retailer_fk, state_fk, r.name as retailer_name
                    FROM static.stores st
                    JOIN static.regions rg on rg.pk = st.region_fk
                    LEFT join static.retailer r on r.pk = st.retailer_fk
                    LEFT join static.state sta on sta.pk= state_fk
                    WHERE st.pk = {0};
                """.format(store_id)
        return self.perform_query(query)

    def perform_query(self, query, **kwargs):
        """

        This function creates a :class: `pandas.DataFrame` using a given cursor\n
        :param str query: The query
        :return: A newly created :class:`pandas.DataFrame` object with the results of the query
        """
        df = self.data_provider.run_query(query)
        return df

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """

        # # SOS Out Of The Box KPIs
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # Global assortment KPIs - v2 for API use
        assortment_res_dict_v2 = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v2)

        # Global assortment KPIs - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        # Global Secondary Displays function
        res_json = self.diageo_generator.diageo_global_secondary_display_secondary_function()
        if res_json:
            self.commonV2.write_to_db_result(fk=res_json['fk'], numerator_id=1, denominator_id=self.store_id,
                                             result=res_json['result'])
        # Local Custom/Client Brand Group Presence
        self.custom_brand_presence()

        # # Global Brand Blocking KPI
        # template_data = self.template_handler.download_template(DiageoKpiNames.BRAND_BLOCKING)
        # results_list = self.diageo_generator.diageo_global_block_together(DiageoKpiNames.BRAND_BLOCKING, template_data)
        # self.save_results_to_db(results_list)

        # committing to new tables
        self.commonV2.commit_results_data()
        # committing to the old tables
        #self.common.commit_results_data()

    def save_results_to_db(self, results_list):
        if results_list:
            for result in results_list:
                if result is not None:
                    self.commonV2.write_to_db_result(**result)

    def get_store_policy(self):

        store_policy = ""

        for row_num, row_data in self.bpa_store.iterrows():
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
                Log.info("store_policy:{} filter_params:{}".format(row_data['store_policy'], filter_params))
                return str(row_data['store_policy']).strip()

        return store_policy

    def custom_brand_presence(self):

        store_policy = self.get_store_policy().strip()

        if len(store_policy) == 0:
            message = "store_policy not available for store_fk:{}".format(self.store_id)
            Log.info(message)
            return

        brand_group_list = []
        for row_num_bpa, row_data_bpa in self.bpa.iterrows():
            store_policies = [x.strip() for x in row_data_bpa['store_policy'].split(',')]
            if store_policy in store_policies:
                brand_group_list.append(dict(row_data_bpa))

        brand_groups = pd.DataFrame(brand_group_list)

        if brand_groups.empty:
            Log.info("No brand groups for the policy:{}".format(store_policy))
            return

        brand_group_names = brand_groups['brand_group_name'].drop_duplicates()

        list_results = []
        assortment_fk = 0

        for brand_group_name in brand_group_names:
            brand_group = brand_groups[brand_groups['brand_group_name'] == brand_group_name]
            dict_result = dict()

            for row_num_brand_group, row_data_brand_group in brand_group.iterrows():
                assortment = self.assortment[self.assortment['assortment_name'] == row_data_brand_group['brand_group_name']]
                if assortment.empty:
                    continue

                assortment_fk = assortment.iloc[0]['pk']
                template_name = row_data_brand_group['scene_type']

                df = self.scif[(self.scif['template_name'] == template_name) & (self.scif['facings'] > 0)]
                if df.empty:
                    template_fk = 0
                else:
                    template_fk = df.iloc[0]['template_fk']

                ean_codes_found = []
                ean_codes = [x.strip() for x in row_data_brand_group['ean_code_list'].split(',')]
                for row_num_2, row_data_2 in df.iterrows():
                    for ean_code in ean_codes:
                        if row_data_2['product_ean_code'] == ean_code:
                            ean_codes_found.append(ean_code)

                result = 0
                if len(ean_codes_found) >= int(row_data_brand_group['target']):
                    result = 1

                dict_result['fk'] = self.get_kpi_fk_by_kpi_type("BRAND_GROUP_PRESENCE_SCENE_LEVEL")
                if template_name == 'Back Bar':
                    dict_result['numerator_id'] = template_fk
                    dict_result['numerator_result'] = result
                elif template_name == 'Menu':
                    dict_result['denominator_id'] = template_fk
                    dict_result['denominator_result'] = result

            if len(dict_result.keys())>0:
                # below code is added when Menu is found and Back Bar scene is missing or vice-versa
                dict_result['numerator_id'] = 0 if 'numerator_id' not in dict_result.keys() else dict_result[
                    'numerator_id']
                dict_result['numerator_result'] = 0 if 'numerator_result' not in dict_result.keys() else dict_result[
                    'numerator_result']
                dict_result['denominator_id'] = 0 if 'denominator_id' not in dict_result.keys() else dict_result[
                    'denominator_id']
                dict_result['denominator_result'] = 0 if 'denominator_result' not in dict_result.keys() else \
                dict_result['denominator_result']

                net_result = int(dict_result['numerator_result']) + int(dict_result['denominator_result'])
                net_result = 1 if net_result > 1 else net_result

                dict_result['context_id'] = assortment_fk
                dict_result['result'] = net_result
                dict_result['score'] = net_result
                list_results.append(dict_result)

        df_results = pd.DataFrame(list_results)

        self.calculate_brand_presence_overall_score(df_results)

        for row_num_results, row_data_results in df_results.iterrows():
            self.commonV2.write_to_db_result(fk=row_data_results['fk'],
                                             numerator_id=row_data_results['numerator_id'],
                                             numerator_result=row_data_results['numerator_result'],
                                             denominator_id=row_data_results['denominator_id'],
                                             denominator_result=row_data_results['denominator_result'],
                                             context_id=row_data_results['context_id'],
                                             score=row_data_results['result'],
                                             result=row_data_results['result'])

        print df_results

    def get_kpi_fk_by_kpi_type(self, kpi_type):
        """
        convert kpi name to kpi_fk
        :param kpi_type: string
        :return: fk
        """
        assert isinstance(kpi_type, (unicode, basestring)), "name is not a string: %r" % kpi_type
        try:
            return self.kpi_static_data[self.kpi_static_data['type'] == kpi_type]['pk'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_type))
            return None

    def calculate_brand_presence_overall_score(self, df):

        if df.empty:
            return

        kpi_fk = self.get_kpi_fk_by_kpi_type("BRAND_GROUP_PRESENCE_OWN_MANF_WHOLE_STORE")
        numerator_id = self.own_manufacturer_fk
        numerator_result = df['result'].sum()
        denominator_id = self.store_id
        denominator_result = df.shape[0]
        score = 0.0

        try:
            score = numerator_result / float(denominator_result)
        except Exception as ex:
            Log.info("Warning: {}".format(ex.message))

        self.commonV2.write_to_db_result(fk=kpi_fk,
                                         numerator_id=numerator_id,
                                         numerator_result=numerator_result,
                                         denominator_id=denominator_id,
                                         denominator_result=denominator_result,
                                         score=score,
                                         result=score)
