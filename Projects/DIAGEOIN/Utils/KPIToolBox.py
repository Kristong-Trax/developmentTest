import os
import pandas as pd
import numpy as np

from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Keys import DbUsers

from KPIUtils.DIAGEO.ParseTemplates import parse_template
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil import TemplateHandler
from KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider import PSAssortmentDataProvider

from KPIUtils.DB.Common import Common

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.Queries import Queries


__author__ = 'satya'


class DIAGEOINToolBox:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.commonV2 = CommonV2(self.data_provider)
        self.rds_conn = PSProjectConnector(self.data_provider.project_name, DbUsers.CalculationEng)
        self.store_assortment = PSAssortmentDataProvider(self.data_provider).execute(policy_name=None)
        self.output = output
        self.kpi_static_data = self.get_kpi_static_data()
        self.store_id = self.data_provider[Data.STORE_FK]
        self.own_manufacturer_fk = 12
        self.store_info = self.get_store_info(self.store_id)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        for column_name in self.scif.columns:
            print (column_name)
        self.common = Common(self.data_provider)
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.template_handler = TemplateHandler(self.data_provider.project_name)
        self.template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data')
        self.template_path = os.path.join(self.template_path, "Template.xlsx")
        self.kpi_template_data = parse_template(self.template_path, sheet_name='KPI')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.current_date = datetime.now()

    def get_kpi_static_data(self):
        """
        This function extracts the static new KPI data (new tables) and saves it into one 'global' data frame.
        The data is taken from static.kpi_level_2.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_assortment_product_info(self):
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
        #self.diageo_generator.activate_ootb_kpis(self.commonV2)

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

    @staticmethod
    def custom_brand_presence_sce_lvl(df):
        list_results = []
        df_product = df[['template_fk', 'assortment_fk', 'kpi_fk_lvl2', 'product_fk', 'item_id']]
        df_product = pd.DataFrame(
            df_product.groupby(['template_fk', 'assortment_fk', 'kpi_fk_lvl2'])['product_fk'].count().reset_index())
        df_product['product_fk'].fillna(0, inplace=True)

        df_item = df[['template_fk', 'assortment_fk', 'kpi_fk_lvl2', 'product_fk', 'item_id']]
        df_item = pd.DataFrame(
            df_item.groupby(['template_fk', 'assortment_fk', 'kpi_fk_lvl2'])['item_id'].count().reset_index())
        df_item['item_id'].fillna(0, inplace=True)

        df_result = df_product.merge(df_item, how='left', on="kpi_fk_lvl2")

        dict_result = {}
        for row_num, row_data in df_result.iterrows():
            dict_result['fk'] = row_data['kpi_fk_lvl2']
            dict_result['numerator_id'] = row_data['kpi_fk_lvl2']
            dict_result['numerator_result'] = row_data['item_id']

            if int(row_data['item_id']) > 0:
                dict_result['result'] = 1
            else:
                dict_result['result'] = 0

            dict_result['denominator_id'] = row_data['assortment_fk_x']
            dict_result['denominator_result'] = row_data['product_fk']
            dict_result['context_id'] = row_data['template_fk_x']
            list_results.append(dict_result)
            dict_result = {}
        return list_results

    def custom_brand_presence(self):
        list_results = []
        bb_assortment = self.store_assortment[['assortment_fk', 'kpi_fk_lvl2', 'product_fk']]
        mn_assortment = self.store_assortment[['assortment_fk', 'kpi_fk_lvl2', 'product_fk']]

        if bb_assortment.empty and mn_assortment.empty:
            Log.warning("No assortments for custom brand presence")
            return

        if self.kpi_template_data.empty:
            Log.warning("KPI Template is empty")
            return

        brand_presence_kpis = self.kpi_template_data[self.kpi_template_data['kpi_group'] == 'BRAND_GROUP_PRESENCE']

        if brand_presence_kpis.empty:
            Log.warning("No KPI in the template with kpi_group=BRAND_GROUP_PRESENCE")
            return

        for kpi_row_num, kpi_row_data in brand_presence_kpis.iterrows():
            back_bar_template = kpi_row_data['back_bar_scenes'].strip()
            menu_template = kpi_row_data['menu_scenes'].strip()

            df_scif_back_bar = self.scif[(self.scif['template_name'] == back_bar_template) & (self.scif['facings'] > 0)]
            df_scif_back_bar = df_scif_back_bar[['template_fk', 'product_fk', 'item_id']]

            df_scif_menu = self.scif[(self.scif['template_name'] == menu_template) & (self.scif['facings'] > 0)]
            df_scif_menu = df_scif_menu[['template_fk', 'product_fk', 'item_id']]

            df_bb = bb_assortment.merge(df_scif_back_bar, how='left', on="product_fk")
            list_results = self.custom_brand_presence_sce_lvl(df_bb)

            df_mn = mn_assortment.merge(df_scif_menu, how='left', on="product_fk")
            list_results.extend(self.custom_brand_presence_sce_lvl(df_mn))

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
