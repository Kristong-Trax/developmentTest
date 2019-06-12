
import os
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

DISPLAY_PRESENCE_SHEET = 'display_presence'
STORE_GROUP_SHEET = 'store_group'

class GSKAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)
        self.display_compliance_template = pd.ExcelFile(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                                      '..', 'Data', 'display_compliance.xlsx'))
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # assortment_store_dict = self.gsk_generator.availability_store_function()
        # self.common.save_json_to_new_tables(assortment_store_dict)
        #
        # assortment_category_dict = self.gsk_generator.availability_category_function()
        # self.common.save_json_to_new_tables(assortment_category_dict)
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
        #
        self.calculate_display_complliance()
        # self.common.commit_results_data()
        return

    def calculate_display_complliance(self):
        display_presence_df = pd.read_excel(self.display_compliance_template,
                                            DISPLAY_PRESENCE_SHEET,
                                            keep_default_na=False)
        store_group_df = pd.read_excel(self.display_compliance_template,
                                       STORE_GROUP_SHEET,
                                       keep_default_na=False)
        # get the posms to check - all present
        # get the ean codes to check -- all present
        # all has price in scif
        all_posms_present = False
        all_ean_codes_present = False
        all_has_price = False
        store_group_match = False
        matched_group_name = ''
        for idx, each_group_data in store_group_df.iterrows():
            filter_dict = {'region_name': each_group_data.region_name.strip(),
                           'retailer_name': each_group_data.retailer_name.strip(),
                           'additional_attribute_1': each_group_data.segment.strip(),
                           'additional_attribute_2': each_group_data.retail_channel.strip(),
                           }
            query_string = ''
            for k, v in filter_dict.iteritems():
                query_string += "{}=='{}' and ".format(k, v)
            if not self.store_info.query(query_string.rstrip(' and ')).empty:
                store_group_match = True
                matched_group_name = each_group_data.group_name
                break
        if store_group_match and matched_group_name:
            displays_to_check_df = display_presence_df.query(
                'store_group==\'{}\''.format(each_group_data.group_name)
            )
            posm_list = [x.strip() for x in displays_to_check_df['POSM'].tolist()]
            posm_scif_df = self.scif.query('template_name in {}'.format(posm_list))
            if all([x in posm_scif_df['template_name'].unique() for x in posm_list]):
                # if all posms in sheet are in the scif
                all_posms_present = True
            if all_posms_present:
                ean_codes_to_check = []
                for each_data in displays_to_check_df['EAN Codes'].tolist():
                    ean_codes_to_check.extend([x.strip() for x in each_data.split(',')])
                if ean_codes_to_check:
                    valid_product_fks = self.products.query('product_ean_code in {}'
                                                            .format(ean_codes_to_check))['product_fk']
                    valid_items_in_scif = self.scif.query('item_id in {}'.format(valid_product_fks.tolist()))
                    if len(valid_items_in_scif) == len(ean_codes_to_check):
                        all_ean_codes_present = True


                else:
                    # no valid products
                    Log.warning("No valid products for session {}".format(self.session_uid))

        else:
            Log.info("No match for the store groups in template in session {}.".format(
                self.session_uid))

        pass
