from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox

import pandas as pd
import numpy as np
from datetime import datetime
import dateutil.parser as dparser
import os

# from Projects.JTIMX.Data.LocalConsts import Consts


__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
PRICE_TARGET_TEMPLATE = 'Price_Target_Template'
EAN_CODE = 'EAN Code'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.visit_date = datetime.combine(self.data_provider[Data.VISIT_DATE], datetime.min.time())
        self.relevant_template = self.retrieve_price_target_df()
        self.mpis = self.data_provider[Data.MATCHES]
        self.manufacturer_fk = self.data_provider.own_manufacturer.param_value.values[0] if \
        self.data_provider.own_manufacturer.param_value.values[0] else 2

    def main_calculation(self):
        self.calculate_price_target_kpi()

    def calculate_price_target_kpi(self):
        kpi_name = 'Price Target'
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        relevant_mpis = self.mpis[['product_fk', 'price']].drop_duplicates('product_fk')
        try:
            present_products_in_session = \
                self.scif.merge(self.relevant_template, how='left', left_on='product_short_name',
                                right_on='English SKU Name')[
                    ['product_fk', 'EAN Code', 'substitution_product_fk', 'brand_fk', 'product_short_name',
                     'Target Price',
                     'facings']]
            # Returns all the present products that relvant from the template
            present_products_in_session = self.__address_substitution_product_fk(present_products_in_session,
                                                                                 relevant_mpis)

            # Calls the 'absent' products by call the products that are in template but not in scif.
            absent_products_in_session = self.relevant_template[
                ~ self.relevant_template['English SKU Name'].isin(present_products_in_session.product_short_name)]
            absent_products_in_session = \
                absent_products_in_session.merge(self.all_products.sort_values(['product_ean_code']).drop_duplicates(['product_name'],keep='first'), how='left', left_on='English SKU Name',
                                                 right_on='product_short_name')[
                    ['product_fk', 'EAN Code', 'brand_fk', 'product_short_name', 'Target Price']]
        except IndexError:
            self.write_to_db(kpi_fk, result=0, score=0, identifier_parent=self.store_id, should_enter=True)

        final_mpis = pd.concat([present_products_in_session, absent_products_in_session]).fillna(0)
        final_mpis = final_mpis[final_mpis.product_short_name.isin(
            self.relevant_template['English SKU Name'])]  # Fix the logic in the future
        template_fk = self.templates.template_fk.iloc[0] if not self.templates.empty else 0


        for i, row in final_mpis.iterrows():
            recognized_price = row['price'] if row.price and row['Target Price'] != 0 else 0
            target_price = row['Target Price']
            if target_price == 0:
                score = 0
            elif target_price == recognized_price:
                score = 1
            else:
                score = 0

            self.write_to_db(kpi_fk, numerator_id=row.product_fk, denominator_id=row.brand_fk,
                             numerator_result=recognized_price, denominator_result=target_price, result=row.facings,
                             score=score, identifier_parent=self.store_id, should_enter=True)

        parent_kpi_relevant_df = final_mpis[final_mpis['Target Price'] != 0]
        parent_kpi_relevant_result = float(
            len(np.where(parent_kpi_relevant_df['Target Price'] == parent_kpi_relevant_df.price)[0])) / len(
            parent_kpi_relevant_df) * 100

        self.write_to_db(self.common.get_kpi_fk_by_kpi_type('Price Target - Parent'), numerator_id=self.manufacturer_fk,
                         denominator_id=self.store_id, context_id=template_fk,
                         result=parent_kpi_relevant_result, identifier_result= self.store_id)

    def retrieve_price_target_df(self):
        data_name_list = os.listdir(TEMPLATE_PATH)

        # Checks all the files in the Data folder.
        # Returns the positions of all the files with 'Price_Target_Template' in its name
        price_target_template_name_index = np.flatnonzero(
            np.core.defchararray.find(data_name_list, [PRICE_TARGET_TEMPLATE]) != -1)
        price_target_template_name_list = [data_name_list[i] for i in price_target_template_name_index]
        relevant_price_target_templates = {}

        # Takes all the all the files with 'Price_Target_Template' and pares the string for dates.
        # If the date format isn't recognized, the file is deleted.
        for i, price_target in enumerate(price_target_template_name_list):
            try:
                relevant_price_target_templates[i] = dparser.parse(price_target, fuzzy=True, dayfirst=False)
            except ValueError:
                os.remove(TEMPLATE_PATH + '/' + price_target)

        # Sorts the files (descending). Then the session date is used to check against the visit date.
        # When the session date is greater than or equal to this visit date, that date is added to list.
        # Due to the fact that the list is sorted, the relevant is at index 0.
        relevant_date_time_value = [date_from_template for date_from_template in
                                    sorted(relevant_price_target_templates.values(), reverse=True) if
                                    self.visit_date >= date_from_template]
        # Calls the file name by calling the first item at index 0.
        relevant_price_target_file = price_target_template_name_list[relevant_price_target_templates.keys()[
            relevant_price_target_templates.values().index(relevant_date_time_value[0])]]

        price_target_df = pd.read_excel(TEMPLATE_PATH + '/' + relevant_price_target_file, sheet_name=0)
        return price_target_df

    @staticmethod
    def __address_substitution_product_fk(relevant_scif, relevant_mpis):
        scif_with_substitution_product_fk = relevant_scif[pd.notna(relevant_scif.substitution_product_fk)]
        relevant_mpis.product_fk = relevant_mpis.product_fk.replace(
            scif_with_substitution_product_fk.product_fk.to_numpy(),
            scif_with_substitution_product_fk.substitution_product_fk.to_numpy())
        present_products_in_session = relevant_scif.merge(relevant_mpis, how='left', on='product_fk')
        present_products_in_session.dropna(subset=['facings'], inplace=True)
        present_products_in_session.drop_duplicates(subset=['product_fk'], inplace=True)

        return present_products_in_session
