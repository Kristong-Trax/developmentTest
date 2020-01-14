from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox

import pandas as pd
import numpy as np
from datetime import datetime
import dateutil.parser as dparser
import os

from Projects.JTIMX.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.visit_date = datetime.combine(self.data_provider[Data.VISIT_DATE], datetime.min.time())
        self.relevant_template = self.retrieve_price_target_df()
        self.mpis = self.data_provider[Data.MATCHES]

    def main_calculation(self):
        self.calculate_price_target_kpi()

    def calculate_price_target_kpi(self):
        kpi_name = 'Price Target'
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        relevant_mpis = self.mpis[['product_fk', 'price']].drop_duplicates('product_fk')
        present_products_in_session = self.scif.merge(self.relevant_template, how='left', left_on='product_short_name',
                                                      right_on='English SKU Name')[
            ['product_fk', 'brand_fk', 'product_short_name', 'Target Price', 'facings']]
        present_products_in_session = present_products_in_session.merge(relevant_mpis, how='left', on='product_fk')

        absent_products_in_session = self.relevant_template[
            ~ self.relevant_template['English SKU Name'].isin(present_products_in_session.product_short_name)]
        absent_products_in_session = \
        absent_products_in_session.merge(self.all_products, how='left', left_on='English SKU Name',
                                         right_on='product_short_name')[
            ['product_fk', 'brand_fk', 'product_short_name', 'Target Price']]
        final_mpis = pd.concat([present_products_in_session,absent_products_in_session]).fillna(0)


        for i, row in final_mpis.iterrows():
            recognized_price = row['price']
            target_price = row['Target Price']
            score = 1 if target_price == recognized_price else 0
            self.write_to_db(kpi_fk, numerator_id=row.product_fk, denominator_id=row.brand_fk,
                             numerator_result=recognized_price, denominator_result=target_price, result=row.facings,
                             score=score)


    def retrieve_price_target_df(self):
        data_name_list = os.listdir(TEMPLATE_PATH)
        price_target_template_name_index = np.flatnonzero(
            np.core.defchararray.find(data_name_list, [Consts.PRICE_TARGET_TEMPLATE]) != -1)
        price_target_template_name_list = [data_name_list[i] for i in price_target_template_name_index]
        relevant_price_target_templates = {}
        for i, price_target in enumerate(price_target_template_name_list):
            try:
                relevant_price_target_templates[i] = dparser.parse(price_target, fuzzy=True, dayfirst=False)
            except ValueError:
                os.remove(TEMPLATE_PATH + '/' + price_target)

        relevant_date_time_value = [date_from_template for date_from_template in
                                    sorted(relevant_price_target_templates.values(), reverse=True) if
                                    self.visit_date >= date_from_template]
        relevant_price_target_file = price_target_template_name_list[relevant_price_target_templates.keys()[
            relevant_price_target_templates.values().index(relevant_date_time_value[0])]]

        price_target_df = pd.read_excel(TEMPLATE_PATH + '/' + relevant_price_target_file, sheet_name=0)
        return price_target_df
