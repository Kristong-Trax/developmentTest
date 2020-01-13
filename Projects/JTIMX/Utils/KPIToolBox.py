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

    def main_calculation(self):
        self.calculate_price_target_kpi()

    def calculate_price_target_kpi(self):
        kpi_name = 'Price Target'
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        for i, row in self.relevant_template.iterrows():
            if np.isnan(row[Consts.EAN_CODE]):
                # do this
                a = 1
            else:
                # do this
                self.all_products.product_ean_code = self.all_products.product_ean_code.astype('float')
                denominator_id = self.all_products[self.all_products.product_ean_code.isin([row[Consts.EAN_CODE]])]
                relevant_scif = self.scif[self.scif.product_ean_code.isin([row[Consts.EAN_CODE]])]
                if relevant_scif.empty:
                    self.write_to_db(fk=kpi_fk,numerator_id=denominator_id, denominator_id=denominator_id, numerator_result=denominator_id, denominator_result=0)


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
