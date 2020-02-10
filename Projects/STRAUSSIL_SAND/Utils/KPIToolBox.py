
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from Projects.STRAUSSIL_SAND.Data.LocalConsts import Consts
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from KPIUtils_v2.Utils.Parsers import ParseInputKPI as Parser


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

__author__ = 'ilays'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.parser = Parser
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]

    def main_calculation(self):
        score = 0
        return score

    def calculate_oos(self):
        numerator = total_facings = 0
        store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.OOS)
        sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.OOS_SKU)
        leading_skus_df =
        skus_ean_list = leading_skus_df[Consts.PARAMS_VALUE_1].tolist()
        skus_ean_set = set([ean_code.strip() for values in skus_ean_list for ean_code in values.split(",")])
        product_fks = self.all_products[self.all_products['product_ean_code'].isin(skus_ean_set)]['product_fk'].tolist()
        # sku level oos
        for sku in product_fks:
            # 2 for distributed and 1 for oos
            product_df = self.scif[self.scif['product_fk'] == sku]
            if product_df.empty:
                numerator += 1
                result = 1
                facings = 0
            else:
                result = 2
                facings = product_df['facings'].values[0]
                total_facings += facings
            self.common.write_to_db_result(fk=sku_kpi_fk, numerator_id=sku, denominator_id=self.store_id,
                                           result=result, numerator_result=result, denominator_result=result,
                                           score=facings, identifier_parent="OOS", should_enter=True)
        # store level oos
        denominator = len(product_fks)
        if denominator == 0:
            numerator = result = 0
        else:
            result = round(numerator / float(denominator), 4)
        self.common.write_to_db_result(fk=store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=result, numerator_result=numerator,
                                       denominator_result=denominator, score=total_facings, identifier_result="OOS")

    def calculate_hierarchy_sos(self):
        store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.SOS_BY_OWN_MAN)
        category_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.SOS_BY_OWN_MAN_CAT)
        brand_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.SOS_BY_OWN_MAN_CAT_BRAND)
        sos_df = self.scif[self.scif['rlv_sos_sc'] == 1]
        # filter_row_dict = {'population': {'include': [{'manufacturer_fk': self.own_manufacturer_fk}]}}
        # store level sos
        store_res, store_num, store_den = self.calculate_own_manufacturer_sos(filters={}, df=sos_df)
        self.common.write_to_db_result(fk=store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=store_res, numerator_result=store_num,
                                       denominator_result=store_den, score=store_res, identifier_result="OWN_SOS")
        # category level sos
        session_categories = self.scif['category_fk'].unique()
        for category_fk in session_categories:
            filters = {'category_fk': category_fk}
            cat_res, cat_num, cat_den = self.calculate_own_manufacturer_sos(filters=filters, df=sos_df)
            self.common.write_to_db_result(fk=category_kpi_fk, numerator_id=category_fk, denominator_id=self.store_id,
                                           result=cat_res, numerator_result=cat_num, denominator_result=cat_den,
                                           score=cat_res, identifier_parent="OWN_SOS", should_enter=True,
                                           identifier_result="OWN_SOS_cat_{}".format(str(category_fk)))
            # brand-category level sos
            cat_brands = self.parser.filter_df(conditions=filters, data_frame_to_filter=sos_df)['brand_fk'].unique()
            for brand_fk in cat_brands:
                filters['brand_fk'] = brand_fk
                brand_res, brand_num, brand_den = self.calculate_own_manufacturer_sos(filters=filters, df=sos_df)
                self.common.write_to_db_result(fk=brand_kpi_fk, numerator_id=brand_fk, denominator_id=category_fk,
                                               result=brand_res, numerator_result=brand_num, should_enter=True,
                                               denominator_result=brand_den, score=brand_res,
                                               identifier_parent="OWN_SOS_cat_{}".format(str(category_fk)))

    def calculate_own_manufacturer_sos(self, filters, df):
        denominator_df = self.parser.filter_df(conditions=filters, data_frame_to_filter=df)
        filters['manufacturer_fk'] = self.own_manufacturer_fk
        numerator_df = self.parser.filter_df(conditions=filters, data_frame_to_filter=df)
        del filters['manufacturer_fk']
        if denominator_df.empty:
            return 0, 0, 0
        denominator = denominator_df['facings'].sum()
        if numerator_df.empty:
            numerator = 0
        else:
            numerator = numerator_df['facings'].sum()
        result = round(numerator / float(denominator), 3)
        return result, numerator, denominator