
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd

from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Projects.STRAUSSIL_SAND.Data.LocalConsts import Consts
from KPIUtils_v2.Utils.Parsers import ParseInputKPI as Parser
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


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
        self.assortment = Assortment(self.data_provider, self.output)
        self.ps_data = PsDataProvider(self.data_provider, self.output)
        self.kpi_external_targets = self.ps_data.get_kpi_external_targets()

    def main_calculation(self):
        self.calculate_core_oos_and_distribution()
        self.calculate_score_sos()

    def calculate_core_oos_and_distribution(self):
        dis_numerator = total_facings = 0
        oos_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.OOS)
        oos_sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.OOS_SKU)
        dis_store_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.DISTRIBUTION)
        dis_cat_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.DISTRIBUTION_CAT)
        dis_sku_kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_type=Consts.DISTRIBUTION_SKU)
        assortment_df = self.assortment.get_lvl3_relevant_ass()
        skus_ean_list = assortment_df['product_ean_code'].tolist()
        skus_ean_set = set([ean_code.strip() for values in skus_ean_list for ean_code in values.split(",")])
        product_fks = self.all_products[self.all_products['product_ean_code'].isin(skus_ean_set)]['product_fk'].tolist()
        categories = self.all_products[self.all_products['product_fk'].isin(product_fks)]['category_fk'].unqiue()
        categories_dict = dict.fromkeys(categories, (0, 0))

        # sku level distribution
        for sku in product_fks:
            # 2 for distributed and 1 for oos
            category_fk = self.all_products[self.all_products['product_fk'] == sku]['category_fk']
            product_df = self.scif[self.scif['product_fk'] == sku]
            if product_df.empty:
                categories_dict[category_fk] = map(sum, zip(categories_dict[category_fk], [0, 1]))
                result = 1
                facings = 0
                # Saving OOS only if product wasn't in store
                self.common.write_to_db_result(fk=oos_sku_kpi_fk, numerator_id=sku, denominator_id=self.store_id,
                                               result=result, numerator_result=result, denominator_result=result,
                                               score=facings, identifier_parent="CORE_OOS", should_enter=True)
            else:
                categories_dict[category_fk] = map(sum, zip(categories_dict[category_fk], [1, 1]))
                result = 2
                facings = product_df['facings'].values[0]
                dis_numerator += 1
                total_facings += facings
            self.common.write_to_db_result(fk=dis_sku_kpi_fk, numerator_id=sku, denominator_id=category_fk,
                                           result=result, numerator_result=result, denominator_result=result,
                                           score=facings, should_enter=True,
                                           identifier_parent="CORE_DIS_CAT_{}".format(str(category_fk)))

        # category level distribution
        for category_fk in categories_dict.keys():
            cat_numerator, cat_denominator = categories_dict[category_fk]
            if cat_denominator == 0:
                cat_numerator = cat_result = 0
            else:
                cat_result = round(cat_numerator / float(cat_denominator), 4)
            self.common.write_to_db_result(fk=dis_cat_kpi_fk, numerator_id=category_fk,
                                           denominator_id=self.store_id, result=cat_result,
                                           numerator_result=cat_numerator, denominator_result=cat_denominator,
                                           score=cat_result, identifier_parent="CORE_DIS",
                                           identifier_result="CORE_DIS_CAT_{}".format(str(category_fk)))

        # store level oos and distribution
        denominator = len(product_fks)
        if denominator == 0:
            dis_numerator = dis_result = oos_result = 0
        else:
            dis_result = round(dis_numerator / float(denominator), 4)
            oos_result = 1 - dis_result
        oos_numerator = denominator - dis_numerator
        self.common.write_to_db_result(fk=oos_store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=oos_result, numerator_result=dis_numerator,
                                       denominator_result=denominator, score=total_facings,
                                       identifier_result="CORE_OOS")
        self.common.write_to_db_result(fk=dis_store_kpi_fk, numerator_id=self.own_manufacturer_fk,
                                       denominator_id=self.store_id, result=dis_result, numerator_result=oos_numerator,
                                       denominator_result=denominator, score=total_facings,
                                       identifier_result="CORE_DIS")

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

    def calculate_score_sos(self):
        pass