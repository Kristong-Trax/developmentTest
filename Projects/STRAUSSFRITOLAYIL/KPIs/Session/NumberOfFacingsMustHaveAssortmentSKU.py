from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
import math
import pandas as pd
import itertools

class NumberOfFacingsMustHaveAssortmentSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfFacingsMustHaveAssortmentSKUKpi, self).__init__(data_provider, config_params=config_params,
                                                                      **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        return
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_FACINGS_MUST_HAVE_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_FACINGS_MUST_HAVE_KPI]
        fields_df = template[['EAN Code', 'Field', 'Target']]
        matches = self.utils.match_product_in_scene_wo_hangers.copy()
        matches['facings'] = 1
        store_df = matches.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
                                   ['bay_number', 'shelf_number', 'facings']]
        assortment = self.tarnsform_kpi_external_targets_to_assortment(template)
        products = list(itertools.chain.from_iterable(assortment[Consts.REPLACMENT_EAN_CODES].values.tolist()))
        categories = set(self.utils.all_products[
                             self.utils.all_products['product_ean_code'].isin(products)]['category_fk'])
        # not_existing_products_df = assortment[assortment['in_store_wo_hangers'] == 0]
        sadot_dict = {}
        for category_fk in categories:
            df = matches[(matches['category_fk'] == category_fk) & (matches['manufacturer_fk'] ==
                                                                    self.utils.own_manuf_fk)]
            category_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
                ['bay_number', 'shelf_number', 'facings']]
            category_df.columns = ['bay_number', 'shelf_number', 'facings category']
            join_df = store_df.merge(category_df, on=['bay_number', 'shelf_number'], how="left").fillna(0)
            join_df['percentage'] = join_df['facings category'] / join_df['facings']
            # number of shelves with more than 50% strauss products
            number_of_shelves = len(join_df[join_df['percentage'] >= 0.5])
            # Adding 0.001 to prevent 0 sadot case
            sadot = math.ceil((number_of_shelves + 0.001) / 5.0)
            sadot_dict[category_fk] = sadot


        # for i, sku_row in assortment.iterrows():
        #     product_fk = sku_row['product_fk']
        #     in_store = sku_row['in_store_wo_hangers']
        #     facings = sku_row['facings_all_products']
        #     category_fk = sku_row['category_fk']
        #     sadot = sadot_dict['category_fk']
        #     if sadot not in sadot_dict:
        #         target = -1
        #     else:
        #         sadot
        #     result = 2 if (in_store == 1) else 1
        #     self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
        #                             denominator_id=assortment_fk, score=facings)

    def tarnsform_kpi_external_targets_to_assortment(self, template):
        assortment = template[['kpi_fk', 'EAN Code', 'Field', 'Target']]
        assortment.rename(columns={'EAN Code': Consts.REPLACMENT_EAN_CODES}, inplace=True)
        assortment['facings'] = assortment['facings_wo_hangers'] = 0
        assortment['in_store'] = assortment['in_store_wo_hangers'] = 0
        assortment[Consts.REPLACMENT_EAN_CODES] = pd.DataFrame(assortment[Consts.REPLACMENT_EAN_CODES].str.split(','),
                                                               columns=[Consts.REPLACMENT_EAN_CODES])
        assortment[Consts.REPLACMENT_EAN_CODES] = assortment[Consts.REPLACMENT_EAN_CODES].apply(
            lambda row: [x.strip() for x in row] if row else None)
        assortment = self.utils.handle_replacment_products_row(assortment)
        assortment = self.add_product_fk_to_missing_products(assortment)
        return assortment

    def add_product_fk_to_missing_products(self, assortment):
        missing_products = assortment[assortment['in_store_wo_hangers'] != 1]
        for i, row in missing_products.iterrows():
            ean_code = row[Consts.REPLACMENT_EAN_CODES][0]
            product_df = self.utils.all_products[self.utils.all_products['product_ean_code'] == ean_code][
                'product_fk']
            product_fk = int(product_df.values[0]) if product_df.values[0] else -1
            assortment.loc[i, 'product_fk'] = product_fk
        return assortment

    def kpi_type(self):
        pass
