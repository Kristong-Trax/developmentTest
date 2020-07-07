from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
import math


class NumberOfFacingsMustHaveAssortmentSKUKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfFacingsMustHaveAssortmentSKUKpi, self).__init__(data_provider, config_params=config_params,
                                                                      **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_FACINGS_MUST_HAVE_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_FACINGS_MUST_HAVE_KPI]
        template_categories = set(template[Consts.CATEGORY])
        fields_df = template[[Consts.EAN_CODE, Consts.FIELD, Consts.TARGET_MAX]]
        matches = self.utils.match_product_in_scene_wo_hangers.copy()
        matches['facings'] = 1
        store_df = matches.groupby(['scene_fk', 'bay_number', 'shelf_number']).sum().reset_index()[
                                   ['scene_fk', 'bay_number', 'shelf_number', 'facings']]
        categories = set(self.utils.all_products[self.utils.all_products[
            'category'].isin(template_categories)]['category_fk'])
        # not_existing_products_df = assortment[assortment['in_store_wo_hangers'] == 0]
        df = matches[(matches['category_fk'].isin(categories)) & (matches['manufacturer_fk'] ==
                                                                  self.utils.own_manuf_fk)]
        category_df = df.groupby(['scene_fk', 'bay_number', 'shelf_number']).sum().reset_index()[
            ['scene_fk', 'bay_number', 'shelf_number', 'facings']]
        category_df.columns = ['scene_fk', 'bay_number', 'shelf_number', 'facings category']
        join_df = store_df.merge(category_df, on=['scene_fk', 'bay_number', 'shelf_number'], how="left").fillna(0)
        join_df['percentage'] = join_df['facings category'] / join_df['facings']
        # number of shelves with more than 50% strauss products
        number_of_shelves = len(join_df[join_df['percentage'] >= 0.5])
        sadot = math.ceil(number_of_shelves / 5.0)
        sadot = sadot if sadot != 0 else 1
        sadot = sadot if sadot < fields_df[Consts.FIELD].max() else fields_df[Consts.FIELD].max()
        template = template[template[Consts.FIELD] == sadot]
        assortment = self.tarnsform_kpi_external_targets_to_assortment(template)
        for i, sku_row in assortment.iterrows():
            product_fk = sku_row['product_fk']
            facings = sku_row['facings_all_products_wo_hangers']
            target = sku_row[Consts.TARGET_MAX]
            score = Consts.PASS if facings >= target else Consts.FAIL
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=facings, weight=sadot, target=target,
                                    denominator_id=self.utils.store_id, score=score)

    def tarnsform_kpi_external_targets_to_assortment(self, template):
        assortment = template[['kpi_fk', Consts.EAN_CODE, Consts.FIELD, Consts.TARGET_MAX]].copy()
        assortment.rename(columns={'EAN Code': Consts.REPLACMENT_EAN_CODES}, inplace=True)
        assortment['facings'] = assortment['facings_wo_hangers'] = 0
        assortment['in_store'] = assortment['in_store_wo_hangers'] = 0
        assortment[Consts.REPLACMENT_EAN_CODES] = assortment[Consts.REPLACMENT_EAN_CODES].map(
            lambda row: [row] if type(row) != list else row)
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
