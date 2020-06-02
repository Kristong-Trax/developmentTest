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
        fields_df = template[['Field', 'Target']]
        matches = self.utils.match_product_in_scene_wo_hangers.copy()
        matches['facings'] = 1
        store_df = matches.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
                                   ['bay_number', 'shelf_number', 'facings']]
        assortment = self.tarnsform_kpi_external_targets_to_assortment(template)
        categories = set(self.utils.all_products[self.utils.all_products[
            'product_ean_code'].isin(assortment['EAN Code'])])
        sadot_dict = {}
        for category_fk in categories:
            df = matches[matches['category_fk'] == category_fk]
            category_df = df.groupby(['bay_number', 'shelf_number']).sum().reset_index()[
                ['bay_number', 'shelf_number', 'facings']]
            shelves_category_percentage = category_df / store_df
            # number of shelves with more than 50% strauss products
            number_of_shelves = len(shelves_category_percentage[shelves_category_percentage['facings'] >= 0.5])
            # Adding 0.001 to prevent 0 sadot case
            sadot = math.ceil((number_of_shelves + 0.001) / 5.0)
            target = fields_df[fields_df['Field'] == sadot]
            sadot_dict[category_fk] = target


        # for i, sku_row in assortment.iterrows():
        #     products_fk = sku_row['EAN Code'].split(",")
        #     in_store = sku_row['in_store']
        #     facings = sku_row['facings_all_products']
        #     assortment_fk = sku_row['assortment_fk']
        #     sku_df = self.utils.all_products[self.utils.all_products['product_ean_code'].isin(products_ean_codes)]
        #     result = 2 if (in_store == 1) else 1
        #     self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
        #                             denominator_id=assortment_fk, score=facings)

    def tarnsform_kpi_external_targets_to_assortment(self, template):
        pass

    def kpi_type(self):
        pass
