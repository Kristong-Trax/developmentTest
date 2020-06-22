from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class SKUFacingsKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SKUFacingsKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.SKU_FACINGS_KPI)
        custom_matches = self.utils.match_product_in_scene_wo_hangers.copy()
        custom_matches = custom_matches[custom_matches['stacking_layer'] == 1]
        custom_matches['facings'] = 1
        custom_matches = custom_matches[custom_matches['product_type'].isin(['SKU', 'Other', 'Empty'])]
        custom_matches = custom_matches[['product_fk', 'manufacturer_fk', 'facings']]
        combined_scenes_matches = custom_matches.groupby(['product_fk', 'manufacturer_fk']).sum().reset_index()
        for i, sku_row in combined_scenes_matches.iterrows():
            product_fk = sku_row['product_fk']
            manufacturer_fk = sku_row['manufacturer_fk']
            result = sku_row['facings']
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=manufacturer_fk, score=result)

    def kpi_type(self):
        pass
