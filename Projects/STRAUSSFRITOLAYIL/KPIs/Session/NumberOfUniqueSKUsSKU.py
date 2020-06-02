from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts
from Trax.Utils.Logging.Logger import Log


class NumberOfUniqueSKUsSKUKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(NumberOfUniqueSKUsSKUKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.NUMBER_OF_UNQIUE_SKUS_SKU_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['kpi_type'] ==
                                                   Consts.NUMBER_OF_UNQIUE_SKUS_KPI]
        if template.empty:
            categories = ['Core Salty']
        else:
            categories = template['category'][0].split(",")
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[~own_manufacturer_matches['product_type'].isin(
            ['Other', 'Empty'])]
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['category'].isin(categories)]
        own_manufacturer_matches['facings'] = 1
        combined_scenes_matches = own_manufacturer_matches.groupby(['product_fk']).sum().reset_index()
        for i, sku_row in combined_scenes_matches.iterrows():
            product_fk = sku_row['product_fk']
            result = sku_row['facings']
            score = 1 if result > 0 else 0
            self.write_to_db_result(fk=kpi_fk, numerator_id=product_fk, result=result,
                                    denominator_id=self.utils.own_manuf_fk, score=score)

    def kpi_type(self):
        pass
