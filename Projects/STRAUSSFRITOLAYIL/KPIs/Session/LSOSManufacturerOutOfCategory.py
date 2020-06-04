from Projects.STRAUSSFRITOLAYIL.KPIs.Utils import StraussfritolayilUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.STRAUSSFRITOLAYIL.Data.LocalConsts import Consts


class LSOSManufacturerOutOfCategoryKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(LSOSManufacturerOutOfCategoryKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.utils = StraussfritolayilUtil(None, data_provider)

    def calculate(self):
        kpi_fk = self.utils.common.get_kpi_fk_by_kpi_type(Consts.LSOS_OWN_BRAND_OUT_OF_CATEGORY_KPI)
        template = self.utils.kpi_external_targets[self.utils.kpi_external_targets['operation_type'] == Consts.SOS_KPIS]
        # todo: implement category extraction
        template_category_fks = [1, 2]
        # todo: implement target
        # target = self.utils.kpi_external_targets['taregt']
        target = 30
        categories = set(self.utils.match_product_in_scene_wo_hangers['category_fk'])
        category_fks = set(template_category_fks) - categories
        own_manufacturer_matches = self.utils.own_manufacturer_matches_wo_hangers.copy()
        own_manufacturer_matches = own_manufacturer_matches[own_manufacturer_matches['stacking_layer'] == 1]
        for category_fk in categories:
            own_skus_category_df = own_manufacturer_matches[own_manufacturer_matches['category_fk'] == category_fk]
            store_category_df = self.utils.match_product_in_scene_wo_hangers[
                self.utils.match_product_in_scene_wo_hangers['category_fk'] == category_fk]
            own_category_linear_length = own_skus_category_df['width_mm_advance'].sum()
            store_category_linear_length = store_category_df['width_mm_advance'].sum()
            sos_result = self.utils.calculate_sos_result(own_category_linear_length, store_category_linear_length)
            kpi_score = 1 if (target <= sos_result) else 0
            self.write_to_db_result(fk=kpi_fk, numerator_id=category_fk, denominator_id=self.utils.own_manuf_fk,
                                    numerator_result=own_category_linear_length,
                                    denominator_result=store_category_linear_length,
                                    result=sos_result, score=kpi_score)

    def kpi_type(self):
        pass
