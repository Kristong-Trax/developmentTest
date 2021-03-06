from Projects.PNGJP_SAND2.KPIS.Util import PNGJP_SAND2Util
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import MatchesConsts, ScifConsts
import numpy as np


class SKUFacingsbySceneKpi(UnifiedCalculationsScript):
    def __init__(self, data_provider, config_params=None, **kwargs):
        super(SKUFacingsbySceneKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PNGJP_SAND2Util(None, data_provider)
        # self.kpi_name = self._config_params['kpi_type']

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.scif.empty:
            ext_targets = self.util.get_target_by_kpi_type('PGJAPAN_SKU_FACINGS_BY_SCENE')
            if not ext_targets.empty:
                template_fk = self.util.scif[ScifConsts.TEMPLATE_FK].values[0]
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type('PGJAPAN_SKU_FACINGS_BY_SCENE')
                target_prameters = ext_targets.iloc[0]
                matches = self.util.filter_matches_for_scene_kpis(target_prameters)
                matches = matches[~(matches[ScifConsts.PRODUCT_TYPE] == 'POS')]
                matches = matches[~(matches[MatchesConsts.BAY_NUMBER] == -1)]
                max_shelf = matches.groupby([MatchesConsts.BAY_NUMBER],
                                            as_index=False).agg({MatchesConsts.SHELF_NUMBER: np.max})
                max_shelf.rename(columns={MatchesConsts.SHELF_NUMBER: self.util.MAX_SHELF}, inplace=True)

                if target_prameters['Include Stacking']:
                    result_df = matches.groupby([MatchesConsts.PRODUCT_FK, MatchesConsts.SHELF_NUMBER,
                                                 MatchesConsts.BAY_NUMBER], as_index=False).agg({'count': np.sum})
                    result_df = result_df.merge(max_shelf, on=MatchesConsts.BAY_NUMBER, how='left')
                else:
                    matches = matches[matches[MatchesConsts.STACKING_LAYER] == 1]
                    result_df = matches.groupby([MatchesConsts.PRODUCT_FK, MatchesConsts.SHELF_NUMBER,
                                                 MatchesConsts.BAY_NUMBER], as_index=False).agg({'count': np.sum})
                    result_df = result_df.merge(max_shelf, on=MatchesConsts.BAY_NUMBER, how='left')

                total_sku_in_bay_shelf = result_df.groupby(
                    [MatchesConsts.BAY_NUMBER, MatchesConsts.SHELF_NUMBER])['count'].sum().to_dict()
                for i, row in result_df.iterrows():
                    bay_shelf_comb_fk = self.util.get_context_fk_from_custom_entity(
                        bay_no=row[MatchesConsts.BAY_NUMBER],
                        shelf_no=row[MatchesConsts.SHELF_NUMBER]
                    )
                    target = total_sku_in_bay_shelf.get(
                        (row[MatchesConsts.BAY_NUMBER], row[MatchesConsts.SHELF_NUMBER]), 0)
                    weight = self.util.get_product_position_code(
                        product_fk=row[MatchesConsts.PRODUCT_FK],
                        shelf_number=row[MatchesConsts.SHELF_NUMBER]
                    )
                    self.write_to_db_result(fk=kpi_fk, numerator_id=row[MatchesConsts.PRODUCT_FK],
                                            numerator_result=row[MatchesConsts.BAY_NUMBER],
                                            denominator_id=template_fk,
                                            denominator_result=row[MatchesConsts.SHELF_NUMBER],
                                            context_id=bay_shelf_comb_fk,
                                            result=row['count'], score=row[self.util.MAX_SHELF], by_scene=True,
                                            target=target,
                                            weight=weight)
