from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DataProvider import ScifConsts
from Projects.DIAGEORU.KPIs.util import DiageoConsts

__author__ = 'michaela'


class DiageoSecondaryDisplays(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(DiageoSecondaryDisplays, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        relevant_scenes = self.util.scif.loc[
            self.util.scif[ScifConsts.LOCATION_TYPE].isin(DiageoConsts.SECONDARY_SHELF)]
        set_score = len(relevant_scenes[ScifConsts.SCENE_FK].unique().tolist())
        numerator_id = self.util.own_manuf_fk
        if numerator_id not in self.util.scif.manufacturer_fk.unique():
            self.util.Log.debug('No products for own Manufacturer')
            return None
        res = self.util.build_dictionary_for_db_insert_v2(DiageoConsts.SECONDARY_DISPLAY, set_score, numerator_id,
                                                          denominator_id=self.util.store_id)
        self.write_to_db_result(**res)
