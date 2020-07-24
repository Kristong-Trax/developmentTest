from Projects.PEPSICOUK.KPIs.Util import PepsicoUtil
from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Trax.Data.ProfessionalServices.PsConsts.DataProvider import ScifConsts


class BinsNotRecognizedKpi(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BinsNotRecognizedKpi, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = PepsicoUtil(None, data_provider)

    def kpi_type(self):
        pass

    def calculate(self):
        if not self.util.commontools.scif.empty:
            if not self.util.commontools.are_all_bins_tagged:
                kpi_fk = self.util.common.get_kpi_fk_by_kpi_type(self.util.BINS_NOT_RECOGNIZED)
                scene_fk = self.util.scif[ScifConsts.SCENE_FK].values[0]
                self.write_to_db_result(fk=kpi_fk, numerator_id=scene_fk,
                                        result=1, score=1, by_scene=True)