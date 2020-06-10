import pandas as pd
from datetime import datetime
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from KPIUtils.GlobalProjects.DIAGEO.Utils.DiageoAssortment import DiageoAssortment
# from KPIUtils.Calculations.LiveAssortment import LiveAssortmentCalculation
from KPIUtils_v2.DB.CommonV3 import Common
from KPIUtils_v2.GlobalDataProvider.LivePsDataProvider import PsDataProvider
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import Consts

from Trax.Utils.Logging.Logger import Log


class CalculateKpi(LiveSessionBaseClass):
    LVL3_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2
    LIVE_OOS = 'Live OOS'
    LIVE_OOS_SKU = 'Live OOS - SKU'
    LIVE_DIST = 'Live Distribution'
    LIVE_DIST_SKU = 'Live Distribution - SKU'
    DIST = 'Distribution'

    def __init__(self, data_provider, output):
        LiveSessionBaseClass.__init__(self, data_provider, output)
        self._data_provider = data_provider
        self.products = self._data_provider.products
        self.common = Common(data_provider, is_live=True)
        self.store_fk = data_provider.store_fk
        self.current_date = datetime.now()
        self.live_ps_provider = PsDataProvider(data_provider, None)
        self.assortment = DiageoAssortment(data_provider, self.common, self.live_ps_provider, False, False) #need to change to False, True

    def calculate_session_live_kpi(self):
        """
        Main function of live project
        """
        kpi_types = [Consts.GDPA_KPI_TYPE]
        assortment_results = self.assortment.main_assortment_calculation(kpi_types)
        assortment_results = pd.DataFrame(assortment_results)
        self.common.write_to_db_results(assortment_results)
        self.common.commit_results_data()
