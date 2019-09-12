from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils.INBEV.INBEVToolBox import INBEVToolBox
from KPIUtils_v2.DB.CommonV2 import Common

__author__ = 'urid'


class INBEVNLINBEVBEGenerator:
    def __init__(self, data_provider, output, template=None):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = data_provider.project_name
        self.output = output
        self.session_uid = self.data_provider.session_uid
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_info = SessionInfo(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.common = Common(self.data_provider)
        self.tool_box = INBEVToolBox(self.data_provider, self.output, template, common=self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
            return
        self.tool_box.tools.update_templates()
        set_names = ['Product Blocking', 'Linear Share of Shelf', 'OSA', 'Pallet Presence', 'Share of Assortment',
                     'Product Stacking', 'Shelf Level', 'Linear Share of Shelf vs. Target', 'Shelf Impact Score',
                     # 'Product Group Blocking']
                    ]
        for kpi_set_name in set_names:
            self.tool_box.main_calculation(set_name=kpi_set_name)
        self.tool_box.save_custom_scene_item_facts_results()
        self.tool_box.save_linear_length_results()
        Log.info('Downloading templates took {}'.format(self.tool_box.download_time))
        self.tool_box.commit_results_data()
        self.tool_box.main_calculation_poce()
        self.common.commit_results_data()

