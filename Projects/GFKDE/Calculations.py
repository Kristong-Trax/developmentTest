from Projects.GFKDE.SOSUnboxedBrandCategory import SOSUnboxedBrandCategory_KPI
from Projects.GFKDE.SOSUnboxedBrandSubCategory import SOSUnboxedBrandSubCategory_KPI
from Projects.GFKDE.SOSUnboxedManufacturerCategory import SOSUnboxedManufacturerCategory_KPI
from Projects.GFKDE.SOSUnboxedManufacturerSubCategory import SOSUnboxedManufacturerSubCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndBrandCategory import SosOnGondolaEndBrandCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndBrandSubCategory import SosOnGondolaEndBrandSubCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndManufacturerCategory import SosOnGondolaEndManufacturerCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndManufacturerSubCategory import SosOnGondolaEndManufacturerSubCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndSKUCategory import SosOnGondolaEndSKUCategory_KPI
from Projects.GFKDE.SOSUnboxedOnGondolaEndSKUSubCategory import SosOnGondolaEndSKUSubCategory_KPI
from Projects.GFKDE.SOSUnboxedSKUCategory import SOSUnboxedSKUCategory_KPI
from Projects.GFKDE.SOSUnboxedSKUSubCategory import SOSUnboxedSKUSubCategory_KPI
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output

__author__ = 'Eli'


class GFKDECalculations(BaseCalculationsScript):
    """
    https://confluence.trax-cloud.com/pages/resumedraft.action?draftId=174198555&draftShareId=feac8c7a-ec57-4b36-b380-190d3668debc
    """
    @log_runtime("GSKDE session runtime")
    def run_project_calculations(self):
        self.timer.start()
        SOS = [SOSUnboxedBrandCategory_KPI, SOSUnboxedManufacturerCategory_KPI, SOSUnboxedSKUCategory_KPI]
        SOS_SUB = [SOSUnboxedBrandSubCategory_KPI, SOSUnboxedManufacturerSubCategory_KPI, SOSUnboxedSKUSubCategory_KPI]
        GONDOLA_END = [SosOnGondolaEndBrandCategory_KPI, SosOnGondolaEndManufacturerCategory_KPI, SosOnGondolaEndSKUCategory_KPI]
        GONDOLA_END_SUB = [SosOnGondolaEndBrandSubCategory_KPI, SosOnGondolaEndManufacturerSubCategory_KPI, SosOnGondolaEndSKUSubCategory_KPI]

        KPIs = SOS + SOS_SUB + GONDOLA_END + GONDOLA_END_SUB
        for kpi in KPIs:
            kpi(data_provider=self.data_provider).calculate()
        # common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

if __name__ == '__main__':
    LoggerInitializer.init('gfkde calculations')
    Config.init()
    project_name = 'gfkde'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['498668b8-0644-4dd2-9391-2702b7d4cffb'] #'fae5cf3a-16ea-40e3-9016-ff4bb2730291',
    for session in sessions:
        data_provider.load_session_data(session)
        GFKDECalculations(data_provider=data_provider, output=None).run_project_calculations()
