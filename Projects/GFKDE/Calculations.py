from KPIUtils.GlobalProjects.GFK.Base.DataExecutionManager import GFKDataManager
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
from Projects.GFKDE.ShareOfBrandedZonesBrandCategory import ShareOfBrandedZonesBrandCategory_KPI
from Projects.GFKDE.ShareOfBrandedZonesBrandSubCategory import ShareOfBrandedZonesBrandSubCategory_KPI
from Projects.GFKDE.ShareOfBrandedZonesManufacturerCategory import ShareOfBrandedZonesManufacturerCategory_KPI
from Projects.GFKDE.ShareOfBrandedZonesManufacturerSubCategory import ShareOfBrandedZonesManufacturerSubCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesBrandCategory import SosOnBrandedZonesBrandCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesBrandSubCategory import SosOnBrandedZonesBrandSubCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesManufacturerCategory import SosOnBrandedZonesManufacturerCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesManufacturerSubCategory import SosOnBrandedZonesManufacturerSubCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesSKUCategory import SosOnBrandedZonesSkuCategory_KPI
from Projects.GFKDE.SosOnBrandedZonesSKUSubCategory import SosOnBrandedZonesSkuSubCategory_KPI
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

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
        SOS_BRANDED = [SosOnBrandedZonesBrandCategory_KPI, SosOnBrandedZonesManufacturerCategory_KPI, SosOnBrandedZonesSkuCategory_KPI]
        SOS_BRANDED_SUB = [SosOnBrandedZonesBrandSubCategory_KPI, SosOnBrandedZonesManufacturerSubCategory_KPI, SosOnBrandedZonesSkuSubCategory_KPI]
        SHARE_BRANDED = [ShareOfBrandedZonesBrandCategory_KPI, ShareOfBrandedZonesManufacturerCategory_KPI]
        SHARE_BRANDED_SUB = [ShareOfBrandedZonesBrandSubCategory_KPI, ShareOfBrandedZonesManufacturerSubCategory_KPI]

        KPIs = SOS + SOS_SUB + GONDOLA_END + GONDOLA_END_SUB + SOS_BRANDED + SOS_BRANDED_SUB + SHARE_BRANDED + SHARE_BRANDED_SUB

        for kpi in KPIs:
            kpi(data_provider=self.data_provider).calculate()
        dm = GFKDataManager(None)
        dm.commit_resutls()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gfkde calculations')
#     Config.init()
#     project_name = 'gfkde'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['9c579504-defe-4504-a15e-925264a6408f']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         GFKDECalculations(data_provider=data_provider, output=None).run_project_calculations()


