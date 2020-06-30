from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Decorators.Decorators import kpi_runtime
from Projects.STRAUSSFRITOLAYIL_SAND.Data.LocalConsts import Consts
from KPIUtils_v2.Utils.Consts.PS import ExternalTargetsConsts
from KPIUtils_v2.Utils.Parsers import ParseInputKPI as Parser
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'ilays'


class ToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)

    def main_calculation(self):
        pass