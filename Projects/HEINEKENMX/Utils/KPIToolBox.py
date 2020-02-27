from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
# import pandas as pd

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

from Projects.HEINEKENMX.Refresco.KPIToolBox import RefrescoToolBox

__author__ = 'nicolaske'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)

    def main_calculation(self):
        # cerveza_tool_box = CervezaToolBox(self.data_provider, self.output, self.common)
        # cerveza_tool_box.main_calculation()


        refresco_tool_box = RefrescoToolBox(self.data_provider, self.output, self.common)
        refresco_tool_box.main_calculation()

        # cocacola_tool_box = CocacolaToolBox(self.data_provider, self.output, self.common)
        # cocacola_tool_box.main_calculation()

        return
