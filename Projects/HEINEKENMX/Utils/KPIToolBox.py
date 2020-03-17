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

from Projects.HEINEKENMX.Data.LocalConsts import Consts
from Projects.HEINEKENMX.Refresco.KPIToolBox import RefrescoToolBox
from Projects.HEINEKENMX.Cerveza.Utils.KPIToolBox import CervezaToolBox
from Projects.HEINEKENMX.Cigarros.Utils.KPIToolBox import CigarrosToolBox
from Projects.HEINEKENMX.RTD.Utils.KPIToolBox import RTDToolBox

__author__ = 'nicolaske'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)

    def main_calculation(self):
        score = 0
        cerveza_tool_box = CervezaToolBox(self.data_provider, self.output, self.common)
        score += cerveza_tool_box.main_calculation()

        refresco_tool_box = RefrescoToolBox(self.data_provider, self.output, self.common)
        score += refresco_tool_box.main_calculation()

        cigarros_tool_box = CigarrosToolBox(self.data_provider, self.output, self.common)
        score += cigarros_tool_box.main_calculation()

        rtd_tool_box = RTDToolBox(self.data_provider, self.output, self.common)
        score += rtd_tool_box.main_calculation()

        kpi_name = Consts.KPI_IE

        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=score,
                         score=score,
                         identifier_result=kpi_fk, should_enter=True)

        # cocacola_tool_box = CocacolaToolBox(self.data_provider, self.output, self.common)
        # cocacola_tool_box.main_calculation()

        return
