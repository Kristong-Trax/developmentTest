
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
# import pandas as pd

from Projects.PEPSIUSV2_SAND.Data.LocalConsts import Consts

# from Trax.Data.ProfessionalServices.PsConsts.DataProvider import 
# from Trax.Data.ProfessionalServices.PsConsts.DB import 
# from Trax.Data.ProfessionalServices.PsConsts.PS import 
# from Trax.Data.ProfessionalServices.PsConsts.Consts import 
# from Trax.Data.ProfessionalServices.PsConsts.Messages import 
# from Trax.Data.ProfessionalServices.PsConsts.Custom import 
# from Trax.Data.ProfessionalServices.PsConsts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'idanr'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)

    def main_calculation(self):
        score = 0
        return score
