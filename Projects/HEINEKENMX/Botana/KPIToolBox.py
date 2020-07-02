from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd


# from Trax.Data.ProfessionalServices.PsConsts.DataProvider import
# from Trax.Data.ProfessionalServices.PsConsts.DB import
# from Trax.Data.ProfessionalServices.PsConsts.PS import
# from Trax.Data.ProfessionalServices.PsConsts.GlobalConsts import
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
from Projects.HEINEKENMX.Botana.Barcel.Utils.KPIToolBox import BarcelToolBox
from Projects.HEINEKENMX.Botana.Sabritas.Utils.KPIToolBox import SabritasToolBox
from Projects.HEINEKENMX.Refresco.Pepsi.Utils.KPIToolBox import PepsiToolBox
from Projects.HEINEKENMX.Refresco.PJ.Utils.KPIToolBox import PJToolBox

from Projects.HEINEKENMX.Botana.Const import Const


__author__ = 'nicolaske'


class BotanaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)

    def main_calculation(self):
        kpi_name = Const.KPI_Botana
        max_possible_point = Const.KPI_WEIGHTS[kpi_name]
        score = 0
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self.get_parent_fk(kpi_name)



        barcel_tool_box = BarcelToolBox(self.data_provider, self.output, self.common)
        score += barcel_tool_box.main_calculation()

        #
        sabritas_tool_box = SabritasToolBox(self.data_provider, self.output, self.common)
        score += sabritas_tool_box.main_calculation()




        ratio = (score / max_possible_point) * 100


        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         result=ratio,
                         score=score, kpi_weight=max_possible_point, target=max_possible_point,
                         identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)
        return score


    def calculate_average_ratio(self, ratio_list):
        ratio_sum = 0
        list_count = len(ratio_list)
        for ratio in ratio_list:
            ratio_sum += ratio

        if list_count != 0:
            final_ratio = round(ratio_sum / float(list_count), 2)
        else:
            final_ratio = 0
        return final_ratio


    def get_parent_fk(self, kpi_name):
        parent_kpi_name = Const.KPIS_HIERACHY[kpi_name]
        parent_fk = self.get_kpi_fk_by_kpi_type(parent_kpi_name)
        return parent_fk

    def calculate_sum_scores(self, score_list):
        score_sum = 0

        for score in score_list:
            score_sum += score

        return score_sum

