
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.HEINEKENMX.Cocacola.Utils.Const import Const

from Projects.HEINEKENMX.Data.LocalConsts import Consts

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

__author__ = 'nicolaske'


class CocacolaToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        template = self.get_template()
        manufacturer_pk = self.all_products['manufacturer_name'][self.all_products['manufacturer_name'] == Const.COCACOLA].iloc[0]


    def main_calculation(self):


        self.calculate_empty_exist()
        # self.calculate_distribution()
        # self.calculate_facing_count()




    def calculate_empty_exist(self):
        for kpi_name in Const.KPI_EMPTY:
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            empty_df = self.scif[(self.scif['template_name'] == Const.RELEVANT_SCENES_TYPES) & (self.scif['product_type'] == Const.EMPTY)]

            if empty_df.empty:
                score = 1
            else:
                score = 0

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, denominator_id=self.store_id, score=score,
                             identifier_parent=parent_fk, identifier_result=kpi_fk, should_enter=True)

    def calculate_facing_count(self):
        for kpi_name in Const.KPI_FACINGS:
            kpi_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
            parent_fk = self.get_parent_fk(kpi_name)

            #place holding these for now, will fix tomorrow feb 19
            score = 0
            numerator_facings = 0
            denominator_facings = 0

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_pk, numerator_result= numerator_facings,
                             denominator_id=self.store_id, denominator_result= denominator_facings, score=score,
                             )




    def calculate_distribution(self):
        #blah blah
        pass


    def calculate_invasion(self):
        score = 0
        possible_points = 0
        self.write_to_db(numerator_result=possible_points ,score=score)



    def get_template(self):
        template_df = pd.read_excel(Const.KPI_TEMPLATE, sheetname='matches')
        return template_df

    def get_parent_fk(self, kpi_name):
        parent_fk = self.get_kpi_fk_by_kpi_name(kpi_name)
        return parent_fk
