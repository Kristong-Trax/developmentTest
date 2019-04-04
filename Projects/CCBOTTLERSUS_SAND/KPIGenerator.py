
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as Common2

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCBOTTLERSUS_SAND.ARA.KPIToolBox import ARAToolBox
from Projects.CCBOTTLERSUS_SAND.CMA.KPIToolBox import CMAToolBox
from Projects.CCBOTTLERSUS_SAND.CMA_SOUTHWEST.KPIToolBox import CMASOUTHWESTToolBox
from Projects.CCBOTTLERSUS_SAND.SCENE_SESSION.KPIToolBox import SceneSessionToolBox
from Projects.CCBOTTLERSUS_SAND.REDSCORE.KPIToolBox import REDToolBox
from Projects.CCBOTTLERSUS_SAND.DISPLAYS.KPIToolBox import DISPLAYSToolBox
# from Projects.CCBOTTLERSUS_SAND.Utils.KPIToolBox import BCIKPIToolBox
from Projects.CCBOTTLERSUS_SAND.SOVI.KPIToolBox import SOVIToolBox
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS_SAND.MSC.KPIToolBox import MSCToolBox
from Projects.CCBOTTLERSUS_SAND.LIBERTY.KPIToolBox import LIBERTYToolBox

__author__ = 'Elyashiv'


class CCBOTTLERSUS_SANDGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common_db = Common2(self.data_provider)

    @log_runtime('Total CCBOTTLERSUS_SANDCalculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        Common(self.data_provider).commit_results_data()
        self.calculate_red_score()  # should be first, because it can include a deletion from the common
        # # self.calculate_bci()
        self.calculate_manufacturer_displays()
        self.calculate_cma_compliance()
        self.calculate_sovi()
        self.calculate_ara()
        self.calculate_msc()
        self.calculate_liberty()
        self.common_db.commit_results_data()

        # self.calculate_cma_compliance_sw()

    @log_runtime('Manufacturer Displays CCBOTTLERSUS_SANDCalculations')
    def calculate_manufacturer_displays(self):
        tool_box = DISPLAYSToolBox(self.data_provider, self.output, self.common_db)
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        if 'Manufacturer Displays' in set_names:
            tool_box.main_calculation()
            set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == 'Manufacturer Displays'][
                'kpi_set_fk'].values[0]
            tool_box.commit_results_data(kpi_set_fk=set_fk)

    # @log_runtime('Bci CCBOTTLERSUS_SANDCalculations')
    # def calculate_bci(self):
    #     tool_box = BCIKPIToolBox(self.data_provider, self.output)
    #     if tool_box.scif.empty:
    #         Log.warning('Scene item facts is empty for this session')
    #     set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
    #     tool_box.tools.update_templates()
    #     for kpi_set_name in set_names:
    #         if kpi_set_name == 'BCI':
    #             tool_box.main_calculation(set_name=kpi_set_name)
    #             tool_box.save_level1(set_name=kpi_set_name, score=100)
    #             Log.info('calculate kpi took {}'.format(tool_box.download_time))
    #             set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data[
    #                                                   'kpi_set_name'] == kpi_set_name]['kpi_set_fk'].values[0]
    #             tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Red Score CCBOTTLERSUS_SANDCalculations')
    def calculate_red_score(self):
        Log.info('starting calculate_red_score')
        try:
            tool_box = REDToolBox(self.data_provider, self.output, Const.SOVI, self.common_db)
            if tool_box.main_calculation() > 0:
                tool_box.commit_results()
                return
            tool_box.remove_queries_of_calculation_type()
            tool_box = REDToolBox(self.data_provider, self.output, Const.MANUAL, self.common_db)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CCBOTTLERSUS RED SCORE :{}'.format(e.message))

    @log_runtime('CMA Compliance CCBOTTLERSUS_SANDCalculations')
    def calculate_cma_compliance(self):
        Log.info('starting calculate_cma_compliance')
        try:
            tool_box = CMAToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))

    @log_runtime('SOVI CCBOTTLERUS_SANDCalculations')
    def calculate_sovi(self):
        Log.info('starting calculate_SOVI')
        try:
            tool_box = SOVIToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate SOVI due to: {}'.format(e.message))

    @log_runtime('CMA Compliance SW CCBOTTLERSUSCalculations')
    def calculate_cma_compliance_sw(self):
        Log.info('starting calculate_cma_compliance')
        try:
            tool_box = CMASOUTHWESTToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))

    @log_runtime('ARA CCBOTTLERSUSCalculations')
    def calculate_ara(self):
        Log.info('starting calculate_ara')
        try:
            tool_box = ARAToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))

    @log_runtime('MSC CCBOTTERSUSCalculations')
    def calculate_msc(self):
        Log.info('starting calculate_msc')
        try:
            tool_box = MSCToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate MSC KPIs due to: {}'.format(e.message))

    @log_runtime('LIBERTY CCBOTTERSUSCalculations')
    def calculate_liberty(self):
        Log.info('starting calculate_liberty')
        try:
            tool_box = LIBERTYToolBox(self.data_provider, self.output, self.common_db)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate LIBERTY KPIs due to: {}'.format(e.message))
