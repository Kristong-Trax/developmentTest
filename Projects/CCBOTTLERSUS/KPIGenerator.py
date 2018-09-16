
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCBOTTLERSUS.CMA.KPIToolBox import CCBOTTLERSUSCMAToolBox
from Projects.CCBOTTLERSUS.CMA_SOUTHWEST.KPIToolBox import CCBOTTLERSUSCMASOUTHWESTToolBox
from Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox import CCBOTTLERSUSREDToolBox
from Projects.CCBOTTLERSUS.DISPLAYS.KPIToolBox import CCBOTTLERSUSDISPLAYSToolBox
from Projects.CCBOTTLERSUS.Utils.KPIToolBox import CCBOTTLERSUSBCIKPIToolBox
from Projects.CCBOTTLERSUS.REDSCORE.Const import Const

__author__ = 'Elyashiv'


class CCBOTTLERSUSGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

    @log_runtime('Total CCBOTTLERSUSCalculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        Common(self.data_provider).commit_results_data()
        # self.calculate_red_score()
        # self.calculate_bci()
        # self.calculate_manufacturer_displays()
        # self.calculate_cma_compliance()
        self.calculate_cma_compliance_sw()

    @log_runtime('Manufacturer Displays CCBOTTLERSUSCalculations')
    def calculate_manufacturer_displays(self):
        tool_box = CCBOTTLERSUSDISPLAYSToolBox(self.data_provider, self.output)
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        for kpi_set_name in set_names:
            if kpi_set_name == 'Manufacturer Displays':
                tool_box.main_calculation()
                set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == kpi_set_name][
                    'kpi_set_fk'].values[0]
                tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Bci CCBOTTLERSUSCalculations')
    def calculate_bci(self):
        tool_box = CCBOTTLERSUSBCIKPIToolBox(self.data_provider, self.output)
        if tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        tool_box.tools.update_templates()
        for kpi_set_name in set_names:
            if kpi_set_name == 'BCI':
                tool_box.main_calculation(set_name=kpi_set_name)
                tool_box.save_level1(set_name=kpi_set_name, score=100)
                Log.info('calculate kpi took {}'.format(tool_box.download_time))
                set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data[
                                                      'kpi_set_name'] == kpi_set_name]['kpi_set_fk'].values[0]
                tool_box.commit_results_data(kpi_set_fk=set_fk)

    @log_runtime('Red Score CCBOTTLERSUSCalculations')
    def calculate_red_score(self):
        Log.info('starting calculate_red_score')
        try:
            for calculation_type in Const.CALCULATION_TYPES:
                tool_box = CCBOTTLERSUSREDToolBox(self.data_provider, self.output, calculation_type)
                tool_box.main_calculation()
                tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CCBOTTLERSUS RED SCORE :{}'.format(e.message))

    @log_runtime('CMA Compliance CCBOTTLERSUSCalculations')
    def calculate_cma_compliance(self):
        Log.info('starting calculate_cma_compliance')
        try:
            tool_box = CCBOTTLERSUSCMAToolBox(self.data_provider, self.output)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))

    @log_runtime('CMA Compliance SW CCBOTTLERSUSCalculations')
    def calculate_cma_compliance_sw(self):
        Log.info('starting calculate_cma_compliance')
        try:
            tool_box = CCBOTTLERSUSCMASOUTHWESTToolBox(self.data_provider, self.output)
            tool_box.main_calculation()
            tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))
