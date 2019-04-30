
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.CCBOTTLERSUS.CMA.KPIToolBox import CCBOTTLERSUSCMAToolBox
from Projects.CCBOTTLERSUS.CMA_SOUTHWEST.KPIToolBox import CCBOTTLERSUSCMASOUTHWESTToolBox
from Projects.CCBOTTLERSUS.REDSCORE.KPIToolBox import CCBOTTLERSUSREDToolBox
from Projects.CCBOTTLERSUS.DISPLAYS.KPIToolBox import CCBOTTLERSUSDISPLAYSToolBox
from Projects.CCBOTTLERSUS.Utils.KPIToolBox import CCBOTTLERSUSBCIKPIToolBox
from Projects.CCBOTTLERSUS.REDSCORE.Const import Const
from Projects.CCBOTTLERSUS.WAREHOUSE_JUICE.KPIToolBox import CCBOTTLERSUSWAREHOUSEJUICEToolBox
from Projects.CCBOTTLERSUS.SOVI.KPIToolBox import SOVIToolBox
from Projects.CCBOTTLERSUS.MSC.KPIToolBox import MSCToolBox
from Projects.CCBOTTLERSUS.LIBERTY.KPIToolBox import LIBERTYToolBox

from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

__author__ = 'Elyashiv'


class CCBOTTLERSUSGenerator:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common_v2 = CommonV2(self.data_provider)

    @log_runtime('Total CCBOTTLERSUSCalculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        Common(self.data_provider).commit_results_data()
        self.calculate_red_score()
        # self.calculate_bci()
        # self.calculate_manufacturer_displays()
        self.calculate_cma_compliance()
        # self.calculate_cma_compliance_sw()
        self.calculate_warehouse_juice()
        self.calculate_sovi()
        # self.calculate_msc()
        self.calculate_liberty()
        self.common_v2.commit_results_data()  # saves results to new tables

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
            tool_box = CCBOTTLERSUSCMASOUTHWESTToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()  # saves to new tables
            # tool_box.commit_results()  # this currently deletes any previous results in report.kpi_level_2_results
        except Exception as e:
            Log.error('failed to calculate CMA Compliance due to :{}'.format(e.message))

    @log_runtime('Warehouse Juice CCBOTTLERSUSCalculations')
    def calculate_warehouse_juice(self):
        Log.info('starting calculate_warehouse_juice')
        try:
            tool_box = CCBOTTLERSUSWAREHOUSEJUICEToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()  # saves to new tables
            # tool_box.commit_results_without_delete()
            # tool_box.commit_results()
        except Exception as e:
            Log.error('failed to calculate Warehouse Juice due to :{}'.format(e.message))

    @log_runtime('SOVI CCBOTTLERSUSCalculations')
    def calculate_sovi(self):
        Log.info('starting calculate_sovi')
        try:
            tool_box = SOVIToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate SOVI due to :{}'.format(e.message))

    @log_runtime('MSC CCBOTTERSUSCalculations')
    def calculate_msc(self):
        Log.info('starting calculate_msc')
        try:
            tool_box = MSCToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate MSC Compliance due to: {}'.format(e.message))

    @log_runtime('LIBERTY CCBOTTERSUSCalculations')
    def calculate_liberty(self):
        Log.info('starting calculate_liberty')
        try:
            tool_box = LIBERTYToolBox(self.data_provider, self.output, self.common_v2)
            tool_box.main_calculation()
        except Exception as e:
            Log.error('failed to calculate LIBERTY KPIs due to: {}'.format(e.message))
