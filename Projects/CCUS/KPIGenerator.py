
from Trax.Utils.Logging.Logger import Log

from Projects.CCUS.MONSTER.Utils.KPIToolBox import MONSTERToolBox
from Projects.CCUS.Utils.CCUSToolBox import log_runtime, CCUSToolBox
from Projects.CCUS.DISPLAYS.KPIToolBox import DISPLAYSToolBox
from Projects.CCUS.OBBO.KPIToolBox import OBBOToolBox
from Projects.CCUS.Programs.Utils.KPIToolBox import PROGRAMSToolBox
from Projects.CCUS.MSC_NEW.Utils.KPIToolBox import MSC_NEWToolBox
from Projects.CCUS.Holiday.Utils.KPIToolBox import HOLIDAYToolBox
from Projects.CCUS.GOLD_PEAK_BLOCK.Utils.KPIToolBox import GOLD_PEAK_BLOCKToolBox
from Projects.CCUS.SpecialPrograms.Utils.KPIToolBox import SpecialProgramsToolBox
from Projects.CCUS.Validation.Utils.KPIToolBox import VALIDATIONToolBox

__author__ = 'ortal'


class CCUSGenerator:
    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        # self.calculate_manufacturer_displays()
        # self.calculate_obbo()
        # self.calculate_dunkin_donuts()
        # self.calculate_monster()
        # self.calculate_programs()
        # self.calculate_holiday_programs()
        # self.calculate_msc_new()
        # self.calculate_gold_peak_block()
        # self.calculate_special_programs()
        self.calculate_validation()

    @log_runtime('Manufacturer Displays Calculations')
    def calculate_manufacturer_displays(self):
        tool_box = DISPLAYSToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data()

    @log_runtime('MONSTER Calculations')
    def calculate_monster(self):
        tool_box = MONSTERToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=27)

    # @log_runtime('OBBO Calculations')
    # def calculate_obbo(self):
    #     tool_box = OBBOToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data()
    #
    # @log_runtime('Dunkin Donuts Calculations')
    # def calculate_dunkin_donuts(self):
    #     set_name = 'Dunkin Donuts'
    #     tool_box = CCUSToolBox(self.data_provider, self.output)
    #     if tool_box.scif.empty:
    #         Log.warning('Scene item facts is empty for this session')
    #     tool_box.tools.update_templates()
    #     tool_box.main_calculation(set_name=set_name)
    #     tool_box.save_level1(set_name=set_name, score=100)
    #     set_fk = tool_box.kpi_static_data[tool_box.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
    #     tool_box.commit_results_data(kpi_set_fk=set_fk)
    #     Log.info('Dunkin Donuts: Downloading templates took {}'.format(tool_box.download_time))

    @log_runtime('Programs Calculations')
    def calculate_programs(self):
        tool_box = PROGRAMSToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=28)

    # @log_runtime('MSC New Calculations')
    # def calculate_msc_new(self):
    #     tool_box = MSC_NEWToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data(kpi_set_fk=29)

    @log_runtime('Holiday Programs Calculations')
    def calculate_holiday_programs(self):
        tool_box = HOLIDAYToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=31)

    # @log_runtime('Programs Calculations')
    # def calculate_gold_peak_block(self):
    #     tool_box = GOLD_PEAK_BLOCKToolBox(self.data_provider, self.output)
    #     tool_box.main_calculation()
    #     tool_box.commit_results_data(kpi_set_fk = 30)

    @log_runtime('Special Programs Calculations')
    def calculate_special_programs(self):
        tool_box = SpecialProgramsToolBox(self.data_provider, self.output)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=32)


    @log_runtime('Special Programs Calculations')
    def calculate_validation(self):
        tool_box = VALIDATIONToolBox(self.data_provider, self.output, kpi_set_fk=33)
        tool_box.main_calculation()
        tool_box.commit_results_data(kpi_set_fk=33)