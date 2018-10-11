
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.DIAGEOCO_SAND.KPIGenerator import DIAGEOCO_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'huntery'


class DIAGEOCO_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEOCO_SANDGenerator(self.data_provider, self.output).main_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_relative_position_function(set_templates_data)
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_block_together(kpi_name, set_templates_data)
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_share_of_shelf_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_touch_point_function(template_path)
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_secondary_display_secondary_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_brand_pouring_status_function(set_templates_data)
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_activation_standard_function(kpi_scores, set_scores, local_templates_path)
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')



