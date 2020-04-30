from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.BEIERSDORF.KPIGenerator import BEIERSDORFGenerator


__author__ = 'idanr'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        BEIERSDORFGenerator(self.data_provider).main_calculation(has_sos_sets=True)
        self.timer.stop('BeiersdorfToolBox.run_project_calculations')
