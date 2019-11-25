from Projects.MARSUAE.Calculations import MarsuaeCalculations
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript


class INTEG19ICalculations(BaseCalculationsScript):
    """
    This class is not needed for new KEngine calculations, so we can just leave it empty
    """
    def run_project_calculations(self):
        MarsuaeCalculations(self.data_provider, self.output, handler_name=None, monitor=None)\
            .run_project_calculations()