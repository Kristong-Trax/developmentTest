from Projects.RINIELSENUS.Calculation import MarsUsCalculations
from Trax.Algo.Calculations import KpiCalculation, KpiResult
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.DesignPatterns.Decorators import classproperty
from Trax.Utils.Logging.Logger import Log


class MyCustomCalculation(KpiCalculation):
    """
    This class provides a reference on how to implement KPI calculation.
    In this example, the KPI calculates average facings number of every manufacturer
    """

    @classproperty
    def kpi_type(cls):
        return 'MY_CUSTOM_CALCULATION'

    @classproperty
    def kpi_supported_actions(cls):
        """
        Gets the list of the actions current KPI supports
        :return: The list of supported actions
        :rtype: List[type]
        """
        return []

    def calculate(self):
        """
        Calculates the kpi and returns list of results.
        :return: The list containing kpi calculation results
        :rtype: list[Trax.Algo.Calculations.KpiResult]
        """
        Log.info('Calculating {}'.format(self.kpi_type))
        return []


class INTEG17INBEVBECalculations(BaseCalculationsScript):
    """
    This class is not needed for new KEngine calculations, so we can just leave it empty
    """
    def run_project_calculations(self):
        Log.info('Inside integ17 project specific calculations')
        MarsUsCalculations(self.data_provider, self.output).run_project_calculations()