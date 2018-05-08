import abc

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import ACEDataProvider, Output
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Algo.Calculations.Core.Vanilla import SessionVanillaCalculations
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
#from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

__author__ = 'zeevs'


class AutomationIntCalculationsGroup(BaseCalculationsGroup):
    @abc.abstractproperty
    def run_calculations(self):
        raise NotImplementedError('This method must be overridden')


class DummyyyCalculations(AutomationIntCalculationsGroup):
    def run_calculations(self):
        Log.info('Starting Activation Products calculation for session_pk: {}.'.format(self.session_pk))
        # self.check_single_survey_response_number(fact_name='50%_Colas_tt',
        #                                          fact_desc='50%COLAS TT',
        #                                          eng_desc='Amount of GDMs (Refrigeretors) 50% '
        #                                                   'full of Cokes for Immediate Consume',
        #                                          question="50% Colas Totais",
        #                                          expected_answer=1)


class AutomationIntCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        Log.info("Activated AUTOMATION_INT specific calculations!")
        DummyyyCalculations(self.data_provider, self.output).run_calculations()


# if __name__ == '__main__':
#     LoggerInitializer.init('Automation Int calculations')
#     Config.init()
#     project_name = 'automation-int'
#     data_provider = ACEDataProvider(project_name)
#     data_provider.load_session_data('c3252b85-46d6-4ffe-b13a-8442e5c3302c')
#     output = Output()
#     SessionVanillaCalculations(data_provider, output).run_project_calculations()
#     AutomationIntCalculations(data_provider, output).run_project_calculations()
#     data_provider.export_session_data(output)
