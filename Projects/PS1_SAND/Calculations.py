
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PS1_SAND.KPIGenerator import PS1SandGenerator


class PS1SandCalculation(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PS1SandGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('ps1-sand calculations')
#     Config.init()
#     project_name = 'ps1-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'F2AF67CE-89BF-48E5-8F24-6A14704582C9'
#     data_provider.load_session_data(session)
#     output = Output()
#     PS1SandCalculation(data_provider, output).run_project_calculations()
