
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Projects.PERNODUS.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    sessions = [
        '48592c46-5041-43b2-a547-0dab6414d9d8',
        '60868889-4bf6-4d4c-91ce-58f7e4cf075d',
        'a7f71b9c-f7cb-4094-aea9-5636dec9b3bc'
    ]

    LoggerInitializer.init('pernodus calculations')
    Config.init()
    data_provider = KEngineDataProvider('pernodus')

    print("start")
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

    print("done")
