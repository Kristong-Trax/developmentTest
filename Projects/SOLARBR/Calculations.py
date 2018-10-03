
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.SOLARBR.KPIGenerator import Generator

__author__ = 'nicolaske'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')




if __name__ == '__main__':
    LoggerInitializer.init('SolarBr calculations')
    Config.init()
    project_name = 'solarbr'
    data_provider = KEngineDataProvider(project_name)
    output = Output()

    # second report
    list_sessions = sessions = ['476fbaf2-649e-45b4-8af5-6bc7ff12dfac',
                                'd64545ad-a607-44a7-b798-c575428ce061',
                                '19ede2c1-9732-4e19-8602-548f5d6f7660',
                                '1057bafc-17ad-473e-a1b8-f691f362614f',
                                '284dcd3d-2a5d-460e-ab03-f640a3ddfa33',
                                'c477daf2-72c9-48f2-a1a0-31089420cf5e',
                                'd130a190-71c5-4923-8730-cf62a76e44fc',
                                'cc31c106-ba1e-4b53-a02c-057b05c919a3',
                                '4adad5e3-86a0-11e8-97fe-12eff49f9640',
                                '17f376c6-86a1-11e8-97fe-12eff49f9640',
                                '53317075-86a1-11e8-97fe-12eff49f9640',
                                '7aa7698c-86a1-11e8-97fe-12eff49f9640',
                                '801c4931-023d-461a-86c2-1b4b73a24bb1',
                                'a95214f1-b130-4314-9832-aa4dea2637ac',
                                '3013aa79-cb11-4b66-9fd8-4fd09687bfef',
                                '5b43afa8-e9d3-4bd3-9992-8c09a7a47490',
                                '55427245-1e8c-4121-9d16-7cca73919ecf',
                                '77635f45-7d98-4c9c-8576-072e2a141f2b',
                                '3456fc7d-a4dd-4dc5-82c2-3e12a188498c',
                                ]


    for session in list_sessions:
        data_provider.load_session_data(session)
        Calculations(data_provider, output).run_project_calculations()
