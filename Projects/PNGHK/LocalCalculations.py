
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGHK.Calculations import Calculations

__author__ = 'ilays'


if __name__ == '__main__':
    LoggerInitializer.init('pnghk calculations')
    Config.init()
    project_name = 'pnghk'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        '4f43c5ec-c6c9-4139-ac19-55334d17be74',
        '41a4abd4-0ee7-490a-868b-9043f162d252',
        '8e687c04-ac58-47a4-8000-bd8afd9382d0',
        '6dc13c96-5943-42f8-ad26-48f9e5617f0f',
        # 'ed5bc391-ce81-4d7e-b16d-1de5a573cbe0',  # has smart probe/69066 has hanger probe/69044 has stock
        # 'ad052993-9609-4aba-a927-dbaaff036280',  # has smart
        # '6c56a073-c8db-42aa-a491-c38b0c1c4086',  # has smart
        # 'dca92352-a549-4956-88b0-0811b35137c3',
    ]
    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()


