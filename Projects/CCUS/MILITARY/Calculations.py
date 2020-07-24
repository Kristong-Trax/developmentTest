from __future__ import division

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCUS.MILITARY.KPIGenerator import MilitaryGenerator

__author__ = "trevaris"

test_sessions = [
    'ecf47463-bf81-43da-8078-6d9c5caa074c',
    '97304f07-2ac1-4f1a-bb08-a57ca625aaa6',
    '92945c9b-ec85-404b-a7c8-dc5fcb3b7180',
    '7cd9609e-40cb-48fe-aade-04470bbcd082',
]


def run_sessions(sessions):
    for i, session in enumerate(sessions, 1):
        run_session(session)
        complete = round(i / len(sessions) * 100, 2)
        print("Completed {}% of sessions".format(round(complete)))


def run_session(session_uid):
    print("==================== {} ====================".format(session_uid))
    data_provider.load_session_data(session_uid)
    output = Output()
    MilitaryGenerator(data_provider, output).main_function()


if __name__ == '__main__':
    LoggerInitializer.init('KEngine')
    Config.init('KEngine')
    project_name = 'ccus'
    data_provider = KEngineDataProvider(project_name)
    run_sessions(test_sessions)
