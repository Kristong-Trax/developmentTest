from __future__ import division

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCUS.MILITARY.KPIGenerator import MilitaryGenerator

__author__ = "trevaris"

test_sessions = [
    '1e55cbe5-027d-450a-92c6-bcbf31661dd7',
    '59e685be-0734-4b6f-8097-fcd09ef777b9',
]


def run_sessions(sessions):
    for i, session in enumerate(sessions, 1):
        run_session(session)
        complete = round(i / len(session) * 100)
        print("Completed {}% of sessions".format(round(complete)))


def run_session(session_uid):
    print("========== {} ==========".format(session_uid))
    data_provider.load_session_data(session_uid)
    output = Output()
    MilitaryGenerator(data_provider, output).main_function()


if __name__ == '__main__':
    LoggerInitializer.init('CCUS/Military Calculations')
    Config.init()
    project_name = 'ccus'
    data_provider = KEngineDataProvider(project_name)
    run_sessions(test_sessions)
