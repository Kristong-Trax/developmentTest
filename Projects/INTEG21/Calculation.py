# import pandas as pd
# from Trax.Utils.Conf.Keys import DbUsers
# from mock import patch
#
# from KPIUtils.DB.Common import Common
from Projects.INTEG21.KPIGenerator import MarsUsGenerator
from Projects.INTEG21.Utils.ParseTemplates import ParseMarsUsTemplates
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log
#from Projects.INTEG21.TYSON.Utils.KPIToolBox import TYSONToolBox

import time


__author__ = 'nethanel'


class MarsUsCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        if True:
            self.timer.start()
            try:
                MarsUsGenerator(self.data_provider, self.output).main_function()
            except:
                Log.error('Mars US kpis not calculated')
            self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    # LoggerInitializer.init('TREX')
    LoggerInitializer.init('integ21')
    Config.init()
    project_name = 'integ21'

    sessions = ['2a85792a-69bc-423e-a44d-93516bd44341'
]


    for session in sessions:
        print
        print('*******************************************************************')
        print('--------------{}-------------'.format(session))
        s = time.time()
        Log.info('starting session : {}'.format(session))
        data_provider = KEngineDataProvider(project_name)
        # session = Common(data_provider).get_session_id(session)
        data_provider.load_session_data(session)
        output = Output()
        MarsUsCalculations(data_provider, output).run_project_calculations()
        print('session took {} minutes to calculate'.format((time.time() - s)/60.0))
