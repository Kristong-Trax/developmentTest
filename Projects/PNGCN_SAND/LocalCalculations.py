#!/usr/bin/env python
# -*- coding: utf-8 -*

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.PNGCN_SAND.Calculations import PngCNEmptyCalculations
from Projects.PNGCN_SAND.SceneKpis.SceneCalculations import save_scene_item_facts_to_data_provider
from Projects.PNGCN_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
import pandas as pd

__author__ = 'ilays'

if __name__ == '__main__':
    LoggerInitializer.init('Png-cn calculations')
    Config.init()
    project_name = 'pngcn-sand'
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    sessions = {'d16a0359-4d0f-4fed-bfe4-0afecca6843e':[14431215, 14431231, 14431239]}
    for session in sessions.keys():
        print "Running for {}".format(str(session))
        for scene in sessions[session]:
            print('Calculating scene id: ' + str(scene))
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene)
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()
        data_provider.load_session_data(session)
        output = Output()
        PngCNEmptyCalculations(data_provider, output).run_project_calculations()