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
    sessions = [
                '6a08a4d4-118e-4629-ac1a-1e2abadf5767'
        # 'b8d2cc45-59c5-44c6-a69d-cbf813aec6fd',
                ]
                # '786d8a10-8016-4c2c-ade6-72385bd62782',
                # 'a98bc243-564b-4894-840f-e5ac980a1dd3',
                # '43b7b7db-9215-46f5-9797-be81e7cc76d4',
                # '448E6DA8-DFEC-4D4B-AC6D-FBBB2843192F',
                # 'a9b28c80-deab-44d9-a036-9d4238e26edb']
                # 'ebebc629-6b82-4be8-a872-0caa248ea248',
                # 'cb2cc33d-de43-4c35-a25b-ce538730037e']
    for session in sessions:
        print "Running for {}".format(session)
        for scene in [14429897]:
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