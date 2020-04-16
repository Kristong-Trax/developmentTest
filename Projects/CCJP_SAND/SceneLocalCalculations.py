import pandas as pd
from collections import OrderedDict

from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.CCJP_SAND.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS


def save_scene_item_facts_to_data_provider(data_provider, output):
    scene_item_facts_obj = output.get_facts()
    if scene_item_facts_obj:
        scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
    else:
        scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
    scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id',
                                     Fields.SCENE_FK: 'scene_id'}, inplace=True)
    data_provider.set_scene_item_facts(scene_item_facts)


if __name__ == '__main__':
    LoggerInitializer.init('CCJP Scene Calculations')
    Config.init()
    project_name = 'ccjp'
    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # # '2020-04-01' AND '2020-04-02'
        ('FF8A2423-D32D-4FA7-954A-F069E7694732', ['6B93F23D-DD9B-476F-BADE-585AC01B63BD']),
        ('F97A0890-9863-4FE9-96AC-3B63A1F421EF', ['3235A2C0-CB04-456B-82EB-1B8C1473EA8C']),
        ('CFC6A2C0-B5BF-47D8-9999-B8CB63C1EECE', ['99D67CE4-1C71-4BB6-AD01-0B2A0FC2BDF7']),
        ('C269A599-6012-4B00-9B98-0272F0D8217C', ['277C3CD1-AC00-4B98-8210-1CC2AD5A83BE']),
        ('BEC827F9-78B4-451A-BF8A-378B141FA934', ['1CD47679-9C74-4EB9-AEBD-ADCBBDE12173']),
        ('B6EAC6D5-1674-4DC5-A984-084A6C1C0A8A', ['040E1189-04D2-4EB1-B2A3-290EB69BD340']),
        ('B6942FA4-EF9B-4569-B764-34C62E727D4A', ['3C196C18-7A76-4A55-B16A-3D7D097CDD60']),
        ('92EC1CCE-734B-4C5F-91AA-5F3DA6D1061B', ['548B295F-308F-4738-92F3-8B8FBDC1D08E']),
        ('8CB720D1-4675-4C8B-871F-B5B6A59BDEB3', ['0F6CD90C-FBDF-46BE-B955-BD6709179F74']),
        ('89CA7721-E229-4A3D-98ED-7C6A6025AF08', ['3157D38F-FA9C-42C4-9DF6-902D77CF653F']),
        ('827437E1-A6D7-4074-BF22-220BF0B45553', ['AE8E7DBC-C2F8-4343-B686-E939F89DEF13']),
        ('783EC601-9433-4241-8ED7-1C04AB23C643', ['9447A958-E3B6-47A1-AFCF-132F812D3ECB']),
        ('73F564DB-E467-4593-A888-CB20E47DE8EA', ['489B457E-7399-4B9C-BB9B-CA4D25564F93',
                                                  '09C54153-856F-4E23-89CF-0ED144AED4C4']),
        ('708064D4-C194-4A58-A468-B3146022681D', ['0D87FD89-A792-4FB5-9354-C287F0A7BC8E']),
        ('5A4DFE32-AB2D-4C52-835D-B061917F5958', ['E8E422BB-1EC3-4D69-8AD5-E1FD11717CEC']),
        ('41D8CFD6-A5B4-4DF9-8824-3EFA6929A029', ['51AACDAC-7BD8-4C34-8C63-44F6D25BD90D']),
    ])

    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider = KEngineDataProvider(project_name)
            data_provider.load_scene_data(session, scene_uid=e_scene)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            output = VanillaOutput()
            SceneVanillaCalculations(data_provider, output).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider, output)
            SceneCalculations(data_provider).calculate_kpis()
#     sessions = [
#         '6DF2E0C8-8AE0-432A-AD3B-C2BE8F086E3B',
#         '6520B138-780D-4AD1-95CF-8DA1727C4580',
#         '6944F6F5-79D7-43FB-BE32-E3FAE237FA63',
#         '6FDB6757-E3DA-4297-941A-8C1A40DD2E90',
#         '9DC66118-F981-4D01-A6DD-1E181FA05507',
#         'E0BE9853-B6C8-4B36-919C-5293BF52EF5B'
#     ]
#     # RUN FOR Session level KPIs
#     for sess in sessions:
#         Log.info("[Session level] Running for session: {}".format(sess))
#         data_provider.load_session_data(sess)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
