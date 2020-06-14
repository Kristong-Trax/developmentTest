import pandas as pd
from collections import OrderedDict
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
from Projects.LIONJP.SceneKpis.SceneCalculations import SceneCalculations
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
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
    LoggerInitializer.init('lionjp Scene Calculations')
    Config.init()
    project_name = 'lionjp'

    # RUN for scene level KPIs
    session_scene_map = OrderedDict([
        # ('0C8C1649-A253-4B1B-8E23-A85E73ADC0D5', ['F5F8B892-4745-4F40-88D7-3F2D03FB0DE7']),
        # ('930FC719-1587-4152-B693-1AC557E142DF', ['2A523332-7DE7-44B6-BD02-87672094B975'])
        # ('D88FCB40-691D-4A2B-8CC4-4F7C27138136', ['4C8654C4-48FE-44F7-A4EF-CBAE3311AE02'])
        # ('DE011375-AA8F-462C-9FB8-2E4565746D24', ['FDB7170E-A803-41DC-96F5-7443DD8A12E0']),
        # ('09F1FD67-4D3D-4372-BB52-7935EBED1EE3', ['DD01CA62-E5EA-44BE-8132-7EDCA264A862']),
        # ('01840BE1-9188-4046-9840-F7EBD9A87B5E', ['F44D78A6-8531-4A7C-AAE4-8EDDF93AF864']),
        # ('1ADCD7B6-F986-4ABF-8124-072807FEF3BD', ['02ECF7DC-8332-469F-B2FB-AB0F4EAE8292']),
        # ('2A999EFE-F9CE-428A-9D2F-A8736026FF2A', ['15F8E0D5-9069-455B-8776-9C7E1079D102']),
        # ('33A2366D-9E8C-4D19-8FE7-DF0FC60B6DDF', ['413DD81B-A2E6-404E-80C3-1632C076831E']),
        # ('371D6BD5-3776-465A-A35C-4FA9B4ED990B', ['2F9B80FF-9073-4F2D-8533-AE31B2D28759']),
        # ('4C95E976-1EA6-4BB6-8B6A-12907B23A170', ['3BB542FD-FF29-4BAE-8A9C-4BA77E382E64']),
        # ('5859CD3D-42F4-44EA-B8EA-D10403F39368', ['D20A81CA-B8C6-4AF7-BCA3-05A979265F17']),
        # ('5ADF45CB-BF59-4913-952C-0E818CBC5BE7', ['9CCBCADE-884A-45A7-BD44-14B150FB5E2B']),
        # ('70B1C1D7-29D7-4810-AA11-AADEA7468986', ['CC3B5160-C14B-440C-9E74-976399B2C210']),
        # ('8890E227-F63B-4400-A7F3-70675799F44A', ['A3F53464-CC75-4F7F-8866-32247AD8B8AC']),
        # ('930FC719-1587-4152-B693-1AC557E142DF', ['2A523332-7DE7-44B6-BD02-87672094B975']),
        # ('9B379C8E-A410-4388-AD36-F2F99F1B94F3', ['B988AE5C-07C0-478B-A098-670FAA7AD48F']),
        # ('A1D8AF90-7733-4E81-AF13-571B39C0B988', ['87649167-97DA-4E5A-A9B9-0E8715080EFA']),
        # ('A92E0448-EBB9-496B-AD5D-899D2A3A4967', ['88C74543-1193-46B5-8A84-EA5B25DDE24F']),
        # ('CCA06D71-37BB-488D-B61C-B4F53B3219AA', ['DCE6037F-9D7A-49E0-ABB5-9A8BFA1F4C4A']),
        # ('CCA06D71-37BB-488D-B61C-B4F53B3219AA', ['97CE3DB8-9EFF-4823-9813-53A699F91252']),
        # ('CCA06D71-37BB-488D-B61C-B4F53B3219AA', ['DAAF950D-3D57-49C6-BAC5-0113F44E5760']),
        # ('CCA06D71-37BB-488D-B61C-B4F53B3219AA', ['082D65CD-84FF-4456-ABA7-A3CCA1AACBE2']),
        # ('D88FCB40-691D-4A2B-8CC4-4F7C27138136', ['4C8654C4-48FE-44F7-A4EF-CBAE3311AE02']),
        # ('E0AA895B-CBE5-4859-8B01-F7EC93AF0069', ['93D90089-F539-4415-865D-F688EB2572BE']),
        # ('E75A7855-32A6-4EC6-921D-7BCAD3518173', ['5FF6F329-07C6-4279-9F21-7991A96C5DA6']),
        # ('EABC37D8-6E03-4AB8-AF94-C01076DCE0DB', ['624AB74F-8FC6-45A6-9D04-EA42119D79EB']),
        # ('ED435CF8-263B-437E-899A-FDADB7871D60', ['EDAA4CCF-2A12-4A65-A851-32E6A02C7330'])

    ])

    for session, scenes in session_scene_map.iteritems():
        for e_scene in scenes:
            print "\n"
            data_provider1 = KEngineDataProvider(project_name)
            data_provider1.load_scene_data(session, scene_uid=e_scene)
            Log.info("**********************************")
            Log.info('*** Starting session: {sess}: scene: {scene}. ***'.format(sess=session, scene=e_scene))
            Log.info("**********************************")
            output1 = VanillaOutput()
            SceneVanillaCalculations(data_provider1, output1).run_project_calculations()
            save_scene_item_facts_to_data_provider(data_provider1, output1)
            SceneCalculations(data_provider1).calculate_kpis()
