
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.ALTRIAUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('altriaus calculations')
    Config.init()
    project_name = 'altriaus'
    sessions = [
        # 'A5629BC3-F3BD-4E16-BAA6-2DD046151C80',  # works!
        # 'F7011A7C-1BB6-4007-826D-2B674BD99DAE',  # works now, failed due to no smokeless section
        # '46EAE20D-ED3B-41F7-97D2-E8DEAABD4270',  # works now, reduced point matching threshold because jumps were big
        # '3EB3EEF7-CA5B-45D6-9D99-1D230B86ECC1',  # works now, added functionality to save empty positions - smokeless
        # '6479848B-7A28-49F1-8568-B1CF4CC2A52F',  # works!
        # '64345CEA-5A15-4919-8FB2-8A06C0DEA7EE',  # works now, added functionality to account for weird locations in
        # # the longest shelf, and smokeless section
        # 'C1822AEA-A997-414B-ADEF-5EA28BD3B915',  # works now, was fixed by accounting for empty smokeless section
        # '2987F99F-579B-4DD5-83A3-DB9C5C75FED5',  # works now, was fixed by accounting for empty smokeless section
        # '3FF91AAC-6008-4B4E-BAD1-1220C1A2A45D'  # works now, a lot was done to fix this
        # '6F94147F-E64C-48BC-8467-D610989CC5C9',
        # '873A646F-1EB1-4998-94CA-000B5AF88032',
        # '48D9F166-F4AF-493C-90C3-E6115E032362',
        # 'AB99B089-FF06-468D-B978-7CB32ECBFFC0',
        # '61C50C83-6A0F-4557-BA9B-EC64D4968118',
        # 'E8464DB4-CEF7-4ADA-85F4-5B60CDDF5FF8',
        # '78BC2765-C7B1-4FB3-8DFB-D82E93EBE02C',
        # 'DCC3F220-39A2-4DD2-AFE1-D6E766322FE4',
        # 'E27BEAB2-721D-44AB-9A6C-80F30C5B9CFD',
        # 'D918336C-B626-4B7C-9CFD-00E99FC1A03C',
        # '877E5A64-81FA-4995-9980-5CF0F20A4DE2',
        # 'C2704C6D-87D5-42EF-8D29-D134AB524471',
        # 'EBE1F5D0-BC2E-4B58-AE90-1E3DDD6523D4',
        # 'B9726241-0FC1-4073-ABA6-D7A1AEFFEA42',
        # '7C54538B-AD78-45D9-9B37-D61549C0B317',
        # 'E999EB3F-6AFD-4974-929C-C6859C9DBA30',
        # 'E999EB3F-6AFD-4974-929C-C6859C9DBA30',
        # '497A5772-F514-4216-A94F-78C76FB05722',
        # '06660FDB-E3C1-47DD-B351-2F620340C948'

    ]
    # 100 for Rifka
    sessions = [
        '4F1CE7F4-0639-4F60-AE5C-FC4983A37646',
        '5945AB19-2E0C-469F-BADA-903C22156510',
        'A6C5B8A7-0C4A-4B10-B36E-4B07BC9693E7',
        '2F76BB02-1C23-4FE7-BC70-8EF5700ABCFF',
        'E690D414-6AB4-4A2E-B9A1-9C029C936760',
        '44F2E9E1-6DA4-4017-81C2-CE1C936D8D30',
        '0723A18D-5BDF-4555-810A-3AA0A833F8AA',
        '8929E9AC-AEFB-45E0-B5DC-71C9132E0BA0',
        '1837D478-9FF5-45AE-BFF3-5CF89293B1C6',
        'C6212FA9-EF0F-4355-8C2F-02DF6E48A036',
        'A568500C-6F7C-456D-9E34-1762F2DC6050',
        '502CB94C-7569-4D03-9CEA-1C87B06FECE4',
        '5227D7E1-2733-41D6-8B4B-8C1E09CC53B2',
        'BEF481E7-F238-465F-8117-8264C33A0707',
        '34A4902D-8292-4A37-888A-478B23F18052',
        'F3968ABF-53E5-4F98-BFBC-B86F2894F7DA',
        '292322B9-8928-483B-8A5F-EE7D5FE94526',
        'DC19ACFB-6AF2-4E49-8BE7-59EBADE67ECD',
        'E93A6B72-A459-4D05-9E67-CD240A9A19C8',
        '9EBF93F2-DBF4-4F6B-A82F-0D65102072E6',
        '19E88C36-7789-4A61-B7C5-2217A6DE50A4',
        'F360D626-3D09-4E0A-AE0A-064DF8E8A783',
        '3B8C7415-C772-4107-8B65-6EE24EB1CDD0',
        '581EDDD5-B519-43C6-B627-8141A87B586F',
        'A7209FAF-9C24-4B70-BC0C-376F8C5FA9FA',
        '5F7956CD-09B7-490B-9DF7-BD80A9E7DF10',
        'A00A89B3-B7B3-41F7-A313-096E2EA31D75',
        'B280748E-8615-47C1-9345-CCC4234145B6',
        '701A9CC2-CBF8-4B94-84F3-B2F4420745B6',
        'D6AB249B-F914-4B2B-BA16-44333D80129B',
        '82E79F94-04DF-419F-8EA6-3B4E50A30E3F',
        'DCCAFED4-EBBC-4D64-A0E4-C7D8736FC4A2',
        '0B6B7E75-541B-477F-84F1-B3EFF4BAB818',
        '844A927E-7EA4-42DA-907A-B778A8E4D48F',
        '8E9241C2-56B6-4C77-8AA2-75D880689AC0',
        '3F249B37-FCDB-4C0A-87DA-A2FEDCBEF5AF',
        '435ADF21-170C-4AAF-916F-466C7C7663C7',
        'ECB052E3-7220-418D-BBC2-5B5DED52F497',
        '5AF7591A-EB41-47C0-9B4D-A8F2A4E60BFA',
        '912458D5-F2F7-4F0B-B05F-2032B0F05F30',
        '5DC20E79-085D-40E5-82E5-E4099BB47593',
        'A2919FF4-7695-42E1-8A7E-7FFEFE5CC040',
        'C96EE428-98CF-40CC-B2A4-6115D89D2DA4',
        '51527944-0ACC-4448-9258-3FE0FFAF5297',
        '5EC0D009-AEB4-4628-9B40-CD602FCE8E8A',
        '01AF83C1-1FDD-469B-9C1B-92CD76295F6B',
        'FF6F357E-54B2-40A8-8899-FD882B440FFF',
        '73FDE946-EB2A-46F8-B780-F14B8F3A4623',
        'E775F70E-C27F-4C4B-B8FF-2F64D4E837CF',
        '78054ACD-1D00-4824-B802-D0290E17AE45',
        '1ED707E5-2327-4D81-A8AE-C1441CAFACC3',
        '4EF96DBC-F445-47E2-93EE-4AF728BA1702',
        '722DB807-9517-4FA0-BD42-2262666176AC',
        '5FFD3098-0B6A-41A3-80F0-4F4E404EFC27',
        'AD68C550-3F9A-4142-BB11-86031616A11A',
        '2AF8AD9A-07F0-4A65-A6D6-84823F526053',
        '41555AFE-B203-4AC3-BD8F-23D191023ADF',
        '0127A61B-7D74-4133-8426-D4FE7E1C29BA',
        '7B15D440-D482-45CB-A8E6-CB220E3582CC',
        '870504EF-D17F-422C-80AF-33B28086A2A2',
        '0003B4DF-AB59-4CB1-815E-EF1AD58467FF',
        'A84DD8EA-46EA-4E9E-8A08-35E65F5F2D04',
        'F7FE95BF-CBF7-4FEF-9192-67CC384C39C6',
        'A60AB24F-FD73-42D7-8F23-0DFE9A958480',
        '727EAAA9-D43C-47CE-9B74-849EF3D7BAEA',
        '3BC8497C-7179-481D-8D0A-78593EB04A37',
        '322E6FE1-D8A5-472E-91EB-7E7E9AB70CE3',
        'A5D1A1AC-F2EE-4F54-AFE7-3B52C1313093',
        '6A78F96E-C1CB-4F41-9B0F-D7DE0129271A',
        '06B274E6-CD15-45CE-9398-51237DF60BAF',
        'C9D24CE1-323B-4657-B710-E7A704DFDDD2',
        '0D2DFB82-4698-400D-B391-C32AA132607D',
        'C59C4E7D-D013-4892-AC6C-003184BC4378',
        '55482F68-3A99-4F60-A6B9-59788667BBE5',
        '06A6E734-2F25-47C6-ADDE-4B09821B64EC',
        '10FA51A7-08D0-417E-AA59-5660C5A58B4B',
        '5B396E6F-7D9F-4900-A43F-2373BD399F9E',
        '0AF52ACE-67F7-4F71-AC73-B9524DC66155',
        '02A03670-ED2C-4440-881E-9491448F156B',
        'D270C6E4-5ACB-4F28-96F5-73500657E1D7',
        '830CB405-48A2-4D88-9B36-07725EA64D6C',
        'C2F7BAB9-9595-41E4-AF4A-4151E76AB13D',
        '22AC34C6-FB48-4785-9739-D5C028AB9830',
        '2767BF04-9C67-4903-9CA8-07C83762A9C1',
        '70DFE5C2-EE64-490E-9C79-E7912346E8E1',
        '3DAA117F-47E8-4A48-B9DD-E849B6886B4B'
    ]

    sessions = [
        '3B3A9AEF-3611-4A5F-9679-7DC64AC6136B',
'64F1175C-21C3-40AE-89A2-E478562F9DB8',
'C9929E3E-8346-4DD2-AB55-455BEF7AB3BA',
'F985ADAE-77A4-4F67-8957-DE78B780A360',
'A958A875-D9CA-47C8-AF5A-8E33CF94706E',
'3C8B9DC7-1B75-4697-BEDB-AE4EAD23D51F',
'89528011-8768-4891-9C67-45D951CAE0E2',
'2E70119B-591E-4170-9763-E3FB8D71B09E',
'EE0548FE-D81A-4E7E-A31F-F80B9E9CFFB3',
'60A147CF-7B77-4ECF-9E91-724F281C8B42'
    ]
    sessions = ['CF275CEB-3A7C-407C-8F0D-EB602A502848']
    # sessions = ['40E2DA30-FA6A-4B9C-B813-F2A18EE77555']


    for session in sessions:
        print('===================={}===================='.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()