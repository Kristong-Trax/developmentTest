from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru_sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = \
        [
            "F00DE0E2-D786-4F9C-9408-4C6894EE9180",

            # "f97d6aeb-1e6c-4f41-9da0-935f3a7bcb01",
            # "f2e5eb75-3bef-43d6-b195-02099f60c48b",
            # "f7389d84-2950-4558-b574-971ab6a578f6",
            # "FBAB77AA-096B-4219-9923-AC2BADAE50B2",
            # "F3A3B97A-EFF9-4987-BB29-DC431A0E463F",
            # "FE922953-560B-4774-8870-92BDCCF05165",
            #
            # "7821CA06-A1F0-40B6-BAAE-B7536DA542DD",
            # "9A651B97-79E3-418B-866C-5A2D865E5808",
            # "dd919f2e-708d-43e4-b135-7f0e1142049f",
            # "EEBC662F-49E4-40E7-A428-2887442B6D77",
            # "F5F6D755-6365-421E-94C9-833DC6B126D8",
            # "F64F2702-E189-41A8-84D0-413F39E864C8",
            # "f6b0c603-9110-4d8e-865a-e2291f96da96",
            # "F81BBDA5-DE76-44E7-B5B8-080D8B201ADA",
            # "F8DF043D-E194-4FF4-908D-9C0938842F84",
            # "f99c52d9-1df2-4b28-bae2-d96eec792702",
            # "f9dfb3e0-4b97-43ae-87ef-d634ebab6c2e",
            # "FB7CA2AD-B366-4239-9F9D-C8D56A5224A1",
            # "fb99e002-41d4-48a6-a878-97f7e5cf111a",
            # "fbe674e2-57af-427f-80c1-3814119dea10",
            # "fc8a97af-2009-4980-8424-ce7743de1662",
            # "fd115de0-57e9-4d99-bf6a-cb8afb976082",
            # "fd6f827a-ae5a-4836-92db-b9e9df5bb831",
            # "FD9BE7BE-89F5-482C-ADDF-8C6A712F0447",
            # "fdc3a9bb-86d8-4418-8854-6730d17200b8",
            # "fdd1daf3-e7bb-4aab-9fed-0cbe40c7ef0b",
            # "fdfda61b-71e1-4b3f-9e8b-94290b8d68e0",
            # "FE142E94-56AC-4E75-80E8-8734696307AB",
            # "fe174e96-a5d2-485c-b7fa-16636e725028",
            # "FE5AB555-75F1-4864-8BC4-A3A1304F8F8C",
            # "fe901d48-852a-4e68-9531-d8183071adcf",
            # "FEA06D94-C212-4E85-B171-2F452E8FCA4C",
            # "FED41DA9-5A9B-461D-8265-CFD65122C59C",
            # "FEFA5153-9A14-4B81-8F7A-BE9BCFFF2545",
            # "FFB50D22-404F-4787-9829-9A96F875B89A",
            # "FFBBF4DF-4EB0-4F03-86E8-D1347BB6527F",
            # "ffd61920-f5c5-49e9-b369-6282d02ca30a",
            # "FFD79C84-A2E7-4C03-9396-6DF16B622D4B",
            # "FFE17CEC-BE33-4889-92C9-0398DF99356D",
            # "FFF90413-B215-4FBA-9B9D-4B06E825879D",
        ]
    for session in session_uids:
        print(session)
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()

