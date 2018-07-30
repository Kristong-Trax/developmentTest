
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CBCIL.KPIGenerator import CBCILCBCIL_PRODGenerator

__author__ = 'Israel'


class CBCILCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CBCILCBCIL_PRODGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('cbcil calculations')
    Config.init()
    project_name = 'cbcil'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 'fd334fd5-5d8d-449d-989b-0a69f92b829a'
        '95513285-4c78-4c91-85dd-cae2ff83bbf5' #tested and changed
       # 'a0ba8c7d-7dec-40c4-9730-eaea3edcfc95' # tested
       #  '4bc4c803-c1cb-4a0f-a753-3da1b7f94216' # tested
       #  '9a8f1598-ca3b-4cf6-a952-c7d025d5c517'# tested
       #  'd12393f4-0a76-4959-9d10-40453ead32d0' # tested
       #  '5059e40d-19ed-4ade-ae0d-92ef9c9b0ff7' #tested
        # '7e752f99-a079-41d2-bcef-850c634656ea',
        # '9a8f1598-ca3b-4cf6-a952-c7d025d5c517',

                # '2629c373-438e-45e3-bef5-35f4d8ae292d',
    #             'A6897C11-57AE-49D8-B5FC-A20857724F5E',
    #             '3E2F7A4F-201E-46BA-A2EB-049711D24CFD',
    #             '60A89D1E-9153-4C8D-B7AF-3302E87C3090',
    # # new 26-03
    #             '570795E7-94DA-45C9-9B41-0B3525C5B739',
    #             '862351E6-0596-4F00-ABF8-C563CE81489D',
    #     '4e949763-2365-11e8-afa3-12c205a16538',
    #     '60A89D1E-9153-4C8D-B7AF-3302E87C3090',
    #     '46EB75B2-82FB-44C5-A66A-75F29030DD9C',
    #     'C66F8398-03AE-4284-A48B-3A41FAE20FBD',
    #     '3E2F7A4F-201E-46BA-A2EB-049711D24CFD',
    #     'A6897C11-57AE-49D8-B5FC-A20857724F5E',
    #     '570795E7-94DA-45C9-9B41-0B3525C5B739',
    #     '862351E6-0596-4F00-ABF8-C563CE81489D',
    #     '8D710298-CBDE-4036-B4F5-0BC2B5F7C87D',
    #     '0a68175e-3300-11e8-afa3-12c205a16538',
    #     '451adfe4-3300-11e8-afa3-12c205a16538',
    #     '8e591b4f-3300-11e8-afa3-12c205a16538',
    #     'f5ce62d3-3300-11e8-afa3-12c205a16538',
    #     '6d376f1d-3301-11e8-afa3-12c205a16538',
    #     'b64a4d32-3301-11e8-afa3-12c205a16538',
    #     'f31a907c-3301-11e8-afa3-12c205a16538',
    #     '2a8f3b1d-3302-11e8-afa3-12c205a16538',
    #     '8f851850-3306-11e8-afa3-12c205a16538',
    #     '1c424df6-33fa-11e8-afa3-12c205a16538',
    #     '5e73a17d-33fa-11e8-afa3-12c205a16538',
    #     'a8afa5fd-33fa-11e8-afa3-12c205a16538',
    #     'f6fa8506-33fa-11e8-afa3-12c205a16538',
    #     '6bde4fb1-33fb-11e8-afa3-12c205a16538',
    #     'b289964a-33fb-11e8-afa3-12c205a16538',
    #     '93f738d0-3628-11e8-afa3-12c205a16538',
    #     '6b3ca2ee-3629-11e8-afa3-12c205a16538',
    #     'de65c281-3629-11e8-afa3-12c205a16538',
    #     '1b4e70da-362b-11e8-afa3-12c205a16538',
    #     'c5d38244-3700-11e8-afa3-12c205a16538',
    #     'e6048c54-3700-11e8-afa3-12c205a16538',
    #     '059653a0-3701-11e8-afa3-12c205a16538',
    #     '301d42d8-3701-11e8-afa3-12c205a16538',
    #     '72ef47b8-3701-11e8-afa3-12c205a16538',
    #     '9c0b448f-3701-11e8-afa3-12c205a16538',
    #     'be34b7e3-3701-11e8-afa3-12c205a16538',
    #     'e2b87b30-3701-11e8-afa3-12c205a16538',
    #     '057ec850-3702-11e8-afa3-12c205a16538',
    #     '60557b7a-3702-11e8-afa3-12c205a16538',
    #     'ee8e48a1-3703-11e8-afa3-12c205a16538',
    #     '7cb1231c-3894-11e8-afa3-12c205a16538',
    #     '6efbb763-3896-11e8-afa3-12c205a16538',
    #     '3c698d03-389d-11e8-afa3-12c205a16538',
    #     '7f9f702d-389d-11e8-afa3-12c205a16538',
    #     '96a18c63-389d-11e8-afa3-12c205a16538',
    #     'ae3840ac-389d-11e8-afa3-12c205a16538',
    #     'da45f255-389d-11e8-afa3-12c205a16538',
    #     '019edf3e-389e-11e8-afa3-12c205a16538',
    #     '4ec27211-389e-11e8-afa3-12c205a16538',
    #     '5fadd8b9-389e-11e8-afa3-12c205a16538',
    #     '7367f47b-389e-11e8-afa3-12c205a16538',
    #     '082b0e75-395b-11e8-afa3-12c205a16538',
    #     '134fca96-395c-11e8-afa3-12c205a16538',
    #     '662cd160-395e-11e8-afa3-12c205a16538',
    #     '1959cda0-395e-11e8-afa3-12c205a16538',
    #     '93035467-3976-11e8-afa3-12c205a16538',
    #     '1b5f5889-3977-11e8-afa3-12c205a16538',
    #     'e948d22b-3975-11e8-afa3-12c205a16538',
    #     '1f173d6a-3a25-11e8-afa3-12c205a16538',
    #     '39323ed7-3a25-11e8-afa3-12c205a16538',
    #     '582d5291-3a25-11e8-afa3-12c205a16538',
    #     '75652551-3a25-11e8-afa3-12c205a16538',
    #     '9b01fc1d-3a25-11e8-afa3-12c205a16538',
    #     '8728e251-3a25-11e8-afa3-12c205a16538',
    #     'b964a569-3a25-11e8-afa3-12c205a16538',
    #     'd113925c-3a25-11e8-afa3-12c205a16538',
    #     'e5b13c22-3a25-11e8-afa3-12c205a16538',
    #     'be31a20b-3a27-11e8-afa3-12c205a16538',
    #     'd1381ab9-3a27-11e8-afa3-12c205a16538',
    #     '535415d2-3a28-11e8-afa3-12c205a16538',
    #     'b291a474-3a28-11e8-afa3-12c205a16538'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        CBCILCalculations(data_provider, output).run_project_calculations()

