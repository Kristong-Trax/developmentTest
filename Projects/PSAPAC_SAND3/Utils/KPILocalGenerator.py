from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from Projects.PSAPAC_SAND3.Utils.KPILocalToolBox import GSKLocalToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data
__author__ = 'prasanna'


class GSKLocalGenerator(GSKGenerator, object):
    def __init__(self, data_provider, output, common, set_up_file):
        super(GSKLocalGenerator, self).__init__(data_provider, output, common, set_up_file)
        self.tool_box = GSKLocalToolBox(self.data_provider, self.output, self.common, self.set_up_file)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

