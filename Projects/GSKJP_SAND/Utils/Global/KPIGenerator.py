# from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox
from Projects.GSKJP_SAND.Utils.Global.KPIToolBox import GSKToolBox
from KPIUtils.GlobalProjects.GSK.Data.LocalConsts import Consts
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Trax.Utils.Logging.Logger import Log



__author__ = 'prasanna'


class GSKGenerator:

    def __init__(self, data_provider, output, common,set_up_file):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = common
        self.set_up_file = set_up_file
        self.tool_box = GSKToolBox(self.data_provider, self.output, self.common,self.set_up_file)
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]

    @log_runtime('Total Calculations', log_start=True)
    def gsk_global_linear_sos_by_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.CATEGORY_KPI_SUFFIX, Consts.SOS_LINEAR)
        except Exception as e:
            Log.error('{}'.format(e))

    def gsk_global_facings_sos_by_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.CATEGORY_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def gsk_global_linear_sos_whole_store_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.STORE_KPI_SUFFIX, Consts.SOS_LINEAR)
        except Exception as e:
            Log.error('{}'.format(e))

    def gsk_global_facings_sos_whole_store_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.STORE_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def gsk_global_linear_sos_by_sub_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.SUB_CATEGORY_KPI_SUFFIX, Consts.SOS_LINEAR)
        except Exception as e:
            Log.error('{}'.format(e))

    def gsk_global_facings_by_sub_category_function(self):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.main_sos_calculation(Consts.SUB_CATEGORY_KPI_SUFFIX, Consts.SOS_FACINGS)
        except Exception as e:
            Log.error('{}'.format(e))

    def availability_store_function(self):

        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.availability_calculation(Consts.STORE)
        except Exception as e:
            Log.error('{}'.format(e))

    def availability_category_function(self):

        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.availability_calculation(Consts.CATEGORY)
        except Exception as e:
            Log.error('{}'.format(e))

    def availability_subcategory_function(self):
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.availability_calculation(Consts.SUB_CATEGORY)
        except Exception as e:
            Log.error('{}'.format(e))

    def get_assortment_data_provider(self):
        try:
            if self.tool_box.scif.empty:
                Log.warning('Scene item facts is empty for this session')
            return self.tool_box.get_assortment_provider()
        except Exception as e:
            Log.error('{}'.format(e))
