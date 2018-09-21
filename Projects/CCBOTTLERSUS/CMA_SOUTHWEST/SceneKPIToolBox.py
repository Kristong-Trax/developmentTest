import os
from datetime import datetime
import pandas as pd
import numpy as np

from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.CMA_SOUTHWEST.Const import Const
# from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider


from Trax.Algo.Calculations.Core.Utils import Validation, PandasUtils
from Trax.Algo.Calculations.Core.Utils import ToolBox as TBox

class SceneCommon(Common):
    def scene_specific(self, scene):
        self.scene_id = scene

class SceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.session_uid = self.data_provider.session_uid

        # self.data_provider = KEngineDataProvider(project_name)
        # self.output = output
        # self.project_name = project_name
        # self.session_uid = self.data_provider.session_uid

        # self.scene_tool_box = CCUSSceneToolBox(self.data_provider, self.output, self.common)

    # @log_runtime('Total Calculations', log_start=True)
    def scene_control(self, scenes, kpi_line, scif, num_filters, general_filters):
        """
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        """
        if not isinstance(scenes, list):
            scenes = [scenes]
        common = SceneCommon(self.data_provider)

        for scene in scenes:
            # self.data_provider.load_scene_data(self.session_uid, scene)
            common.scene_specific(scene)
            # scif = self.data_provider[Data.SCENE_ITEM_FACTS]
            scif = scif[scif['scene_fk'] == scene]
            if scif.empty:
                pass
                Log.warning('Match product in scene is empty for this scene')
            else:
                CCBOTTLERSUSSceneToolBox(self.data_provider, common).sos_with_num_and_dem(
                                                kpi_line, scif, num_filters, general_filters)
                common.commit_results_data(result_entity='scene')
                common.kpi_results = pd.DataFrame(columns=common.COLUMNS)


class CCBOTTLERSUSSceneToolBox:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.common = common
        from Projects.CCBOTTLERSUS.CMA_SOUTHWEST.KPIToolBox import CCBOTTLERSUSCMASOUTHWESTToolBox

        self.TBox = CCBOTTLERSUSCMASOUTHWESTToolBox(self.data_provider, None)
        # self.output = output


        # self.template_group = self.templates['template_group'].iloc[0]
        # self.scene_id = self.scene_info['scene_fk'][0]
        # self.kpi_fk = self.common.get_kpi_fk_by_kpi_name(Const.POC)
        # self.poc_number = 1

        # self.products = self.data_provider[Data.PRODUCTS]
        # self.templates = self.data_provider[Data.TEMPLATES]
        # self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        # self.match_product_in_scene = self.data_provider[Data.MATCHES]
        # self.visit_date = self.data_provider[Data.VISIT_DATE]
        # self.scene_info = self.data_provider[Data.SCENES_INFO]

    def sos_with_num_and_dem(self, kpi_line, relevant_scif, num_filters,  general_filters):

        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_line['KPI name'])

        num_scif = relevant_scif[self.TBox.get_filter_condition(relevant_scif, **num_filters)]
        den_scif = relevant_scif[self.TBox.get_filter_condition(relevant_scif, **general_filters)]

        try:
            Validation.is_empty_df(den_scif)
            Validation.is_empty_df(num_scif)
            Validation.df_columns_equality(den_scif, num_scif)
            Validation.is_subset(den_scif, num_scif)
        except Exception, e:
            msg = "Data verification failed: {}.".format(e)
            raise Exception(msg)
        num = num_scif[self.TBox.facings_field].sum()
        den = den_scif[self.TBox.facings_field].sum()

        # numerator = PandasUtils.num_of_rows(num_scif)
        # denominator = PandasUtils.num_of_rows(den_scif)
        ratio = num / float(den)
        # umerator_id=product_fk,
        self.common.write_to_db_result(fk=kpi_fk, numerator_result=num, denominator_result=den,
                                       result=ratio, by_scene=True)
        return num, ratio, den






# from KPIUtils_v2.DB.CommonV2 import Common
# self.common = Common(data_provider)
#
# self.common.commit_results_data(result_entity='scene')