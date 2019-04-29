import os
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.GMIUS.Utils.KPIToolBox import ToolBox
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.GMIUS.Helpers.Result_Uploader import ResultUploader
from Projects.GMIUS.Helpers.Entity_Uploader import EntityUploader
from Projects.GMIUS.Helpers.Atomic_Farse import AtomicFarse

__author__ = 'Sam'


class Generator:
    # SUPER_CATS = ['Yogurt', 'RBG', 'Mexican', 'Soup']
    # SUPER_CATS = ['Yogurt', 'RBG', 'Soup']
    SUPER_CATS = ['Yogurt', 'Soup'] # Overwriting for testing purposes

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = ToolBox(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        if self.tool_box.scif.empty:
            Log.warning('Distribution is empty for this session')
        else:
            for cat in self.SUPER_CATS:
                template_path = self.find_template(cat)
                # ru = ResultUploader(self.project_name, template_path)
                # eu = EntityUploader(self.project_name, template_path)
                # af = AtomicFarse(self.project_name, template_path)
                self.tool_box.main_calculation(template_path)
            self.common.commit_results_data()

    def find_template(self, cat):
        ''' screw maintaining 4 hardcoded template paths... '''
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data')
        files = os.listdir(path)
        candidates = [f for f in files if f.split(' ')[0] == cat and f.split('.')[-1] == 'xlsx']
        versioned_candidates = []
        all_vers = []
        for x in candidates:
            all_vers += x.split(' v')[-1].replace('.xlsx', '').split('.')
        max_digits = len(max(all_vers, key=len))
        for x in candidates:
            version_comps = x.split(' v')[-1].replace('.xlsx', '').split('.')
            normed_components = []
            for i, comp in enumerate(version_comps):
                norm_comp = int(comp) * 10**max_digits
                normed_components.append(str(norm_comp))
            versioned_candidates.append((float(''.join(normed_components)), x))
        template = sorted(versioned_candidates, key=lambda x: x[0])[-1][1]
        return os.path.join(path, template)

