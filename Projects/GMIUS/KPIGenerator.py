import os
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.GMIUS.Helpers.Result_Uploader import ResultUploader
from Projects.GMIUS.Helpers.Entity_Uploader import EntityUploader
from Projects.GMIUS.Helpers.Atomic_Farse import AtomicFarse

__author__ = 'Sam'


class Generator:
    # SUPER_CATS = ['Yogurt', 'RBG', 'Mexican', 'Soup']
    SUPER_CATS = ['Yogurt', 'RBG', 'Soup', 'Mexican']
    # SUPER_CATS = ['Mexican'] # Overwriting for testing purposes
    SUPER_CATS = ['Snacks']

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.common = Common(self.data_provider)
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.toolboxes = {}
        self.load_toolboxes()

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
            for cat in self.SUPER_CATS:
                if self.toolboxes[cat].scif.empty:
                    Log.warning('Distribution is empty for this session')
                    continue
                template_path = self.find_template(cat)
                # ResultUploader(self.project_name, template_path)
                # EntityUploader(self.project_name, template_path)
                # AtomicFarse(self.project_name, template_path)
                self.toolboxes[cat].main_calculation(template_path)
            self.common.commit_results_data()

    def load_toolboxes(self):
        ToolBox = 'imma lazy and no like red lines'
        for cat in self.SUPER_CATS:
            exec('from Projects.GMIUS.{0}.Utils.KPIToolBox import ToolBox'.format(cat))
            self.toolboxes[cat] = ToolBox(self.data_provider, self.output, self.common)

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

