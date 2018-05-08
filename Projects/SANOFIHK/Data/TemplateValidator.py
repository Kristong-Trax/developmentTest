import os
from KPIUtils.GlobalProjects.SANOFI.Utils.TemplateValidator import SanofiTemplateValidator


if __name__ == '__main__':
    # get directory path
    dir_path = os.path.dirname(os.path.realpath(__file__))
    SanofiTemplateValidator(dir_path).run()
