import os
import shutil
from distutils.dir_util import copy_tree


__author__ = 'yoava'

"""
this script takes a dependencies file as input and copies all relevant dependencies to a different directory for 
creating new package, and then uploads the newly created package to s3
after it's done, you can delete the dependency of theGarage in the python interpreter
"""


class Packager:

    THE_GARAGE = 'theGarage'
    TRAX_ACE_FACTORY = 'trax_ace_factory'
    SETUP_FILE = 'DevloperTools/pypi-package/setup.py'

    def __init__(self):
        self.rel_path = os.path.expanduser('~')
        self.garage_path = os.path.join(self.rel_path, 'dev', self.THE_GARAGE)
        self.ace_path = os.path.join(self.rel_path, 'dev', self.TRAX_ACE_FACTORY)
        self.setup_path = os.path.join(self.ace_path, self.SETUP_FILE)
        self.tmp_package_path = os.path.join(self.rel_path, 'Trax_tmp')
        self.readme_path = os.path.join(os.getcwd(), 'README')
        self.deps_file_path = self.create_deps_file()

    def create_imports_set(self):
        """
        this method creates set of all imports in repository
        :return: set
        """
        print "creating import set..."
        imports = set()
        with open(self.deps_file_path, 'r') as f:
            for row in f:
                if not row.__contains__('python2.7') and not row.__contains__('None') and \
                        row.__contains__(self.THE_GARAGE):
                    py_class = row.split(',')[3][:-3].strip()[1:-1]
                    if py_class != 'on':
                        imports.add(py_class)

        imports = sorted(imports)
        print "import set created..."
        return imports

    def copy_imports(self, imports):
        """
        this method copies all imports from theGarage to the new path
        :param imports:
        :return: None
        """
        print "copying imports..."
        for im in imports:
            cur_path = os.path.join(self.garage_path, im)
            if os.path.exists(cur_path):
                new_path = cur_path.replace(self.garage_path, self.tmp_package_path)
                if not str(new_path).endswith('.py'):
                    path_without_file = new_path.split('/')
                else:
                    path_without_file = new_path.split('/')[:-1]
                if not os.path.exists('/'.join(path_without_file)):
                    os.makedirs('/'.join(path_without_file))
                try:
                    shutil.copy(cur_path, '/'.join(path_without_file))
                except IOError:
                    pass
        print "imports copied..."

    def create_deps_file(self):
        """
        this method creates dependencies file using snakefood
        :return: text file
        """
        if not os.path.exists(os.path.join(os.getcwd(), 'Dependencies')):
            os.makedirs(os.path.join(os.getcwd(), 'Dependencies'))
        deps_file = os.path.join(os.getcwd(), 'Dependencies', 'deps.txt')
        os.system('sfood --recursive ' + self.ace_path + ' > ' + deps_file)
        return deps_file

    def copy_init_files(self):
        """
        this method copies all the 'init.py' files from theGarage
        :return: None
        """
        print "copying init files..."
        for subdir, dirs, files in os.walk(self.garage_path):
            for dir_name in dirs:
                path = os.path.join(subdir, dir_name)
                init_file = os.path.join(path, '__init__.py')

                new_path = path.replace(self.garage_path, self.tmp_package_path)
                if os.path.exists(new_path):
                    if os.path.exists(init_file):
                        shutil.copyfile(init_file, os.path.join(new_path, '__init__.py'))
        print "init files copied"

    def copy_secret_keys_file(self):
        """
        this method copies the.conf file because it's needed but it's not imported anywhere
        :return: None
        """
        shutil.copyfile(os.path.join(self.garage_path, 'Trax', 'Utils', 'Conf', 'secret_keys.conf'),
                        os.path.join(self.tmp_package_path, 'Trax', 'Utils', 'Conf', 'secret_keys.conf'))

    def copy_setup_path(self):
        """
        this method copies the setup.py file to the package path
        notice to change version
        :return: None
        """
        shutil.copyfile(self.setup_path, os.path.join(self.tmp_package_path, 'setup.py'))

    def copy_readme(self):
        """
        this method copies the readme file to the package path
        :return: None
        """
        shutil.copyfile(self.readme_path, os.path.join(self.tmp_package_path, 'README.txt'))

    def upload_to_s3(self):
        """
        this method uploads the package to s3
        :return: None
        """
        os.chdir(self.tmp_package_path)
        os.system('s3pypi --bucket trax-pypi')

    def create_and_upload_package(self):
        imports_set = self.create_imports_set()
        self.copy_imports(imports_set)
        self.copy_secret_keys_file()
        self.copy_init_files()
        self.copy_setup_path()
        self.copy_readme()
        self.upload_to_s3()

    def delete_tmp(self):
        os.remove(self.deps_file_path)
        shutil.rmtree(self.tmp_package_path)


class KPIUtilsGenerator:

    TRAX_ACE_FACTORY = 'trax_ace_factory'
    KPI_UTILS_V2 = 'KPIUtils_v2'
    SETUP = 'setup.py'

    def __init__(self):
        self.rel_path = os.path.expanduser('~')
        self.ace_path = os.path.join(self.rel_path, 'dev', self.TRAX_ACE_FACTORY)
        self.kpi_path = os.path.join(self.ace_path, self.KPI_UTILS_V2)
        self.tmp_package_path = os.path.join(self.rel_path, 'KPIUtils_tmp')

    def copy_package_to_tmp(self):
        if not os.path.exists(self.tmp_package_path):
            os.makedirs(os.path.join(self.tmp_package_path, self.KPI_UTILS_V2))
        copy_tree(self.kpi_path, os.path.join(self.tmp_package_path, self.KPI_UTILS_V2))
        shutil.move(os.path.join(self.tmp_package_path, self.KPI_UTILS_V2, self.SETUP),
                    os.path.join(self.tmp_package_path, self.SETUP))
        shutil.move(os.path.join(self.tmp_package_path, self.KPI_UTILS_V2, 'README'),
                    os.path.join(self.tmp_package_path, 'README.txt'))

    def upload_to_s3(self):
        """
        this method uploads the package to s3
        :return: None
        """
        os.chdir(self.tmp_package_path)
        os.system('s3pypi --bucket trax-pypi')

    def create_and_upload_package(self):
        """
        this method creates and uploads the package to s3
        :return: None
        """
        self.copy_package_to_tmp()
        self.upload_to_s3()


if __name__ == '__main__':
    packager = Packager()
    packager.create_and_upload_package()
    utils_maker = KPIUtilsGenerator()
    utils_maker.create_and_upload_package()
