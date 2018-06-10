import StringIO
import os
import shutil
import tarfile


class DeploymentUtils:
    def __init__(self):
        pass

    @staticmethod
    def make_tar_file_for_files(source_dir, files):
        tar_stream = StringIO.StringIO()
        with tarfile.open(mode="w|gz", fileobj=tar_stream) as tar:
            for file_ in files:
                tar.add(os.path.join(source_dir, file_), arcname=file_)
        return tar_stream

    @staticmethod
    def recursive_overwrite(src, dest, ignore=None):
        # files deleted in factory, are not deleted from live
        for root, dirs, files in os.walk(dest):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))
        if os.path.isdir(src):
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(src)
            if ignore is not None:
                ignored = ignore(files)
            else:
                ignored = set()
            for f in files:
                if f not in ignored:
                    DeploymentUtils.recursive_overwrite(os.path.join(src, f),
                                                        os.path.join(dest, f),
                                                        ignore)
        else:
            shutil.copyfile(src, dest)

    @staticmethod
    def save_file_stream(storage_connector, file_path, file_name, file_stream):
        file_stream.seek(0)
        storage_connector.save_file_stream(file_path, file_name, file_stream)
