import os
import shutil
import tempfile
from git import Repo


PROJECTS_FOLDER = 'Projects'
TMP_PROJECTS_FOLDER = 'Projects_tmp'

ACE_LIVE_FACTORY_REPO_NAME = 'trax_ace_live'
SDK_FACTORY_REPO_NAME = 'sdk_factory'
KPI_FACTORY_REPO_NAME = 'kpi_factory'
FACTORY_REPO_OWNER = 'traxtechnology'

SSH_CMD = r'ssh -i ~/.ssh/id_rsa'
GIT_MASTER_BRANCH = 'master'

KPI_UTILS_V2 = 'KPIUtils_v2'
KPI_UTILS = 'KPIUtils'
PROJECT_FOLDER = 'Projects'
OUT_OF_THE_BOX = 'OutOfTheBox'
DEVELOPER_TOOLS_FOLDER = 'DevloperTools'
SANITY_TESTS_FOLDER = 'SanityTests'


def create_new_tag(prefix, repo):
    last_tag = get_last_tag_for_prefix(prefix, repo)
    if last_tag:
        parts = str(last_tag).split('.')
        # Create new tag
        parts[len(parts) - 1] = str(int(parts[len(parts) - 1]) + 1)
        new_tag = '{}_v{}'.format(prefix.title(), '.'.join(parts))
    else:
        # Create new prefix initial tag
        new_tag = '{}_v1.0.0'.format(prefix.title())
    print "new tag={}".format(new_tag)
    return new_tag


def get_last_tag_for_prefix(prefix, repo):
    prefix_versioned = "{}{}".format(prefix.title(), '_v')
    tag_sub_list = [x for x in repo.tags if str(x).startswith(prefix_versioned)]
    if not tag_sub_list:
        return None

    version_list = [tag.name[tag.name.find('_v') + 2:] for tag in tag_sub_list]
    version_list.sort(key=lambda s: map(int, s.split('.')))
    return version_list[len(version_list) - 1]


def get_last_tag_on_current_commit(repo, prefix):
    """
    Verifies if a tag exists, returns it if it does
    :params project_name: project that is being deployed
    checks if the current commit is tagged with a tag that contains system_name.
    if it is, returns the tag. Otherwise returns None
    :return: the tag name, if it exists
    """
    repo.git.fetch()
    prefix_versioned = "{}{}".format(prefix.title(), '_v')
    # check that current commit is tagged, and starts_with_prefix is in the tag
    current_hash = repo.git.log("-n1", "--pretty=%H")
    tags = repo.git.tag("--points-at", current_hash).split("\n")
    return next((tag for tag in tags if tag.startswith(prefix_versioned)), None)


def latest_tags(repo):
    last_commit_date = repo.head.commit.committed_date
    last_tag_date = 0
    last_tag = None
    prev_tag = None
    prev_tag_date = 0
    for tag_reference in repo.tags:
        if tag_reference.commit.committed_date > last_commit_date:
            continue
        elif tag_reference.tag.tagged_date > last_tag_date:
            prev_tag = last_tag
            prev_tag_date = last_tag_date
            last_tag = tag_reference
            last_tag_date = tag_reference.tag.tagged_date
        elif tag_reference.tag.tagged_date > prev_tag_date:
            prev_tag = tag_reference
            prev_tag_date = tag_reference.tag.tagged_date
    return prev_tag, last_tag


def get_repository(repository_dir, url):
    print "get_repository: dir={}, url={}".format(repository_dir, url)
    try:
        if os.path.exists(repository_dir):
            shutil.rmtree(repository_dir)
        repo = Repo.init(repository_dir)
        origin = fetch_from_remote(repo, url)
        pull_from_repository(repo, origin)
        return repo
    except Exception as e:
        print 'Could not get repository: {}, exception: {}'.format(url, e)
        raise


def get_path_of_tmp_folder(repository_name):
    path = os.path.join(tempfile.gettempdir(), repository_name)
    return path


def get_sdk_factory_git_url():
    url = 'git@bitbucket.org:' + FACTORY_REPO_OWNER + '/' + SDK_FACTORY_REPO_NAME + '.git'
    return url


def get_live_git_url():
    url = 'git@bitbucket.org:' + FACTORY_REPO_OWNER + '/' + ACE_LIVE_FACTORY_REPO_NAME + '.git'
    return url


def get_kpi_factory_git_url():
    url = 'git@bitbucket.org:' + FACTORY_REPO_OWNER + '/' + KPI_FACTORY_REPO_NAME + '.git'
    return url


def get_live_git_folder():
    folder = ACE_LIVE_FACTORY_REPO_NAME
    path = os.path.join(tempfile.gettempdir(), folder)
    return path


def get_kpi_factory_repo():
    kpi_factory_local_folder = get_path_of_tmp_folder(KPI_FACTORY_REPO_NAME)
    repo = get_repository(kpi_factory_local_folder, get_kpi_factory_git_url())
    return kpi_factory_local_folder, repo


def get_sdk_factory_repository():

    # get path of tmp sdk_factory in local machine
    factory_git_folder = get_path_of_tmp_folder(SDK_FACTORY_REPO_NAME)
    # get path of sdk in remote machine
    factory_git_url = get_sdk_factory_git_url()
    print 'fetching sdk_factory repository from {} to {}'.format(factory_git_url, factory_git_folder)
    # get repository from remote
    repo = get_repository(factory_git_folder, factory_git_url)
    return factory_git_folder, repo


def get_kpi_factory_repository():
    # get path of tmp kpi_factory in local machine
    factory_git_folder = get_path_of_tmp_folder(KPI_FACTORY_REPO_NAME)
    # get path of kpi in remote machine
    factory_git_url = get_kpi_factory_git_url()
    print 'fetching kpi_factory repository from {} to {}'.format(factory_git_url, factory_git_folder)
    # get repository from remote
    repo = get_repository(factory_git_folder, factory_git_url)
    return factory_git_folder, repo


def get_live_repository():
    live_git_folder = get_live_git_folder()
    live_git_url = get_live_git_url()
    print 'fetching ace_live repository from {} to {}'.format(live_git_url, live_git_folder)
    live_repo = get_repository(repository_dir=live_git_folder, url=live_git_url)
    return live_git_folder, live_repo


def copy_to_ace_live(sdk_factory_folder, ace_live_folder, kpi_factory_folder=None, project_name=None):
    shutil.rmtree(os.path.join(ace_live_folder, KPI_UTILS))
    shutil.rmtree(os.path.join(ace_live_folder, KPI_UTILS_V2))
    shutil.rmtree(os.path.join(ace_live_folder, OUT_OF_THE_BOX))
    shutil.copytree(os.path.join(sdk_factory_folder, KPI_UTILS), os.path.join(ace_live_folder, KPI_UTILS))
    shutil.copytree(os.path.join(sdk_factory_folder, KPI_UTILS_V2), os.path.join(ace_live_folder, KPI_UTILS_V2))
    shutil.copytree(os.path.join(sdk_factory_folder, OUT_OF_THE_BOX), os.path.join(ace_live_folder, OUT_OF_THE_BOX))
    shutil.copytree(os.path.join(kpi_factory_folder, DEVELOPER_TOOLS_FOLDER, SANITY_TESTS_FOLDER),
                    os.path.join(ace_live_folder, DEVELOPER_TOOLS_FOLDER, SANITY_TESTS_FOLDER))
    if project_name is not None:
        # if it's a new project we won't have it in ace live
        if os.path.exists(os.path.join(ace_live_folder, PROJECT_FOLDER, project_name)):
            shutil.rmtree(os.path.join(ace_live_folder, PROJECT_FOLDER, project_name))
        shutil.copytree(os.path.join(kpi_factory_folder, PROJECT_FOLDER, project_name),
                        os.path.join(ace_live_folder, PROJECT_FOLDER, project_name))

def pull_from_repository(repo, origin):
    try:
        with repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
            if len(repo.refs) > 0:
                origin.pull(GIT_MASTER_BRANCH)
    except Exception as e:
        raise


def fetch_from_remote(repo, url):
    with repo.git.custom_environment(GIT_SSH_COMMAND=SSH_CMD):
        if len(repo.remotes) == 0:
            origin = repo.create_remote('origin', url)
        else:
            origin = repo.remote('origin')
        origin.fetch()
        return origin
