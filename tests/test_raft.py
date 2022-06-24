#!/usr/bin/env python

import pytest
from hashlib import md5
from glob import glob
import os
import shutil
from pathlib import Path
import json
import sys
import re

from os.path import join as pjoin

import raft

class Args:
    pass

BASE_DIR = os.path.join(os.getcwd(), 't')
SCRIPTS_DIR = os.getcwd()

def setup_defaults(self, test_name):
    """
    """
    args = Args()
    args.default = True
    tmp_dir = pjoin(BASE_DIR, test_name)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)
    os.chdir(tmp_dir)
    raft.setup(args)


def teardown_instance(self, test_name):
    """
    """
    tmp_dir = pjoin(BASE_DIR, test_name)
    shutil.rmtree(tmp_dir, ignore_errors=True)

class TestSetup:
    def test_setup_defaults(self):
        """
        Test setup mode with -d/--default option.
        """
        args = Args()
        args.default = True
        tmp_dir = pjoin(BASE_DIR, 'test_setup_defaults')
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        raft.setup(args)
        os.chdir('..')
        shutil.rmtree(tmp_dir, ignore_errors=True)

#def test_setup_user_defined_dirs(self):
#    """
#    Test setup mode with user-specified directory.
#    """
#    pass

    def test_setup_cfg_creation(self):
        """
        """
        args = Args()
        args.default = True
        tmp_dir = pjoin(BASE_DIR, 'test_setup_cfg_creation')
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        raft.setup(args)
        os.chdir('..')
        capd_raft_cfg = glob(os.path.join(tmp_dir, '.raft.cfg'))[0]
        shutil.rmtree(tmp_dir, ignore_errors=True)
        assert capd_raft_cfg


    def test_setup_cfg_backup_and_creation(self):
        """
        """
        args = Args()
        args.default = True
        tmp_dir = pjoin(BASE_DIR, 'test_setup_cfg_creation')
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        raft.setup(args)
        raft.setup(args)
        os.chdir('..')
        capd_raft_cfg = glob(os.path.join(tmp_dir, '.raft.cfg'))[0]
        capd_raft_cfg_orig = glob(os.path.join(tmp_dir, '.raft.cfg.orig'))[0]
        shutil.rmtree(tmp_dir, ignore_errors=True)
        assert capd_raft_cfg
        assert capd_raft_cfg_orig

    def test_setup_chk_cfg_contents(self):
        """
        """
        args = Args()
        args.default = True
        tmp_dir = pjoin(BASE_DIR, 'test_setup_cfg_creation')
        shutil.rmtree(tmp_dir, ignore_errors=True)
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        raft.setup(args)
        os.chdir('..')
        capd_raft_cfg = glob(os.path.join(tmp_dir, '.raft.cfg'))[0]
        cfg_md5 = md5()
        with open(capd_raft_cfg, 'rb') as fo:
                cfg_md5.update(fo.read())
        cfg_md5 = cfg_md5.hexdigest()
        shutil.rmtree(tmp_dir, ignore_errors=True)
        assert cfg_md5 == '193c0ef03f2cc8eaf86f98a4f94ce3b2'

class TestInitProject:

    def test_init_project_default(self):
        """
        """
        test_name = 'test_init_project_init_cfg'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        os.chdir('..')
        dirs = [os.path.basename(x) for x in glob(os.path.join(tmp_dir, 'projects', test_name, '*'))]
        print(sorted(dirs))
        teardown_instance(self, test_name)
        assert sorted(dirs) == ['fastqs', 'indicies', 'logs', 'metadata', 'outputs', 'references', 'rftpkgs', 'tmp', 'work', 'workflow']

    def test_init_project_duplicate_project_id(self):
        """
        """
        with pytest.raises(SystemExit):
                test_name = 'test_init_project_duplicate_project_id'
                tmp_dir = pjoin(BASE_DIR, test_name)
                os.makedirs(tmp_dir)
                os.chdir(tmp_dir)
                setup_defaults(self, test_name)
                #Initial
                args = Args()
                args.init_config = pjoin(tmp_dir, '.init.cfg')
                args.project_id = test_name
                args.repo_url = ''
                raft.init_project(args)
                #Duplicate
                raft.init_project(args)
                os.chdir('..')
        teardown_instance(self, test_name)

    def test_init_project_nameless_project_id(self):
        """
        """
        with pytest.raises(SystemExit):
                test_name = 'test_init_project_nameless_project_id'
                tmp_dir = pjoin(BASE_DIR, test_name)
                os.makedirs(tmp_dir)
                os.chdir(tmp_dir)
                setup_defaults(self, test_name)
                #Initial
                args = Args()
                args.init_config = pjoin(tmp_dir, '.init.cfg')
                args.project_id = ''
                args.repo_url = ''
                raft.init_project(args)
                os.chdir('..')
        teardown_instance(self, test_name)

    def test_init_project_alt_init_cfg(self):
        """
        """
        test_name = 'test_init_project_alt_init_cfg'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        # Changing to alternate .init.cfg here...

        init_cfg = {"this_dir_indicates_an_alt_cfg": "",
                    "indicies": "",
                    "references": "",
                    "fastqs": "",
                    "tmp": "",
                    "outputs": "",
                    "workflow": "",
                    "work": "",
                    "metadata": "",
                    "logs": "",
                    "rftpkgs": "",
                    ".raft": ""}

        with open(pjoin(tmp_dir, '.init.alt.cfg'), 'w', encoding='utf8') as init_cfg_fo:
            json.dump(init_cfg, init_cfg_fo)

        args.init_config = pjoin(tmp_dir, '.init.alt.cfg')
        raft.init_project(args)
        os.chdir('..')
        dirs = [os.path.basename(x) for x in glob(os.path.join(tmp_dir, 'projects', test_name, '*'))]
        print(sorted(dirs))
        teardown_instance(self, test_name)
        assert sorted(dirs) == ['fastqs', 'indicies', 'logs', 'metadata', 'outputs', 'references', 'rftpkgs', 'this_dir_indicates_an_alt_cfg', 'tmp', 'work', 'workflow']

    def test_init_project_malformed_init_cfg(self):
        """
        """
        with pytest.raises(json.decoder.JSONDecodeError):
            test_name = 'test_init_project_malformed_init_cfg'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            # Changing to alternate .init.cfg here...

            init_cfg = {"this_is_a_malformed_cfg":"",
                        "indicies": "",
                        "references": "",
                        "fastqs": "",
                        "tmp": "",
                        "outputs": "",
                        "workflow": "",
                        "work": "",
                        "metadata": "",
                        "logs": "",
                        "rftpkgs": "",
                        ".raft": ""}

            with open(pjoin(tmp_dir, '.init.t.cfg'), 'w', encoding='utf8') as init_cfg_fo:
                json.dump(init_cfg, init_cfg_fo)

            with open(pjoin(tmp_dir, '.init.t.cfg'), encoding='utf8') as t_cfg_fo:
                with open(pjoin(tmp_dir, '.init.malf.cfg'), 'w', encoding='utf8') as malf_cfg_fo:
                    for line in t_cfg_fo.readlines():
                        line = line.partition(':')[:2]
                        malf_cfg_fo.write("{}\n".format(line))

            args.init_config = pjoin(tmp_dir, '.init.malf.cfg')
            raft.init_project(args)
        teardown_instance(self, test_name)

    def test_init_project_repo_url(self):
        """
        """
        pass

    def test_init_project_malformed_repo_url(self):
        """
        """
        pass

class TestLoadReference:
    def test_load_reference_standard(self):
        """
        """
        test_name = 'test_load_reference_standard'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
        raft.init_project(args)
        args = Args()
        args.file = 'test.fa'
        args.sub_dir = ''
        args.project_id = test_name
        args.mode = 'symlink'
        raft.load_reference(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'references', 'test.fa'))[0]
        teardown_instance(self, test_name)

    def test_load_reference_load_duplicate_ref(self):
        """
        """
        with pytest.raises(SystemExit):
            test_name = 'test_load_reference_load_duplicate_ref'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
            raft.init_project(args)
            args = Args()
            args.file = 'test.fa'
            args.sub_dir = ''
            args.project_id = test_name
            args.mode = 'symlink'
            # Initial loading
            raft.load_reference(args)
            # Loading again
            raft.load_reference(args)
        teardown_instance(self, test_name)

    def test_load_reference_load_nonspecific_ref(self):
        with pytest.raises(SystemExit):
            test_name = 'test_load_reference_load_nonspecific_ref'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            os.mkdir(pjoin(BASE_DIR, test_name, 'references', 'dup1'))
            os.mkdir(pjoin(BASE_DIR, test_name, 'references', 'dup2'))
            shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'dup1', 'test.fa'))
            shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'dup2', 'test.fa'))
            raft.init_project(args)
            args = Args()
            args.file = 'test.fa'
            args.sub_dir = ''
            args.project_id = test_name
            args.mode = 'symlink'
            raft.load_reference(args)
        teardown_instance(self, test_name)

    def test_load_reference_load_missing_ref(self):
        """
        """
        with pytest.raises(SystemExit):
            test_name = 'test_load_reference_load_missing_ref'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
            raft.init_project(args)
            args = Args()
            args.file = 'test2.fa'
            args.sub_dir = ''
            args.project_id = test_name
            args.mode = 'symlink'
            raft.load_reference(args)
        teardown_instance(self, test_name)

    def test_load_reference_load_to_subdir(self):
        """
        """
        pass
        test_name = 'test_load_reference_to_subdir'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
        raft.init_project(args)
        args = Args()
        args.file = 'test.fa'
        args.sub_dir = 'subdir_test'
        args.project_id = test_name
        args.mode = 'symlink'
        raft.load_reference(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'references', 'subdir_test', 'test.fa'))[0]
        teardown_instance(self, test_name)

    def test_load_reference_load_to_mult_subdirs(self):
        """
        """
        pass

    def test_load_reference_load_symlink(self):
        """
        """
        test_name = 'test_load_reference_standard'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        os.symlink(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
        raft.init_project(args)
        args = Args()
        args.file = 'test.fa'
        args.sub_dir = ''
        args.project_id = test_name
        args.mode = 'symlink'
        raft.load_reference(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'references', 'test.fa'))[0]
        teardown_instance(self, test_name)

    def test_load_reference_load_w_invalid_project_id(self):
        """
        """
        with pytest.raises(SystemExit):
            test_name = 'test_load_reference_w_invalid_project_id'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            shutil.copyfile(pjoin(SCRIPTS_DIR, 'data', 'references', 'test.fa'), pjoin(BASE_DIR, test_name, 'references', 'test.fa'))
            raft.init_project(args)
            args = Args()
            args.file = 'test.fa'
            args.sub_dir = ''
            args.project_id = 'this_project_doesnt_exist'
            args.mode = 'symlink'
            raft.load_reference(args)
        teardown_instance(self, test_name)

    def test_load_reference_chk_mounts_config(self):
        """
        """
        pass


class TestLoadMetadata:
    def test_load_metadata_standard(self):
        """
        """
        pass

    def test_load_metadata_load_duplicate_ref(self):
        """
        """
        pass

    def test_load_metadata_load_nonspecific_ref(self):
        """
        """
        pass

    def test_load_metadata_load_missing_ref(self):
        """
        """
        pass

    def test_load_metadata_load_to_subdir(self):
        """
        """
        pass

    def test_load_metadata_load_to_mult_subdirs(self):
        """
        """
        pass

    def test_load_metadata_load_symlink(self):
        """
        """
        pass

    def test_load_metadata_load_w_invalid_project_id(self):
        """
        """
        pass

    def test_load_metadata_chk_mounts_config(self):
        """
        """
        pass

class TestLoadModule:
    def test_load_module_standard(self):
        """
        """
        test_name = 'test_load_module_standard'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'salmon'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))[0]
        teardown_instance(self, test_name)

    def test_load_module_invalid_project_id(self):
        """
        """
        pass

    def test_load_module_chk_submodules(self):
        """
        """
        test_name = 'test_load_module_chk_submodules'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))[0]
        teardown_instance(self, test_name)

    def test_module_alt_repo(self):
        """
        """
        pass

    def test_load_module_alt_branch(self):
        """
        """
        pass

    def test_load_module_spec_dependency_alt_branch(self):
        """
        """
        pass

    def test_load_module_multi_primary_load(self):
        """
        """
        pass

    def test_load_module_multi_load_dependency(self):
        """
        If a module has already been loaded, then RAFT should skip it.
        """
        test_name = 'test_load_module_multi_primary_load'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'salmon'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        raft.load_module(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))[0]
        teardown_instance(self, test_name)

    def test_load_module_no_deps(self):
        """
        """
        test_name = 'test_load_module_no_deps'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = True
        args.delay = 15
        raft.load_module(args)
        assert len(glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))) == 0
        teardown_instance(self, test_name)

    def test_load_module_multi_modules(self):
        """
        """
        test_name = 'test_load_module_multi_modules'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'salmon'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        args.module = 'star'
        raft.load_module(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))[0]
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'star'))[0]
        teardown_instance(self, test_name)

    def test_load_module_alt_delay(self):
        """
        """
        test_name = 'test_load_module_multi_modules'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'salmon'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 30
        raft.load_module(args)
        assert glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'salmon'))[0]
        teardown_instance(self, test_name)

class TestAddStep:
    def test_add_step_valid_step(self):
        """
        """
        test_name = 'test_add_step_valid_step'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        args = Args()
        args.alias = ''
        args.subworkflow = 'main'
        args.project_id = test_name
        args.module = 'rna_quant'
        args.step = 'manifest_to_star_alns_salmon_counts'
        raft.add_step(args)
        with open(glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'main.nf'))[0]) as fo:
            assert any([re.search('manifest_to_star_alns_salmon_counts', x) for x in fo.readlines()])
        teardown_instance(self, test_name)

    def test_add_step_invalid_step(self):
        """
        """
        with pytest.raises(SystemExit):
            test_name = 'test_add_step_invalid_step'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            raft.init_project(args)
            args = Args()
            args.project_id = test_name
            args.repo = ''
            args.module = 'salmon'
            args.branches = 'main'
            args.no_deps = False
            args.delay = 15
            raft.load_module(args)
            args = Args()
            args.alias = ''
            args.subworkflow = 'main'
            args.project_id = test_name
            args.module = 'salmon'
            args.step = 'this_is_a_fake_step'
            raft.add_step(args)
        teardown_instance(self, test_name)

    def test_add_step_valid_multiple_times(self):
        """
        """
        with pytest.raises(SystemExit):
            test_name = 'test_add_valid_multiple_times'
            tmp_dir = pjoin(BASE_DIR, test_name)
            args = Args()
            args.init_config = pjoin(tmp_dir, '.init.cfg')
            args.project_id = test_name
            args.repo_url = ''
            os.makedirs(tmp_dir)
            os.chdir(tmp_dir)
            setup_defaults(self, test_name)
            raft.init_project(args)
            args = Args()
            args.project_id = test_name
            args.repo = ''
            args.module = 'rna_quant'
            args.branches = 'main'
            args.no_deps = False
            args.delay = 15
            raft.load_module(args)
            args = Args()
            args.alias = ''
            args.subworkflow = 'main'
            args.project_id = test_name
            args.module = 'rna_quant'
            args.step = 'manifest_to_star_alns_salmon_counts'
            raft.add_step(args)
            raft.add_step(args)
        teardown_instance(self, test_name)

    def test_add_step_check_mainnf_inclusion(self):
        """
        """
        test_name = 'test_add_step_valid_step'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        args = Args()
        args.alias = ''
        args.subworkflow = 'main'
        args.project_id = test_name
        args.module = 'rna_quant'
        args.step = 'manifest_to_star_alns_salmon_counts'
        raft.add_step(args)
        with open(glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'main.nf'))[0]) as fo:
            assert any([re.search("include { manifest_to_star_alns_salmon_counts } from './rna_quant/rna_quant.nf'", x) for x in fo.readlines()])
        teardown_instance(self, test_name)

    def test_add_step_check_mainnf_workflow(self):
        """
        """
        test_name = 'test_add_step_valid_step'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        args = Args()
        args.alias = ''
        args.subworkflow = 'main'
        args.project_id = test_name
        args.module = 'rna_quant'
        args.step = 'manifest_to_star_alns_salmon_counts'
        raft.add_step(args)
        with open(glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'main.nf'))[0]) as fo:
            assert any([re.search('manifest_to_star_alns_salmon_counts', x) for x in fo.readlines()])
        teardown_instance(self, test_name)


    def test_add_step_check_primary_parameters(self):
        """
        Primary parameters are implilcit "params" within the step being added.
        """
        pass

    def test_add_step_check_secondary_parameters(self):
        """
        Secondary parameters are implilcit "params" within any substeps of the step being added.
        """
        pass

    def test_add_step_check_invalid_project(self):
        """
        Secondary parameters are implilcit "params" within any substeps of the step being added.
        """
        pass

    def test_add_step_using_alias(self):
        test_name = 'test_add_step_using_alias'
        tmp_dir = pjoin(BASE_DIR, test_name)
        args = Args()
        args.init_config = pjoin(tmp_dir, '.init.cfg')
        args.project_id = test_name
        args.repo_url = ''
        os.makedirs(tmp_dir)
        os.chdir(tmp_dir)
        setup_defaults(self, test_name)
        raft.init_project(args)
        args = Args()
        args.project_id = test_name
        args.repo = ''
        args.module = 'rna_quant'
        args.branches = 'main'
        args.no_deps = False
        args.delay = 15
        raft.load_module(args)
        args = Args()
        args.alias = 'manifest_to_star_alns_salmon_counts_alt'
        args.subworkflow = 'main'
        args.project_id = test_name
        args.module = 'rna_quant'
        args.step = 'manifest_to_star_alns_salmon_counts'
        raft.add_step(args)
        with open(glob(pjoin(BASE_DIR, test_name, 'projects', test_name, 'workflow', 'main.nf'))[0]) as fo:
            assert any([re.search('manifest_to_star_alns_salmon_counts_alt', x) for x in fo.readlines()])
        teardown_instance(self, test_name)

    def test_add_step_using_taken_alias(self):
        """
        """
        pass

class TestRunWorkflow:

    def test_run_workflow_stock(self):
        """
        """
        pass

    def test_run_workflow_invalid_project(self):
        """
        """
        pass

    def test_run_workflow_no_resume(self):
        """
        """
        pass

    def test_run_workflow_no_resume(self):
        """
        """
        pass

    def test_run_workflow_keep_old_outputs(self):
        """
        """
        pass

    def test_run_workflow_pass_nf_params(self):
        """
        """
        pass

    def test_run_workflow_pass_nf_params(self):
        """
        """
        pass

class TestListSteps:
    def test_list_steps_invalid_project(self):
        """
        """
        pass

    def test_list_steps_entire_module(self):
        """
        """
        pass

    def tests_list_steps_single_step(self):
        """
        """
        pass

    def test_list_steps_invalid_module(self):
        """
        """

    def test_list_steps_invalid_step(self):
        """
        """
        pass

#class TestPackageProject:
#class TestLoadProject:


