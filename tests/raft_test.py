#!/usr/bin/env python

import pytest
from hashlib import md5
from glob import glob
import os
import shutil
from pathlib import Path

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
        assert cfg_md5 == '6b3069f3151a33cfd53773ace7a17744'

class TestInitProject:

    def test_init_project_default(self):
        """
        """
        test_name = 'test_init_project_default'
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
        dirs = [os.path.basename(x) for x in glob(os.path.join(tmp_dir, '*'))]
        teardown_instance(self, test_name)
        assert dirs == ['work', 'projects', 'references', 'fastqs', 'imgs', 'metadata', 'shared']
        
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
        pass
        
    def test_init_project_malformed_init_cfg(self):
        """
        """
        pass

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
        pass
        
    def test_load_reference_load_duplicate_ref(self):
        """
        """
        pass
        
    def test_load_reference_load_nonspecific_ref(self):
        """
        """
        pass
        
    def test_load_reference_load_missing_ref(self):
        """
        """
        pass
        
    def test_load_reference_load_to_subdir(self):
        """
        """
        pass
        
    def test_load_reference_load_to_mult_subdirs(self):
        """
        """
        pass
        
    def test_load_reference_load_symlink(self):
        """
        """
        pass
        
    def test_load_reference_load_w_invalid_project_id(self):
        """
        """
        pass
        
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
        pass

    def test_load_module_invalid_project_id(self):
        """
        """
        pass

    def test_load_module_chk_submodules(self):
        """
        """
        pass

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
        If a module has already been loaded, then an error should occur and -force should override.
        """
        pass

    def test_load_module_no_deps(self):
        """
        """
        pass

    def test_load_module_multi_modules(self):
        """
        """
        pass

    def test_load_module_alt_delay(self):
        """
        """
        pass

class TestAddStep:
    def test_add_step_valid_step(self):
        """
        """
        pass
        
    def test_add_step_invalid_step(self):
        """
        """
        pass
        
    def test_add_step_valid_multiple_times(self):
        """
        """
        pass
        
    def test_add_step_invalid_step(self):
        """
        """
        pass
        
    def test_add_step_check_mainnf_inclusion(self):
        """
        """
        pass
        
    def test_add_step_check_mainnf_workflow(self):
        """
        """
        pass


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
        """
        """
        pass
        
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

  
