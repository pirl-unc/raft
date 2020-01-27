#!/usr/bin/env python

# Run this *in* the RAFT directory, or bad things will happen (or nothing at all).

import argparse
from git import Repo
from glob import glob
import hashlib
import json
import os
import random
import re
import shutil
import string
import subprocess
import sys
import tarfile
import time

# These are repeatedly called, so trying to make life easier.
from os.path import join as pjoin
from os import getcwd


def get_args():
    """
    """
    parser = argparse.ArgumentParser(prog="RAFT",
                                     description="Reproducible Analysis Framework and Tools")


    subparsers = parser.add_subparsers(dest='command', required=True)


    # Subparser for initial RAFT setup. Utilizies user prompts to produce config file.
    parser_setup = subparsers.add_parser('setup',
                                         help="RAFT setup and configuration.")


    # Subparser for initializing an analysis.
    parser_init_analysis = subparsers.add_parser('init-analysis',
                                                 help="Initialize a RAFT analysis.")
    parser_init_analysis.add_argument('-c', '--init-config',
                                      help="Analysis config file.",
                                      default=pjoin(getcwd(), '.init.cfg'))
    parser_init_analysis.add_argument('-n', '--name',
                                      help="Analysis name.",
                                      required=True)


    # Subparser for loading samples into an analysis.
    parser_load_samples = subparsers.add_parser('load-samples',
                                                help="Loads samples into an analysis.")
    parser_load_samples.add_argument('-c', '--manifest-csv',
                                     help="Manifest CSV. Check docs for more info.",
                                     required=True)

    parser_load_samples.add_argument('-a', '--analysis',
                                     help="Analysis to add samples to.",
                                     required=True)
    
    
    # Subparser for loading metadata into an analysis.
    parser_load_samples = subparsers.add_parser('load-metadata',
                                                help="Loads metadata for an analysis.")
    parser_load_samples.add_argument('-c', '--metadata-csv',
                                     help="Metadata CSV. Check docs for more info.",
                                     required=True)
    parser_load_samples.add_argument('-a', '--analysis',
                                     help="Analysis to add metadata to.",
                                     required=True)


    # Subparser for loading workflow into an analysis.
    parser_load_workflow = subparsers.add_parser('load-workflow',
                                        help="Clones Nextflow workflow into analysis.")
    parser_load_workflow.add_argument('-a', '--analysis',
                                      help="Analysis to add workflow to.",
                                      required=True)
    parser_load_workflow.add_argument('-r', '--repo', # Defaults to BGV NF workflow repo.
                                      help="Repo to fetch workflow from.",
                                      default='')
    parser_load_workflow.add_argument('-w', '--workflow',
                                      help="Workflow to add to analysis.",
                                      required=True)
    # Need support for commits and tags here as well.
    parser_load_workflow.add_argument('-b', '--branch',
                                      help="Branch to checkout. Defaults to 'develop'.",
                                      default='develop')
    parser_load_workflow.add_argument('-n', '--no-modules',
                                      help="Do not load any common modules.",
                                      default=False)
    

    # Subparser for loading private modules into an analysis.
    parser_load_private_module = subparsers.add_parser('load-private-module',
                                        help="Clones private module into analysis.")
    parser_load_private_module.add_argument('-a', '--analysis',
                                      help="Analysis to add workflow to.",
                                      required=True)
    parser_load_private_module.add_argument('-w', '--workflow',
                                      help="Workflow to add to analysis.",
                                      required=True)
    parser_load_private_module.add_argument('-r', '--repo', # Default = BGV NF priv module repo.
                                      help="Repo to fetch workflow from.",
                                      default='')
    parser_load_private_module.add_argument('-m', '--module',
                                      help="Module to add to analysis.",
                                      required=True)
    # Need support for commits and tags here as well.
    parser_load_private_module.add_argument('-b', '--branch',
                                      help="Branch to checkout. Defaults to 'develop'.",
                                      default='develop')


    # Subparser for running workflow on samples.
    parser_run_workflow = subparsers.add_parser('run-workflow',
                                                help="Runs workflow on sample(s)")
    # At least one of these should be required, but should they be mutually exclusive?
    parser_run_workflow.add_argument('-c', '--manifest-csvs',
                                     help="Comma-separated list of manifest CSVs")
    parser_run_workflow.add_argument('-s', '--samples',
                                     help="Comma-separated list of sample(s).")
    parser_run_workflow.add_argument('-w', '--workflow',
                                     help="Workflow to run.")
    parser_run_workflow.add_argument('-n', '--nf-params',
                                     help="Param string passed to NF. Check docs for more info.")
    parser_run_workflow.add_argument('-a', '--analysis',
                                     help="Analysis",
                                     required=True)

    # Subparser for packaging analysis (to generate sharable rftpkg tar file)
    parser_package_analysis = subparsers.add_parser('package-analysis',
                                                    help="Package analysis for distribution.")
    parser_package_analysis.add_argument('-a', '--analysis',
                                         help="Analysis to package.")
    parser_package_analysis.add_argument('-o', '--output',
                                         help="Output file.",
                                         default='')


    # Subparser for loading analysis (after receiving rftpkg tar file)
    parser_load_analysis = subparsers.add_parser('load-analysis',
                                                 help="Load an analysis from a rftpkg file.")
    parser_load_analysis.add_argument('-a', '--analysis', help="Analysis name.")
    parser_load_analysis.add_argument('-r', '--rftpkg', help="rftpkg file.")


    return parser.parse_args()


def setup():
    """
    """
    # Ideally, users should be able to specify where .raft.cfg lives.
    cfg_path = pjoin(getcwd(), '.raft.cfg')

    # Make backup of previous configuration file.
    if os.path.isfile(cfg_path):
        bkup_cfg_path = cfg_path + '.orig'
        print("A configuration file already exists.")
        print("Copying original to {}.".format(bkup_cfg_path))
        os.rename(cfg_path, bkup_cfg_path)


    # Setting up filesystem paths. Initialized with defaults, then prompts user for input.
    raft_paths = {'datasets': pjoin(getcwd(), 'datasets'),
                  'analyses': pjoin(getcwd(), 'analyses'),
                  'indices': pjoin(getcwd(), 'indices'),
                  'references': pjoin(getcwd(), 'references'),
                  'fastqs': pjoin(getcwd(), 'fastqs'),
                  'imgs': pjoin(getcwd(), 'imgs'),
                  'repos': pjoin(getcwd(), 'repos')}

    for raft_path, default in raft_paths.items():
        user_spec_path = input("Please provide a shared directory for {} (Default: {}): "
                               .format(raft_path, default))

        # Should be doing some sanity checking here to ensure the path can exist...
        if user_spec_path:
            rath_paths[raft_path] = user_spec_path

    # Setting up Nextflow workflow/module repositories.
    nf_repos = {'workflow-subgroup':
                'git@sc.unc.edu:benjamin-vincent-lab/Nextflow/nextflow-workflows',
                'modules': 
                'git@sc.unc.edu:benjamin-vincent-lab/Nextflow/nextflow-modules.git'}

    # Allow users to specify their own Nextflow workflows and modules repos.
    for nf_repo, default in nf_repos.items():
        user_spec_repo = input("Provide a repository for Nextflow {}\n(Default: {}):"
                               .format(nf_repo, default))
        if user_spec_repo:
            nf_repos[nf_repo] = user_spec_repo



    raft_repos = {}
    message = ["Provide any git repositories you'd like RAFT to access.",
               "These are for pushing RAFT packages and are not required",
               "for pulling RAFT packages from public repositories.",
               "NOTE: Make sure ssh/pgp credentials in place before using these repositories."]
    print('\n'.join(message))
    repo_qry = input("Would you like to add a repository now? (Y/N)")
    while repo_qry == 'Y':
        repo_name = input("Provide a local name for repo (e.g. public, private, repo1):")
        repo_url = input("Provide the git url for repo (or hit <ENTER> for local init):")
        raft_repos[repo_name] = repo_url
        repo_qry = input("Would you like to add an additional repository? (Y/N)")


    # Would like to have master_cfg constructed in its own function eventually.
    master_cfg = {'filesystem': raft_paths,
                  'nextflow_repos': nf_repos,
                  'analysis_repos': raft_repos}

    dump_cfg(cfg_path, master_cfg)

    setup_run_once(master_cfg)


def dump_cfg(cfg_path, master_cfg):
    """
    """
    with open(cfg_path, 'w') as fo:
        json.dump(master_cfg, fo, indent=4)


def setup_run_once(master_cfg):
    """
    """
    for dir in master_cfg['filesystem'].values():
        if os.path.isdir(dir):
            os.symlink(dir, getcwd())
        else:
            os.mkdir(dir)

    for name, repo_url in master_cfg['analysis_repos'].items():
        if repo_url:
            try:
                Repo.clone_from(repo_url, pjoin(master_cfg['filesystem']['repos'], name))
            except:
                print("Unable to create repo {} from url {}. Review your .raft.cfg."
                      .format(name, repo_url))
        else:
            Repo.init(pjoin(master_cfg['filesystem']['repos'], name))


def init_analysis(args):
    """
    """
    anlys_dir = mk_anlys_dir(args.name)
    bound_dirs = fill_dir(anlys_dir, args.init_config)
    mk_mounts_cfg(anlys_dir, bound_dirs)
    mk_auto_raft(args)


def mk_auto_raft(args):
    """
    """
    raft_cfg = load_raft_cfg()
    auto_raft_path = os.path.join(raft_cfg['filesystem']['analyses'], args.name, '.raft', 'auto.raft')
    with open(auto_raft_path, 'w') as fo:
        fo.write("{}\n".format(' '.join(sys.argv)))


def mk_anlys_dir(name):
    """
    """
    anlys_dir = ''
    raft_cfg = load_raft_cfg()
    global_dir = raft_cfg['filesystem']['analyses']
    anlys_dir = pjoin(global_dir, name)

    try:
        os.mkdir(anlys_dir)
    except:
        sys.exit("Analysis directory already exists. Please try another.")

    return anlys_dir


def fill_dir(dir, init_cfg):
    """
    """
    # Getting the directories to be bound by this function as well.
    bound_dirs = []
    raft_cfg = load_raft_cfg()
    req_sub_dirs = {}
    with open(init_cfg) as fo:
        req_sub_dirs = json.load(fo)
    for name, sdir in req_sub_dirs.items():
        # If the desired directory has a "USECFG" value and has a default in
        # the global raft configuration file, then symlink the default global
        # directory within the analysis directory.
        if sdir.upper() == 'USECFG' and name in raft_cfg['filesystem'].keys():
            os.symlink(raft_cfg['filesystem'][name], pjoin(dir, name))
            bound_dirs.append(pjoin(dir, name))
        # Else if the desired directory has an included path, link that path to
        # within the analysis directory. This should include some sanity
        # checking to ensure the sub_dir directory even exists.
        elif sdir:
            os.symlink(sdir, pjoin(dir, name))
            bound_dirs.append(pjoin(dir, name))
        # Else if the desired directory doesn't have an included path, simply
        # make a directory by that name within the analysis directory.
        elif not sdir:
            os.mkdir(pjoin(dir, name))
            bound_dirs.append(pjoin(dir, name))

    bound_dirs.append(getcwd())

    # Bound directories are returned so they can be used to generate
    # mounts.config which allows Singularity (and presumably Docker) to bind
    # (and access) these directories.
    return bound_dirs


def mk_mounts_cfg(dir, bound_dirs):
    """
    """
    raft_cfg = load_raft_cfg()
    imgs_dir = raft_cfg['filesystem']['imgs']
    out = []
    out.append('singularity {\n')
    out.append('  runOptions = "-B {}"\n'.format(','.join(bound_dirs)))
    out.append('  cacheDir = "{}"\n'.format(imgs_dir))
    out.append("  autoMount = 'true'\n")
    out.append('}')

    with open(pjoin(dir, 'workflow', 'mounts.config'), 'w') as fo:
        for row in out:
            fo.write(row)


def load_samples(args):
    """
    """
    fastqs_dir = ''
    datasets_dir = ''
    raft_cfg = load_raft_cfg()
    if os.path.isdir(pjoin(raft_cfg['filesystem']['analyses'], args.analysis)):
        # If the specified analysis doesn't exist, then should it be created automatically?
        metadata_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        shutil.copyfile(args.manifest_csv,
                        pjoin(metadata_dir, os.path.basename(args.manifest_csv)))
        # This is checking the global, shared FASTQ directory for FASTQs.
        fastqs_dir = pjoin(raft_cfg['filesystem']['fastqs'])
        datasets_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets')

    with open(args.manifest_csv) as fo:
        hdr = fo.readline()
        hdr = hdr.strip('\n').split(',')
        # Will certainly need a better way to do this, but this will work for now.
        cols_to_check = [i for i in range(len(hdr)) if hdr[i] not in ['Dataset', 'Patient ID']]
        dataset_col = hdr.index('Dataset')
        pat_id_col = hdr.index('Patient ID')


        for row in fo:
            row = row.strip('\n').split(',')
            dataset = row[dataset_col]
            pat_id = row[pat_id_col]
            # Probably a better way to do this.
            #try:
            os.makedirs(os.path.join(datasets_dir, dataset), exist_ok=True)
            os.makedirs(os.path.join(raft_cfg['filesystem']['datasets'], dataset), exist_ok=True)
            os.makedirs(os.path.join(raft_cfg['filesystem']['datasets'], dataset, pat_id), exist_ok=True)
#            print("Symlinking pat_id dir to analysis/datasets")
#            os.symlink(os.path.join(raft_cfg['filesystem']['datasets'], dataset, pat_id), os.path.join(datasets_dir, dataset, pat_id))
        #    except:
        #        print("Oops!") 
        #        pass


            for col in cols_to_check:
                fastq_prefix = row[col]
                if fastq_prefix == 'NA':
                    continue
                print("Checking for FASTQ prefix {} in /fastqs...".format(fastq_prefix))
                hits = glob(pjoin(fastqs_dir, fastq_prefix), recursive=True)
                if hits:
                    print("Found FASTQs for prefix {} in /fastqs!".format(fastq_prefix))
                else:
                    print("Unable to find FASTQs for prefix {} in /fastqs. Check your metadata csv!\n".format(fastq_prefix)) 
                if len(hits) == 1 and os.path.isdir(hits[0]):
                    try:
                        os.symlink(hits[0], os.path.join(datasets_dir, dataset, pat_id, fastq_prefix))
                    except:
                        pass

def load_metadata(args):
    """
    """
    raft_cfg = load_raft_cfg()
    if os.path.isdir(pjoin(raft_cfg['filesystem']['analyses'], args.analysis)):
        # If the specified analysis doesn't exist, then should it be created automatically?
        metadata_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        shutil.copyfile(args.metadata_csv,
                        pjoin(metadata_dir, os.path.basename(args.metadata_csv)))

def load_private_module(args):
    """
    """
    raft_cfg = load_raft_cfg()
    # This shouldn't be hard-coded, but doing it for now.
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['modules_private_subgroup']
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow', args.workflow, 'pmodules')
        Repo.clone_from(pjoin(args.repo, args.module), pjoin(workflow_dir, args.module), branch=args.branch)



def load_workflow(args):
    """
    """
    raft_cfg = load_raft_cfg()
    # This shouldn't be hard-coded, but doing it for now.
    modules_repo = raft_cfg['nextflow_repos']['modules']
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['workflows_subgroup']
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow')
        Repo.clone_from(pjoin(args.repo, args.workflow), pjoin(workflow_dir, args.workflow), branch=args.branch)
    if not args.no_modules:
        Repo.clone_from(modules_repo, os.path.join(workflow_dir, args.workflow, 'modules'), branch='develop')


def run_workflow(args):
    """
    """
    init_dir = getcwd()
    raft_cfg = load_raft_cfg()
    all_samp_ids = []
    processed_samp_ids = []
    if args.manifest_csvs:
        manifest_csvs = [i for i in args.manifest_csvs.split(',')]
        print(manifest_csvs)
        for manifest_csv in manifest_csvs:
            print(manifest_csv)
            all_samp_ids.extend(extract_samp_ids(args, manifest_csv))
      
    if args.samples:
        all_samp_ids.extend([i for i in args.samples.split(',')])
    # Should probably check that the workflow exists within the analysis...
    # Thought process here, get string, figure out map between csv columns and workflow params
    # Best thing to do here is to take the manifest CSVs and convert them to a list of strings
    print(all_samp_ids)
    if not all_samp_ids:
        generic_nf_cmd = get_generic_nf_cmd(args)
        generic_nf_cmd = prepend_nf_cmd(args, generic_nf_cmd)
        # Going to update with work dir and log dir later.
        print("Running:\n{}".format(generic_nf_cmd))
        subprocess.run(generic_nf_cmd, shell=True, check=False)
        print("Started process...")
    else:
        for samp_id in all_samp_ids:
            print("Processing sample {}".format(samp_id))
            if samp_id not in processed_samp_ids:
                samp_mani_info = get_samp_mani_info(args.analysis, samp_id)
                work_dir = pjoin(raft_cfg['filesystem']['datasets'], samp_mani_info['Dataset'], samp_mani_info['Patient ID'], 'work')
                local_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets', samp_mani_info['Dataset'], samp_mani_info['Patient ID'])
                tmp_dir =  pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'tmp', samp_mani_info['Patient ID'], str(time.time()))
    
                os.makedirs(work_dir, exist_ok=True)
                os.makedirs(local_dir, exist_ok=True)
                os.makedirs(tmp_dir, exist_ok=True)
    
                samp_nf_cmd = get_samp_nf_cmd(args, samp_mani_info)
                samp_nf_cmd = prepend_nf_cmd(args, samp_nf_cmd)
                samp_nf_cmd = add_nf_wd(work_dir, samp_nf_cmd)
                samp_nf_cmd = add_log_dir(samp_id, args, samp_nf_cmd)
    
                os.chdir(tmp_dir)
                print("Currently in: {}".format(getcwd()))
                print("Running:\n{}".format(samp_nf_cmd))
                subprocess.run(samp_nf_cmd, shell=True, check=False)
                print("Started process...")
                processed_samp_ids.append(samp_id)
                print("Waiting 10 seconds before sending next request.")
                time.sleep(10)
                os.chdir(init_dir)
         

def extract_samp_ids(args, manifest_csv):
    """
    """
    raft_cfg = load_raft_cfg()
    samp_ids = []
    with open(pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata', os.path.basename(manifest_csv))) as fo:
        hdr = fo.readline().rstrip('\n').split(',')
        pat_idx = hdr.index("Patient ID")
        for line in fo:
            line = line.rstrip('\n').split(',')
            samp_ids.append(line[pat_idx])
    return samp_ids
    

def add_log_dir(samp_id, args, samp_nf_cmd):
    """
    """
    raft_cfg = load_raft_cfg()
    
    log_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'logs', '{}.log'.format(samp_id))
    return ' '.join([samp_nf_cmd, '> {} 2>&1 &'.format(log_dir)])

def add_nf_wd(work_dir, samp_nf_cmd):
    """
    """
    return ' '.join([samp_nf_cmd, '-w {}'.format(work_dir), '-resume'])
 

def get_samp_mani_info(analysis, samp_id):
    """
    """
    # This is kinda gross, clean it up.
    samp_mani_info = {}
    raft_cfg = load_raft_cfg()
    manifest_dir = os.path.join(raft_cfg['filesystem']['analyses'], analysis, 'metadata')
    manifest_csvs = glob(os.path.join(manifest_dir, '*csv'))
    for manifest_csv in manifest_csvs:
        with open(manifest_csv) as fo:
            hdr = fo.readline().rstrip('\n').split(',')
            pat_idx = hdr.index("Patient ID")
            for line in fo:
                line = line.rstrip('\n').split(',')
                if line[pat_idx] == samp_id:
                   samp_mani_info = {hdr[idx]: line[idx] for idx in range(len(line))}
                   break
    return samp_mani_info

def prepend_nf_cmd(args, samp_nf_cmd):
    """
    """
    raft_cfg = load_raft_cfg()
    workflow_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow', args.workflow)
    print(workflow_dir)
    #Ensure only one nf is discoverd here! If more than one is discovered, then should multiple be run?
    discovered_nf = glob(os.path.join(workflow_dir, '*.nf'))[0]
    print(discovered_nf)
    cmd = ' '.join(['nextflow', discovered_nf, samp_nf_cmd])
    return cmd

def get_samp_nf_cmd(args, samp_mani_info):
    """
    """
    print("Original: {}".format(args.nf_params))

    #Deconstruct the string, see where raft parameters are, replace them
    cmd = args.nf_params.split(' ')
    new_cmd = [] 
    for component in cmd:
        if re.match('META:', component):
            component = component.replace('META:', '')
            # Need a uniqueness test here to ensure variable specific enough.
            # Going to allow comma-separated for now.
            components = [i for i in component.split(',')]
            for k, v in samp_mani_info.items():
                if all([re.search(component, k) for component in components]):
                    new_cmd.append(v)
        else:
            new_cmd.append(component)

    raft_cfg = load_raft_cfg()
    # Should this be in its own additional function?
    if not re.search('--analysis_dir', args.nf_params):
        analysis_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis)
        new_cmd.append("--analysis_dir {}".format(analysis_dir))

    return ' '.join(new_cmd)


def get_generic_nf_cmd(args):
    """
    """
    cmd = args.nf_params.split(' ')
    new_cmd = [] 
    raft_cfg = load_raft_cfg()
    # Should this be in its own additional function?
    for component in cmd:
        # Do any processing here.
        new_cmd.append(component)
    if not re.search('--analysis_dir', args.nf_params):
        analysis_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis)
        new_cmd.append("--analysis_dir {}".format(analysis_dir))
    print(new_cmd)
    return ' '.join(new_cmd)
    

def rndm_str_gen(size=5):
    """
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(size)) 


def load_raft_cfg():
    """
    """
    cfg = {}
    cfg_path = os.path.join(os.getcwd(), '.raft.cfg')
    with open(cfg_path) as fo:
        cfg = json.load(fo)
    return cfg

def dump_to_auto_raft(args):
    """
    """
    if args.command not in ['init-analysis', 'run-auto', 'package-analysis', 'load-analysis', 'setup']:
        raft_cfg = load_raft_cfg()
        auto_raft_path = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, '.raft', 'auto.raft')
        with open(auto_raft_path, 'a') as fo:
            fo.write("{}\n".format(' '.join(sys.argv)))


def package_analysis(args):
    """
    """
    raft_cfg = load_raft_cfg()
    anlys_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis)
    foo = rndm_str_gen()
    anlys_tmp_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'tmp', foo)
    
    os.mkdir(anlys_tmp_dir)
    for dir in os.listdir(os.path.join(raft_cfg['filesystem']['analyses'], args.analysis)):
        # These directories should be hidden (at least optionally so) in the future
        if dir in ['references', 'indices', 'fastqs', 'repos', 'tmp']:
            continue
        print(dir)

    # Copying metadata directory. Should probably perform some size checks here.
    shutil.copytree(os.path.join(anlys_dir, 'metadata'), os.path.join(anlys_tmp_dir, 'metadata'))

    # Getting required checksums. Currently only doing /datasets, but should
    # probably do other directories produced by workflow as well.
    with open(os.path.join(anlys_tmp_dir, 'datasets.md5'), 'w') as fo:
        files = glob(os.path.join('analyses', args.analysis, 'datasets', '**'), recursive=True)
        hashes = {file: hashlib.md5(file_as_bytes(open(file, 'rb'))).hexdigest() for file in files if os.path.isfile(file)}
        json.dump(hashes, fo, indent=4)
     
    # Get Nextflow configs, etc.
    os.mkdir(os.path.join(anlys_tmp_dir, 'workflow'))
    for dir in glob(os.path.join(anlys_dir, 'workflow', '*')):
        if os.path.isdir(dir):
            shutil.copytree(os.path.join(dir), os.path.join(anlys_tmp_dir, 'workflow', os.path.basename(dir)))

    # Get auto.raft
    shutil.copyfile(os.path.join(anlys_dir, '.raft', 'auto.raft'), os.path.join(anlys_dir, '.raft', 'snapshot.raft'))
    shutil.copyfile(os.path.join(anlys_dir, '.raft', 'snapshot.raft'), os.path.join(anlys_tmp_dir, 'snapshot.raft'))

    rftpkg = ''
    if args.output:
        rftpkg = args.output
    else:
        rftpkg = os.path.join(anlys_dir, '.raft', 'default.rftpkg')
    with tarfile.open(rftpkg, 'w') as taro:
        for i in os.listdir(anlys_tmp_dir):
            print(i)
            taro.add(os.path.join(anlys_tmp_dir, i), arcname = i)

        
    #shutil.rmtree(anlys_tmp_dir) 
   

#https://stackoverflow.com/a/3431835
def file_as_bytes(file):
    with file:
        return file.read() 

def load_analysis(args):
    """
    """
    raft_cfg = load_raft_cfg()
    #Should really be using .init.cfg from package here...
    fixt_args = {'init_config': os.path.join(os.getcwd(), '.init.cfg'),
                 'name': args.analysis}
    fixt_args = argparse.Namespace(**fixt_args)
    
    # Initialize analysis
    init_analysis(fixt_args)

    # Copy rftpkg into analysis
    shutil.copyfile(args.rftpkg, os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, '.raft', os.path.basename(args.rftpkg)))
    tarball = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, '.raft', os.path.basename(args.rftpkg))
   
    # Extract and distribute tarball contents
    tar = tarfile.open(tarball)
    tar.extractall(os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, '.raft'))
    tar.close()
    


def main():
    """
    """
    args = get_args()

    dump_to_auto_raft(args)

    # I'm pretty sure .setdefaults within subparsers should handle running
    # functions, but this will work for now.
    if args.command == 'setup':
        setup()
    elif args.command == 'init-analysis':
        init_analysis(args)
    elif args.command == 'load-samples':
        load_samples(args)
    elif args.command == 'load-metadata':
        load_metadata(args)
    elif args.command == 'load-workflow':
        load_workflow(args)
    elif args.command == 'load-private-module':
        load_private_module(args)
    elif args.command == 'run-workflow':
        run_workflow(args)
    elif args.command == 'run-auto':
        run_auto(args)
    elif args.command == 'package-analysis':
        package_analysis(args)
    elif args.command == 'load-analysis':
        load_analysis(args)
    



if __name__=='__main__':
    main()
