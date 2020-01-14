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


def get_args():
    """
    """
    parser = argparse.ArgumentParser(prog="RAFT", 
                                     description="Reproducible Analysis Framework and Tools")
    subparsers = parser.add_subparsers(dest='command')

    
    parser_setup = subparsers.add_parser('setup', 
                                         help="RAFT setup and configuration.")

    
    parser_init_analysis = subparsers.add_parser('init-analysis', 
                                         help="Initialize a RAFT analysis.")
    
    parser_init_analysis.add_argument('-c', '--init-config', help="init configuration file.", 
                             default=os.path.join(os.getcwd(), '.init.cfg'))
    parser_init_analysis.add_argument('-n', '--name', help="Analysis name (Default: random string)",
                             default=rndm_str_gen(5))


    parser_load_samples = subparsers.add_parser('load-samples',
                                        help="Loads samples and ensure FASTQs are available")
    parser_load_samples.add_argument('-c', '--metadata-csv', required=True)
    parser_load_samples.add_argument('-a', '--analysis', default='')

    
    parser_load_workflow = subparsers.add_parser('load-workflow',
                                        help="Shallow clone of workflow into analysis directory.")
    parser_load_workflow.add_argument('-a', '--analysis', default='')
    parser_load_workflow.add_argument('-r', '--repo', default='')
    parser_load_workflow.add_argument('-w', '--workflow', required=True)
  
 
    parser_run_workflow = subparsers.add_parser('run-workflow',
                                        help="Runs specified workflow on specified sample(s)")
    #At least one of these should be required, but should they be mutually exclusive?
    parser_run_workflow.add_argument('-c', '--manifest-csvs',
                            help="Comma-separated list of manifest CSV(s) of samples to process.")
    parser_run_workflow.add_argument('-s', '--samples',
                            help="Comma-separated list of sample(s) to process.")
    parser_run_workflow.add_argument('-w', '--workflow',
                            help="Workflow to run on sample(s)")
    parser_run_workflow.add_argument('-n', '--nf-string',
                            help="String of parameters to be passed to Nextflow workflow. Note special behaviors in documentation.")
    parser_run_workflow.add_argument('-a', '--analysis',
                            help="Analysis")


    parser_package_analysis = subparsers.add_parser('package-analysis',
                                                    help="Package analysis for distribution.")
    parser_package_analysis.add_argument('-a', '--analysis', help="Specified analysis.")
    parser_package_analysis.add_argument('-o', '--output', help="Output file.", default='')

    parser_load_analysis = subparsers.add_parser('load-analysis',
                                                 help="Load an analysis from a rftpkg file.") 
    parser_load_analysis.add_argument('-a', '--analysis', help="Analysis name.")
    parser_load_analysis.add_argument('-r', '--rftpkg', help="rftpkg file.")
    
    
 
    return parser.parse_args()


def setup():
    """
    """
    cfg_path = os.path.join(os.getcwd(), '.raft.cfg')
    if os.path.isfile(cfg_path):
        bkup_cfg_path = cfg_path + '.orig'
        print("A configuration file already exists. Copying to {}.".format(bkup_cfg_path)) 
        os.rename(cfg_path, bkup_cfg_path)


    # Setting up filesystem paths
    cfg_bfr = {'datasets': os.path.join(os.getcwd(), 'datasets'),
               'analyses': os.path.join(os.getcwd(), 'analyses'),
               'indices': os.path.join(os.getcwd(), 'indices'),
               'references': os.path.join(os.getcwd(), 'references'),
               'fastqs': os.path.join(os.getcwd(), 'fastqs'),
               'repos': os.path.join(os.getcwd(), 'repos')}
    for path, default in cfg_bfr.items():
        actual_path = input("Please provide a shared directory for {} (Default: {}): ".format(path, default))
        #Should be doing some sanity checking here to ensure the path can exist...
        if actual_path:
            cfg_bfr[path] = actual_path

    # Setting up Nextflow workflow/module repositories
    nf_repo_bfr = {'workflows': 'git@sc.unc.edu:benjamin-vincent-lab/Nextflow/nextflow-workflows.git',
                   'modules': 'git@sc.unc.edu:benjamin-vincent-lab/Nextflow/nextflow-modules.git'}

    for path, default in nf_repo_bfr.items():
        actual_repo = input("Please provide a repository for Nextflow {}\n(Default: {})\n".format(path, default))
        if actual_repo:
            nf_repo_bfr[path] = actual_repo


    
    anlys_repo_bfr = {}
    print("Please provide any git repositories you'd like RAFT to access.\nThese are intended for pushing RAFT packages and are not required for pulling RAFT packages from public repositories\nNOTE:Please make sure you have ssh/pgp credentials in place before adding repositories.")
    repo_qry = input("Would you like to add a repository now? (Y/N)")
    while repo_qry == 'Y':
        repo_name = input("Please provide a local name for repo (e.g. public, private, johns_repo):")
        repo_url = input("Please provide the git url for repo (e.g git@github.com:spvensko/raft-test.git, <ENTER> for local init):")
        anlys_repo_bfr[repo_name] = repo_url
        repo_qry = input("Would you like to add an additional repository? (Y/N)")



    #Would like to have master_cfg constructed in its own function eventually.
    master_cfg = {'filesystem': cfg_bfr,
                  'nextflow_repos': nf_repo_bfr,
                  'analysis_repos': anlys_repo_bfr}

   
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
        if os.direxists(dir):
            os.symlink(dir, os.getcwd())
        else:
            os.mkdir(dir)

    for name, repo_url in master_cfg['analysis_repos'].items():
        if repo_url:
            try:
                Repo.clone_from(repo_url, os.path.join(master_cfg['filesystem']['repos'], name))
            except:
                print("Unable to create repo {} from url {}. Review your configuration file (.raft.cfg) and try again.".format(name, repo_url))
        else:
            Repo.init(os.path.join(master_cfg['filesystem']['repos'], name))

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
    cfg = load_raft_cfg()
    shrd_dir = cfg['filesystem']['analyses']
    anlys_dir = os.path.join(shrd_dir, name)
    
    try:
        os.mkdir(anlys_dir)
    except:
        pass
    return anlys_dir


def fill_dir(dir, config):
    """
    """
    # Getting the directories to be bound by this function as well.
    bound_dirs = []
    raft_cfg = load_raft_cfg()
    req_sub_dirs = {}
    with open(config) as fo:
        req_sub_dirs = json.load(fo)
    for name, sub_dir in req_sub_dirs.items():
        if sub_dir.upper() == 'USECFG' and name in raft_cfg['filesystem'].keys():
            os.symlink(raft_cfg['filesystem'][name], os.path.join(dir, name))
            bound_dirs.append(os.path.join(dir, name))
        elif sub_dir:
            os.symlink(sub_dir, os.path.join(dir, name))
            bound_dirs.append(os.path.join(dir, name))
        elif not sub_dir:
           os.mkdir(os.path.join(dir, name))
           bound_dirs.append(os.path.join(dir, name))
    return bound_dirs
   
 
def mk_mounts_cfg(dir, bound_dirs):
    """
    """
    out = []
    out.append('singularity {\n')
    out.append('  runOptions = "-B {}"\n'.format(','.join(bound_dirs)))
    out.append('}')

    with open(os.path.join(dir, 'workflow', 'mounts.config'), 'w') as fo:
        for row in out:
            fo.write(row)
            


def load_samples(args):
    """
    """
    fastqs_dir = ''
    datasets_dir = ''
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        raft_cfg = load_raft_cfg()
        metadata_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        shutil.copyfile(args.metadata_csv, os.path.join(metadata_dir, args.metadata_csv))        
        
        fastqs_dir = os.path.join(raft_cfg['filesystem']['fastqs'])
        datasets_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets')

    with open(args.metadata_csv) as fo:
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
            print("Making local dataset directory")
            os.makedirs(os.path.join(datasets_dir, dataset), exist_ok=True)
            print("Making shared dataset directory")
            os.makedirs(os.path.join(raft_cfg['filesystem']['datasets'], dataset), exist_ok=True)
            print("Making shared dataset/pat_id directory")
            os.makedirs(os.path.join(raft_cfg['filesystem']['datasets'], dataset, pat_id), exist_ok=True)
#            print("Symlinking pat_id dir to analysis/datasets")
#            os.symlink(os.path.join(raft_cfg['filesystem']['datasets'], dataset, pat_id), os.path.join(datasets_dir, dataset, pat_id))
        #    except:
        #        print("Oops!") 
        #        pass


            for col in cols_to_check:
                fastq_prefix = row[col]
                print("Checking for FASTQ prefix {} in /fastqs...".format(fastq_prefix))
                hits = glob(os.path.join(fastqs_dir, fastq_prefix), recursive=True)
                if hits:
                    print("Found FASTQs for prefix {} in /fastqs!".format(fastq_prefix))
                else:
                    print("Unable to find FASTQs for prefix {} in /fastqs. Check your metadata csv!\n".format(fastq_prefix)) 
                if len(hits) == 1 and os.path.isdir(hits[0]):
                    try:
                        os.symlink(hits[0], os.path.join(datasets_dir, dataset, pat_id, fastq_prefix))
                    except:
                        pass
       

def load_workflow(args):
    """
    """
    raft_cfg = load_raft_cfg()
    # This shouldn't be hard-coded, but doing it for now.
    modules_repo = raft_cfg['nextflow_repos']['modules']
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['workflows']
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow')
        Repo.clone_from(args.repo,os.path.join(workflow_dir, args.workflow), branch=args.workflow)
        Repo.clone_from(modules_repo, os.path.join(workflow_dir, args.workflow, 'modules'), branch='develop')


def run_workflow(args):
    """
    """
    raft_cfg = load_raft_cfg()
    processed_samp_ids = []
    if args.manifest_csvs:
        args.manifest_csvs = [i for i in args.manifest_csvs.split(',')]
    if args.samples:
        args.samples = [i for i in args.samples.split(',')]
    # Should probably check that the workflow exists within the analysis...
    # Thought process here, get string, figure out map between csv columns and workflow params
    # Best thing to do here is to take the manifest CSVs and convert them to a list of strings
    for samp_id in args.samples:
        if samp_id not in processed_samp_ids:
            samp_mani_info = get_samp_mani_info(args.analysis, samp_id)
            work_dir = os.path.join(raft_cfg['filesystem']['datasets'], samp_mani_info['Dataset'], samp_mani_info['Patient ID'], 'work')
            local_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets', samp_mani_info['Dataset'], samp_mani_info['Patient ID'])
            os.makedirs(work_dir, exist_ok=True)
            os.makedirs(local_dir, exist_ok=True)
            samp_nf_cmd = get_samp_nf_cmd(args, samp_mani_info)
            # Need to add work dir parameter here, -w
            samp_nf_cmd = prepend_nf_cmd(args, samp_nf_cmd)
            samp_nf_cmd = add_nf_wd(work_dir, samp_nf_cmd)
            print("Running:\n{}".format(samp_nf_cmd))
            subprocess.run(samp_nf_cmd, shell=True, check=True)
            print("Started process...")
            processed_samp_ids.append(samp_id)
         

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
                   print("Found it!")
                   samp_mani_info = {hdr[idx]: line[idx] for idx in range(len(line))}
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
    cmd = ' '.join(['./nextflow', discovered_nf, samp_nf_cmd])
    return cmd

def get_samp_nf_cmd(args, samp_mani_info):
    """
    """
    print("Original: {}".format(args.nf_string))

    #Deconstruct the string, see where raft parameters are, replace them
    cmd = args.nf_string.split(' ')
    new_cmd = [] 
    for component in cmd:
        if re.match('META:', component):
            component = component.replace('META:', '')
            #Need a uniqueness test here to ensure variable specific enough.
            for k, v in samp_mani_info.items():
                if re.search(component, k):
                    new_cmd.append(v)
        else:
            new_cmd.append(component)

    raft_cfg = load_raft_cfg()
    # Should this be in its own additional function?
    if not re.search('--analysis_dir', args.nf_string):
        analysis_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis)
        new_cmd.append("--analysis_dir {}".format(analysis_dir))

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
    if args.command not in ['init-analysis', 'run-auto', 'package-analysis', 'load-analysis']:
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
    #Should really be using .init.cfg from package here...
    parser = argparse.ArgumentParser()
    init_args = parser.parse_args(['init_config', os.path.join(os.getcwd(), '.init.cfg'),
                 'name', args.analysis])
    init_analysis(init_args)
                


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
    elif args.command == 'load-workflow':
        load_workflow(args)
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
