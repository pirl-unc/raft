#!/usr/bin/env python3

# Run this *in* the RAFT directory, or bad things will happen.


import argparse
from git import Repo
from glob import glob
import json
import os
import random
import re
import shutil
import string
import sys


def get_args():
    """
    """
    parser = argparse.ArgumentParser(prog="RAFT", 
                                     description="Reproducible Analysis Framework and Tools")
    subparsers = parser.add_subparsers(dest='command')

    
    parser_setup = subparsers.add_parser('setup', 
                                         help="RAFT setup and configuration.")

    
    parser_init = subparsers.add_parser('init', 
                                         help="Initialize a RAFT analysis.")
    
    parser_init.add_argument('-c', '--init-config', help="init configuration file.", 
                             default=os.path.join(os.getcwd(), '.init.cfg'))
    parser_init.add_argument('-n', '--name', help="Analysis name (Default: random string)",
                             default=rndm_str_gen(5))


    parser_load = subparsers.add_parser('load',
                                        help="Loads samples and ensure FASTQs are available")
    parser_load.add_argument('-c', '--metadata-csv', required=True)
    parser_load.add_argument('-a', '--analysis', default='')

    
    parser_workflow = subparsers.add_parser('workflow',
                                        help="Shallow clone of workflow into analysis directory.")
    parser_workflow.add_argument('-a', '--analysis', default='')
    parser_workflow.add_argument('-r', '--repo', default='')
    parser_workflow.add_argument('-w', '--workflow', required=True)
   
 
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
        repo_url = input("Please provide the git url for repo (e.g git@github.com:spvensko/raft-test.git):")
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
        try:
            Repo.clone_from(repo_url, os.path.join(master_cfg['filesystem']['repos'], name))
        except:
            print("Unable to create repo {} from url {}. Review your configuration file (.raft.cfg) and try again.".format(name, repo_url))

def init(args):
    """
    """
    anlys_dir = mk_anlys_dir(args.name)
    fill_dir(anlys_dir, args.init_config)


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
    raft_cfg = load_raft_cfg()
    req_sub_dirs = {}
    with open(config) as fo:
        req_sub_dirs = json.load(fo)
    for name, sub_dir in req_sub_dirs.items():
        if sub_dir.upper() == 'USECFG' and name in raft_cfg['filesystem'].keys():
            os.symlink(raft_cfg['filesystem'][name], os.path.join(dir, name))
        elif sub_dir:
            os.symlink(sub_dir, os.path.join(dir, name))
        elif not sub_dir:
           os.mkdir(os.path.join(dir, name))
     

def load(args):
    """
    """
    fastqs_dir = ''
    datasets_dir = ''
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        raft_cfg = load_raft_cfg()
        metadata_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        shutil.copyfile(args.metadata_csv, os.path.join(metadata_dir, args.metadata_csv))        
        
        fastqs_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'fastqs')
        datasets_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets')

    with open(args.metadata_csv) as fo:
        hdr = fo.readline()
        hdr = hdr.strip('\n').split(',')
        # Will certainly need a better way to do this, but this will work for now.
        cols_to_check = [i for i in range(len(hdr) - 1) if hdr[i] not in ['Dataset', 'Patient ID']]
        dataset_col = hdr.index('Dataset')
        pat_id_col = hdr.index('Patient ID')


        for row in fo:
            row = row.strip('\n').split(',')
            dataset = row[dataset_col]
            pat_id = row[pat_id_col]
            # Probably a better way to do this.
            try:
                os.mkdir(os.path.join(datasets_dir, dataset))
            except:
                pass
            try:
                os.mkdir(os.path.join(datasets_dir, dataset, pat_id))
            except:
                pass
            for col in cols_to_check:
                fastq_prefix = row[col]
                print("Checking for FASTQ prefix {} in /fastqs...".format(fastq_prefix))
                hits = glob(os.path.join(fastqs_dir, fastq_prefix), recursive=True)
                if hits:
                    print("Found FASTQs for prefix {} in /fastqs!\n".format(fastq_prefix))
                else:
                    print("Unable to find FASTQs for prefix {} in /fastqs. Check your metadata csv!\n".format(fastq_prefix)) 
                if len(hits) == 1 and os.path.isdir(hits[0]):
                    os.symlink(hits[0], os.path.join(datasets_dir, dataset, pat_id, fastq_prefix))
       

def workflow(args):
    """
    """
    raft_cfg = load_raft_cfg()
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['workflows']    
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = os.path.join(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow')
        Repo.clone_from(args.repo, os.path.join(workflow_dir, args.workflow), branch=args.workflow)

 
        


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


def main():
    """
    """
    # I'm pretty sure .setdefaults within subparsers should handle running
    # functions, but this will work for now.
    args = get_args()
    if args.command == 'setup':
        setup()
    elif args.command == 'init':
        init(args)
    elif args.command == 'load':
        load(args)
    elif args.command == 'workflow':
        workflow(args)



if __name__=='__main__':
    main()
