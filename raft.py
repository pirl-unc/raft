#!/usr/bin/env python3

import argparse
from git import Repo
import json
import os
import re


def get_args():
    """
    """
    parser = argparse.ArgumentParser(prog="RAFT", 
                                     description="Reproducible Analysis Framework and Tools")
    subparsers = parser.add_subparsers()

    
    parser_setup = subparsers.add_parser('setup', 
                                         help="RAFT setup and configuration.")
    parser_setup.set_defaults(func=setup())

    
    parser_init = subparsers.add_parser('init', 
                                         help="Initialize a RAFT analysis.")
    
    parser_init.add_argument('-c', '--init-config', help="init configuration file.", 
                             default=os.path.join(os.getcwd(), '.init.cfg')
    parser_init.add_argument('-n', '--name', help="Analysis name. Defaults to random string.",
                             default=rndm_str_gen())
    parser_init.set_defaults(func=init(args.init_config, args.name))
    
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
        os.mkdir(dir)

    for name, repo_url in master_cfg['analysis_repos'].items():
        try:
            Repo.clone_from(repo_url, os.path.join(master_cfg['filesystem']['repos'], name))
        except:
            print("Unable to create repo {} from url {}. Review your configuration file (.raft.cfg) and try again.".format(name, repo_url))

def init(init_config, name):
    """
    """
    pass


def rndm_str_gen(size=5):
    """
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(size)) 


def main():
    """
    """
    args = get_args()


if __name__=='__main__':
    main()
