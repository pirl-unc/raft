#!/usr/bin/env python3

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
                                     description="""Reproducible
                                                    Analyses
                                                    Framework
                                                    and
                                                    Tools""")


    subparsers = parser.add_subparsers(dest='command')


    # Subparser for initial RAFT setup.
    parser_setup = subparsers.add_parser('setup',
                                         help="""RAFT setup
                                                 and configuration.""")
    parser_setup.add_argument('-d', '--default',
                              help="Use default paths for setup.",
                              action="store_true",
                              default=False)


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
    parser_load_manifest = subparsers.add_parser('load-manifest',
                                                 help="Loads manifest into an analysis.")
    parser_load_manifest.add_argument('-c', '--manifest-csv',
                                      help="""Manifest CSV. Check documentation
                                              for more information.""",
                                     required=True)
    parser_load_manifest.add_argument('-a', '--analysis',
                                      help="Analysis requiring samples.",
                                      required=True)


    # Subparser for loading metadata into an analysis.
    parser_load_metadata = subparsers.add_parser('load-metadata',
                                                 help="Loads metadata for an analysis.")
    parser_load_metadata.add_argument('-f', '--metadata-file',
                                      help="Metadata file. Check docs for more info.",
                                      required=True)
    parser_load_metadata.add_argument('-s', '--sub-dir',
                                      help="Subdirectory for metadata file.", default='')
    parser_load_metadata.add_argument('-a', '--analysis',
                                      help="Analysis to add metadata to.",
                                      required=True)


    # Subparser for loading workflow into an analysis.
    parser_load_component = subparsers.add_parser('load-component',
                                               help="Clones Nextflow component into analysis.")
    parser_load_component.add_argument('-a', '--analysis',
                                    help="Analysis to add workflow to.",
                                    required=True)
    parser_load_component.add_argument('-r', '--repo',
                                    help="Repo for workflow.",
                                    default='')
    parser_load_component.add_argument('-c', '--component',
                                    help="Component to add to analysis.",
                                    required=True)
    # Need support for commits and tags here as well.
    parser_load_component.add_argument('-b', '--branch',
                                    help="Branch to checkout. Default='develop'.",
                                    default='develop')
    parser_load_component.add_argument('-p', '--private',
                                    help="Clones from private subgroup.",
                                    action='store_true',
                                    default=False)
    parser_load_component.add_argument('-n', '--no-deps',
                                    help="Do not automatically load dependencies.",
                                    default=False)


    # Subparser for adding a step into workflow step of an analysis.
    parser_add_step = subparsers.add_parser('add-step',
                                            help="Clones private module into analysis.")
    parser_add_step.add_argument('-a', '--analysis',
                                 help="Analysis to add workflow to.",
                                 required=True)
    parser_add_step.add_argument('-w', '--workflow',
                                 help="Workflow to add step to.",
                                 default='main')
    # Default=BGV NF priv module repo.
    parser_add_step.add_argument('-n', '--no-populate',
                                 help="Load step without placeholder variables.",
                                 action='store_true')


    # Subparser for running workflow on samples.
    parser_run_workflow = subparsers.add_parser('run-workflow',
                                                help="Runs workflow")
    # At least one of these should be required, but should they be mutually exclusive?
    parser_run_workflow.add_argument('-c', '--manifest-csvs',
                                     help="Comma-separated list of manifest CSV(s)")
    parser_run_workflow.add_argument('-s', '--samples',
                                     help="Comma-separated list of sample(s).")
    parser_run_workflow.add_argument('-w', '--workflow',
                                     help="Workflow to run.",
                                     required=True)
    parser_run_workflow.add_argument('-n', '--nf-params',
                                     help="""Param string passed to Nextflow.
                                             Check documentation for
                                             more information.""")
    parser_run_workflow.add_argument('-a', '--analysis',
                                     help="Analysis",
                                     required=True)


    # Subparser for packaging analysis (to generate sharable rftpkg tar file)
    parser_package_analysis = subparsers.add_parser('package-analysis',
                                                    help="""Package analysis
                                                            for distribution.""")
    parser_package_analysis.add_argument('-a', '--analysis',
                                         help="Analysis to package.")
    parser_package_analysis.add_argument('-o', '--output',
                                         help="Output file.",
                                         default='')


    # Subparser for loading analysis (after receiving rftpkg tar file)
    parser_load_analysis = subparsers.add_parser('load-analysis',
                                                 help="""Load an analysis
                                                         from a rftpkg file.""")
    parser_load_analysis.add_argument('-a', '--analysis', help="Analysis name.")
    parser_load_analysis.add_argument('-r', '--rftpkg', help="rftpkg file.")


    return parser.parse_args()


def setup(args):
    """
    Part of the setup mode.

    Installs RAFT into current working directory.
    Installation consists of:

        - Moving any previouls generated RAFT configuration files.

        #Paths
        - Prompting user for paths for paths shared amongst analyses.

        #NF Repos
        - Prompting user for git urls for workflow- and module-level repositories.

        #RAFT Repos
        - Prompting user for git urls for RAFT-specific repositories (storing rftpkgs).

        #Saving
        - Saving these urls in a JSON format in ${PWD}/.raft.cfg

        #Executing
        - Making the required shared paths.
        - Checking out RAFT-specific repositories to repos directory specific in cfg.

    Args:
        args (Namespace object): User-provided arguments.
    """
    print("Setting up RAFT...")
    if args.default:
        print("Using defaults due to -d/--default flag...")
    # DEFAULTS
    raft_paths = {'work': pjoin(getcwd(), 'work'),
                  'analyses': pjoin(getcwd(), 'analyses'),
                  'indices': pjoin(getcwd(), 'indices'),
                  'references': pjoin(getcwd(), 'references'),
                  'fastqs': pjoin(getcwd(), 'fastqs'),
                  'imgs': pjoin(getcwd(), 'imgs'),
                  'repos': pjoin(getcwd(), 'repos')}

    # This prefix should probably be user configurable
    git_prefix = 'git@sc.unc.edu:benjamin-vincent-lab/Nextflow'
    #nextflow-components is a subgroup, not a repo.
    nf_repos = {'nextflow_components': pjoin(git_prefix, 'nextflow-components')}

    raft_repos = {}

    # Ideally, users should be able to specify where .raft.cfg lives.
    cfg_path = pjoin(getcwd(), '.raft.cfg')

    # Make backup of previous configuration file.
    if os.path.isfile(cfg_path):
        bkup_cfg_path = cfg_path + '.orig'
        print("A configuration file already exists.")
        print("Copying original to {}.".format(bkup_cfg_path))
        os.rename(cfg_path, bkup_cfg_path)

    # Setting up filesystem paths.
    if not args.default:
        raft_paths = get_user_raft_paths(raft_paths)

    # Setting up Nextflow workflow/module repositories.
    if not args.default:
        nf_repos = get_user_nf_repos(nf_repos)

    # Setting any RAFT repositories
    if not args.default:
        raft_repos = get_user_raft_repos(raft_repos)


    # Would like to have master_cfg constructed in its own function eventually.
    master_cfg = {'filesystem': raft_paths,
                  'nextflow_repos': nf_repos,
                  'analysis_repos': raft_repos}

    print("Saving configuration file to {}...".format(cfg_path))
    dump_cfg(cfg_path, master_cfg)

    print("Executing configuration file...")
    setup_run_once(master_cfg)

    print("Setup complete.")


def setup_get_user_raft_paths(raft_paths):
    """
    Part of setup mode.

    NOTE: The language should really be cleared up here. Users should
    understand that the keys are simply names while the values are actual
    filesystem paths.

    Prompts user for desired path for directories to be shared among analyses
    (e.g. indexes, fastqs, etc.)

    Args:
        raft_paths (dict): Dictionary containing RAFT paths (e.g. indexes,
        fastqs, etc.) as keys and the default path as values.

    Returns:
        Dictionary containing RAFT paths as keys and user-specified directories as values.
    """
    for raft_path, default in raft_paths.items():
        user_spec_path = input("Provide a shared directory for {} (Default: {}): "
                               .format(raft_path, default))
        # Should be doing some sanity checking here to ensure the path can exist.
        if user_spec_path:
            rath_paths[raft_path] = user_spec_path
    return raft_paths


def setup_get_user_nf_repos(nf_repos):
    """
    Part of setup mode.

    Ideally, Nextflow repos should have more flexibility within RAFT. This is
    sufficient for usage within LCCC/BGV, though.

    Prompts user for desired Nextflow reposities.
    Examples include:
        nextflow-components

    Args:
        nf_repos (dict): Dictionary containing repo names as keys and git url as values.

    Returns:
        Dictionary containing repo names as keys and user-specific git urls as values.
    """
    # Allow users to specify their own Nextflow workflows and modules repos.
    for nf_repo, default in nf_repos.items():
        user_spec_repo = input("Provide a repository for Nextflow {}\n(Default: {}):"
                               .format(nf_repo, default))
        if user_spec_repo:
            nf_repos[nf_repo] = user_spec_repo
    return nf_repos


def setup_get_user_raft_repos(raft_repos):
    """
    Part of setup mode.

    Prompts user for desired RAFT-specific repositories. These are repositories
    for pushing/pulling RAFT analysis packages (rftpkgs).

    Args:
        raft_repos (dict): Empty dictionary.

    Returns:
        Dictionary containing user-specified repo names as keys and
        user-specific git urls (or null) as values. Null values are intended to
        be used to generate local repositories.
    """
    message = ["Provide any git repositories you'd like RAFT to access.",
               "These are for pushing RAFT packages and are not required",
               "for pulling RAFT packages from public repositories.",
               "!!!Make sure ssh/pgp credentials in place before using these repos!!!"]
    print('\n'.join(message))
    repo_qry = input("Would you like to add a repository now? (Y/N)")
    while repo_qry == 'Y':
        repo_name = input("Provide local name (e.g. public, private, repo1):")
        repo_url = input("Provide git url (or hit <ENTER> for local init):")
        raft_repos[repo_name] = repo_url
        repo_qry = input("Would you like to add an additional repository? (Y/N)")
    return raft_repos


def dump_cfg(cfg_path, master_cfg):
    """
    Part of setup mode.

    Writes configuration file to cfg_path.

    Args:
        cfg_path (str): Path for writing output file.
        master_cfg (dict): Dictionary containing configuration information.
    """
    with open(cfg_path, 'w') as fo:
        json.dump(master_cfg, fo, indent=4)


def setup_run_once(master_cfg):
    """
    Part of setup mode.

    Makes/symlinks directories in the 'filesystem' portion of configuration
    file. Clones/initializes any RAFT repositories.

    Args:
        master_cfg (dict): Dictionary containing configuration information.
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
    Part of init-analysis mode.

    Initializes analysis.

    Initializing an analysis includes:
        - Make analysis directory within RAFT /analyses directory.
        - Populate analysis directory using information within specificed
          init_config file.
        - Make a mounts.config file to allow Singularity to access RAFT directories.
        - Make auto.raft file (which records steps taken within RAFT).
        - Create workflow/modules/ directory and <analysis>.nf overall workflow.

    Args:
        args (Namespace object): User-provided arguments
    """
    anlys_dir = mk_anlys_dir(args.name)
    bound_dirs = fill_dir(anlys_dir, args.init_config)
    mk_mounts_cfg(anlys_dir, bound_dirs)
    mk_auto_raft(args)
    mk_main_wf_and_cfg(args)


def mk_main_wf_and_cfg(args):
    """
    Part of the init-analysis mode.

    Copies default main.nf template and creates sparse nextflow.config.

    Args:
        args (Namespace object): User-provided arguments
    """
    raft_cfg = load_raft_cfg()
    template_wf_path = os.path.join(os.getcwd(), '.init.wf')
    template_nfcfg_path = os.path.join(os.getcwd(), '.nextflow.config')
    anlys_wf_path = pjoin(raft_cfg['filesystem']['analyses'],                                      
                                   args.name,
                                   'workflow')
    shutil.copyfile(template_wf_path, pjoin(anlys_wf_path, 'main.wf'))
    shutil.copyfile(template_nfcfg_path, pjoin(anlys_wf_path, 'nextflow.config'))

    

def mk_auto_raft(args):
    """
    Part of the init-analysis mode.

    Makes auto.raft file (within Analysis .raft directory). auto.raft keeps
    track of RAFT commands executed within an analysis.

    Args:
        args (Namespace object): User-provided arguments
    """
    raft_cfg = load_raft_cfg()
    auto_raft_path = pjoin(raft_cfg['filesystem']['analyses'],
                                  args.name,
                                  '.raft',
                                  'auto.raft')

    with open(auto_raft_path, 'w') as fo:
        fo.write("{}\n".format(' '.join(sys.argv)))


def mk_anlys_dir(name):
    """
    Part of the init-analysis mode.

    Makes the analysis directory within the RAFT /analyses directory.

    Args:
        name (str): Analysis name.

    Returns:
        str containing the generated analysis path.
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
    Part of the init-analysis mode.

    Populates an analysis directory with template defined in init_cfg. Returns
    a list of directories to be included in the mounts.config file for the
    analysis.

    Args:
        dir (str): Analysis path.
        init_cfg (str): Initialization configuration path. File should be in
                        JSON format.

    Returns:
        bound_dirs (list): List of directories to be included in mounts.config
                           file.
    """
    # Getting the directories to be bound by this function as well. This should
    # probably be done a different way.
    bound_dirs = []
    raft_cfg = load_raft_cfg()
    req_sub_dirs = {}
    with open(init_cfg) as fo:
        req_sub_dirs = json.load(fo)
    for name, sdir in req_sub_dirs.items():
        # If the desired directory has an included path, link that path to
        # within the analysis directory. This should include some sanity
        # checking to ensure the sub_dir directory even exists.
        if sdir:
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
    Part of the init-analysis mode.

    Creates a mounts.config file for an analysis. This file is provided to
    Nextflow and used to bind directories during Singularity execution. This
    will have to be modified to use Docker, but works sufficiently for
    Singularity now.

    Args:
        dir (str): Analysis path.
        bound_dirs (list): Directories to be included in mounts.config file.
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


def update_mounts_cfg(mounts_cfg, bound_dirs):
    """
    Part of load-manifest mode.

    Updates a mount.config file for an analysis.

    This is primarily intended to update the mount.config file with absolute
    paths for symlinked FASTQs, but can also be used generally.

    Args:
        mount_cfg (str): Path to mounts.config file to update.
        bound_dirs (list): Directories to be included in mounts.config file.
    """
    out = []
    with open(mounts_cfg, 'r') as ifo:
        for line in ifo:
            if re.search('runOptions', line):
                line = line.strip('"\n') + ',{}'.format(','.join(bound_dirs)) + '\n'
            out.append(line)

    with open(mounts_cfg, 'w') as fo:
        for row in out:
            fo.write(row)


def load_manifest(args):
    """
    Part of the load-samples mode.

    Given a user-provided manifest CSV file:
        - Copy file to analysis /metadata directory.
        - Checks to see if any columns not labeled "Dataset", "Patient_ ID" are
          present in the analysis /fastqs directory.
        - Creates <DATASET>/<PATIENT ID> directory in analysis datasets directory.

    NOTE: RAFT assumes any columns (except "Dataset" or "Patient ID") contain
          FASTQ prefixes.

    NOTE: This function will eventually handle more than FASTQs prefixes.

    Args:
        args (Namespace object): User-provided arguments.
    """
    raft_cfg = load_raft_cfg()
    print("Loading samples in analysis {}...".format(args.analysis))
    overall_mani = pjoin(raft_cfg['filesystem']['analyses'],
                         args.analysis,
                         'metadata',
                         args.analysis + '_manifest.csv')

    # This is checking the global, shared FASTQ directory for FASTQs.
    global_fastqs_dir = pjoin(raft_cfg['filesystem']['fastqs'])
    local_fastqs_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'fastqs')
    datasets_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'datasets')

    print("Copying metadata file into analysis metadata directory...")
    if os.path.isdir(pjoin(raft_cfg['filesystem']['analyses'], args.analysis)):
        # If the specified analysis doesn't exist, then should it be created automatically?
        metadata_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        shutil.copyfile(args.manifest_csv,
                        pjoin(metadata_dir, os.path.basename(args.manifest_csv)))

    reconfiged_hdr = 'samp_id,dataset,tissue,prefix\n'
    reconfiged_mani = []

    bound_dirs = [] #Stores directories containing absolute path to FASTQs.

    print("Checking contents of manifest csv...")
    with open(args.manifest_csv) as fo:
        hdr = fo.readline()
        hdr = hdr.strip('\n').split(',')
        # Will certainly need a better way to do this, but this will work for now.
        cols_to_check = [i for i in range(len(hdr)) if hdr[i] not in ['dataset', 'samp_id']]
        dataset_col = hdr.index('dataset')
        samp_id_col = hdr.index('samp_id')

        for row in fo:
            row = row.strip('\n').split(',')
            dataset = row[dataset_col]
            samp_id = row[samp_id_col]

            for col in cols_to_check:
                tissue = hdr[col] #This is where translation will happen, if needed!
                prefix = row[col]
                if prefix == 'NA':
                    continue
                reconfiged_mani.append(','.join([samp_id, dataset, tissue, prefix]))
                print("Checking for FASTQ prefix {} in global /fastqs...".format(prefix))
                hits = glob(pjoin(global_fastqs_dir, prefix + '*'), recursive=True)
                #Check here to ensure that these FASTQs actually belong to the same sample.
                if hits:
                    print("Found FASTQs for prefix {} in /fastqs!".format(prefix))
                    for hit in hits:
                        os.symlink(os.path.realpath(hit), pjoin(local_fastqs_dir, os.path.basename(hit)))
                        #Just adding each file individually for now...
                        bound_dirs.append(os.path.dirname(os.path.realpath(hit)))
                else:
                    print("""Unable to find FASTQs for prefix {} in /fastqs.
                             Check your metadata csv!\n""".format(prefix))

    bound_dirs = list(set(bound_dirs))

    update_mounts_cfg(pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow', 'mounts.config'), bound_dirs)

    with open(overall_mani, 'w') as mfo:
        contents = ''
        try:
            contents = mfo.readlines()
        except:
            pass
        if reconfiged_hdr not in contents:
            mfo.write(reconfiged_hdr)
        mfo.write('\n'.join([row for row in reconfiged_mani if row not in contents]))

    #Need to udpate mounts.config with the symlinks for these FASTQs.





def load_metadata(args):
    """
    Part of the load-metadata mode.

    NOTE: This is effectively load_samples without the sample-level checks.
          These can probably be easily consolidated.

    Given a user-provided metadata CSV file:
        - Copy file to analysis /metadata directory.

    Args:
        args (Namespace object): User-provided arguments.

    """
    raft_cfg = load_raft_cfg()
    if os.path.isdir(pjoin(raft_cfg['filesystem']['analyses'], args.analysis)):
        # If the specified analysis doesn't exist, then should it be created automatically?
        metadata_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'metadata')
        if args.sub_dir:
            os.mkdir(pjoin(metadata_dir, args.sub_dir))
        shutil.copyfile(args.metadata_file,
                        pjoin(metadata_dir, args.sub_dir, os.path.basename(args.metadata_file)))


def recurs_load_components(args):
    """
    Recurively loads Nextflow components. This occurs in multiple waves such that each time a dependencies is loaded/cloned, another wave is initated. This continues until a wave in which no new dependencies are loaded/cloned. There's probably a more intelligent way of doing this, but this should be able to handle the multiple layers of dependencies we're working with. A notable blind spot is the ability to specify the branch to use for each tool (defaults to develop).

    Args:
        args (Namespace object): User-provided arguments.

    """
    raft_cfg = load_raft_cfg()
    wf_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow')
    new_deps = 1
    while new_deps == 1:
        new_deps = 0
        nfs = glob(pjoin(wf_dir, '**', '*.nf'), recursive=True)
        for nf in nfs:
            base = nf.strip('.nf')
            deps = []
            with open(nf) as nffo:
                for line in nffo:
                    if re.search('^include', line):
                        dep = line.split()[-1].replace("'", '').split('/')[1]
                        if dep not in deps:
                            deps.append(dep)
#                deps.append([re.search('.nf', line).group() for line in nffo if re.search('^include', line)])
        for dep in deps:
            if dep not in glob(pjoin(wf_dir)):
                new_deps = 1
                spoofed_args = args
                spoofed_args.component = dep
                load_component(spoofed_args)
             

def load_private_module(args):
    """
    Part of hte load-private-module mode.

    Loads a "private" module. These are modules that are not contained within
    the common nextflow modules repository.

    Args:
        args (Namespace object): User-provided arguments.
    """
    raft_cfg = load_raft_cfg()
    # This shouldn't be hard-coded, but doing it for now.
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['modules_private_subgroup']
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = pjoin(raft_cfg['filesystem']['analyses'],
                             args.analysis,
                             'workflow',
                             args.workflow,
                             'pmodules')
        Repo.clone_from(pjoin(args.repo, args.module),
                        pjoin(workflow_dir, args.module),
                        branch=args.branch)



def load_component(args):
    """
    Part of the load-component mode.

    Loads a Nextflow component (e.g. module) into an analysis.
    Allows users to specify a specific branch to checkout.
    Automatically loads 'develop' branch of modules repo unless specified by user.

    Args:
        args (Namespace object): User-provided arguments.
    """
    raft_cfg = load_raft_cfg()
    if not args.repo:
        args.repo = raft_cfg['nextflow_repos']['nextflow_components']
    if args.analysis:
        # Should probably check here and see if the specified analysis even exists...
        workflow_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow')
        Repo.clone_from(pjoin(args.repo, args.component),
                        pjoin(workflow_dir, args.component),
                        branch=args.branch)
        recurs_load_components(args)
    # Need to add config info to nextflow.config.


def run_workflow(args):
    """
    Part of the run-workflow mode.

    Runs a specified workflow on a user-specific set of sample(s), for all
    samples in manifest csv file(s), or both. Executes checked out branch of
    workflow unless specificed by user.

    Args:
        args (Namespace object): User-provided arguments.
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
    # Thought process here: get string, figure out map between csv columns and workflow params
    # Best thing to do here is to take the manifest CSVs and convert them to a list of strings
    print(all_samp_ids)
    if not all_samp_ids:
        # If there are no samples provided by the user (e.g. no -s or -c), then
        # we can assume this workflow is not meant to be run on a sample-level.
        # This can probably be cleaned up a bit, but this is functional for
        # now.
        generic_nf_cmd = get_generic_nf_cmd(args)
        generic_nf_cmd = prepend_nf_cmd(args, generic_nf_cmd)
        # Going to update with work dir and log dir later.
        #print("Running:\n{}".format(generic_nf_cmd))
        subprocess.run(generic_nf_cmd, shell=True, check=False)
        print("Started process...")
    else:
        for samp_id in all_samp_ids:
            print("Processing sample {}".format(samp_id))
            if samp_id not in processed_samp_ids:
                samp_mani_info = get_samp_mani_info(args.analysis, samp_id)
                # Work directory should probably just be RAFT/work.
                work_dir = raft_cfg['filesystem']['work']
                local_dir = pjoin(raft_cfg['filesystem']['analyses'],
                                  args.analysis,
                                  'datasets',
                                  samp_mani_info['Dataset'],
                                  samp_mani_info['Patient ID'])
                tmp_dir =  pjoin(raft_cfg['filesystem']['analyses'],
                                 args.analysis,
                                 'tmp',
                                 samp_mani_info['Patient ID'],
                                 str(time.time()))

                os.makedirs(work_dir, exist_ok=True)
                os.makedirs(local_dir, exist_ok=True)
                os.makedirs(tmp_dir, exist_ok=True)

                # Constructing a sample-level Nextflow command.
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
                # TODO: This should only be done with all but the last
                # submission. This isn't hard to fix, but not worth it right
                # now.
                print("Waiting 10 seconds before sending next request.")
                time.sleep(10)
                os.chdir(init_dir)


def extract_samp_ids(args, manifest_csv):
    """
    Part of run-workflow mode.

    Given a manifest csv, extract and return sample identifiers.

    Args:
        args (Namespace object): User-provided arguments.
        manifest_csv (str): Path to manifest csv file.

    Returns:
        List containing sample identifiers extracted from manifest csv.
    """
    raft_cfg = load_raft_cfg()
    samp_ids = []
    with open(pjoin(raft_cfg['filesystem']['analyses'],
                    args.analysis,
                    'metadata',
                    os.path.basename(manifest_csv))) as fo:
        hdr = fo.readline().rstrip('\n').split(',')
        pat_idx = hdr.index("Patient ID")
        for line in fo:
            line = line.rstrip('\n').split(',')
            samp_ids.append(line[pat_idx])
    return samp_ids


def add_log_dir(samp_id, args, samp_nf_cmd):
    """
    Part of run-workflow mode.

    Appends logging functionality to Nextflow command.

    Args:
        samp_id (str): Sample identifier.
        args (Namespace object): User-provided arguments.
        samp_nf_cmd (str): Sample-specific Nextflow command.

    Returns:
        Str containing the modified Nextflow command with logging functionality.
    """
    raft_cfg = load_raft_cfg()

    log_dir = pjoin(raft_cfg['filesystem']['analyses'],
                    args.analysis,
                    'logs',
                    '{}.log'.format(samp_id))

    return ' '.join([samp_nf_cmd, '> {} 2>&1 &'.format(log_dir)])


def add_nf_wd(work_dir, samp_nf_cmd):
    """
    Part of run-workflow mode.

    Appends working directory to Nextflow command.

    Args:
        work_dir (str): Work directory path to be appended.
        samp_nf_cmd (str): Sample-specific Nextflow command.

    Returns:
        Str containing the modified Nextflow command with a working directory.
    """
    return ' '.join([samp_nf_cmd, '-w {}'.format(work_dir), '-resume'])


def get_samp_mani_info(analysis, samp_id):
    """
    Part of run-workflow mode.

    Part of the "META:" functionality within run-workflow. This function
    attempts to create a sample-level dictionary where the key is a description
    of a FASTQ prefix (e.g. "WES Tumor") and the value is the FASTQ prefix from
    the manifest csv.

    Args:
        analysis (str): Analysis name.
        samp_id (str): Sample identifer (as defined within manifest csv).

    Returns:
        Dict where keys are descriptors of FASTQ prefixes (e.g. "WES Tumor")
        and values are the FASTQ prefixes.

    """
    # This is kinda gross, clean it up.
    samp_mani_info = {}
    raft_cfg = load_raft_cfg()
    manifest_dir = pjoin(raft_cfg['filesystem']['analyses'], analysis, 'metadata')
    manifest_csvs = glob(pjoin(manifest_dir, '*csv'))
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
    Part of run-workflow mode.

    Prepends the actual Nextflow execution portion to Nextflow command prior to
    execution. This currently globs for a *.nf file within the specified
    workflow directory (so workflow Nextflow files do NOT have to be named the
    same as the workflow repo).

    Args:
        args (Namespace object): User-specific arguments.
        samp_nf_cmd (str): Nextflow command.

    Returns:
        Str containing modified Nextflow command with execution portion.
    """
    raft_cfg = load_raft_cfg()
    workflow_dir = pjoin(raft_cfg['filesystem']['analyses'], args.analysis, 'workflow', args.workflow)
    print(workflow_dir)
    #Ensure only one nf is discoverd here! If more than one is discovered, then should multiple be run?
    discovered_nf = glob(pjoin(workflow_dir, '*.nf'))[0]
    print(discovered_nf)
#    cmd = ' '.join(['nextflow', discovered_nf, samp_nf_cmd])
    cmd = ' '.join(['nextflow', discovered_nf, samp_nf_cmd, '-resume'])
    return cmd


def get_samp_nf_cmd(args, samp_mani_info):
    """
    Part of run-workflow mode.

    Given a nextflow command (from args), parse the command and replace any
    instances with "META:<PATTERN>" with the correct sample-level information
    from the manifest csv. This is primarily performed when replacing the
    "META:" pattern with a FASTQ prefix. This allows users to execut Nextflow
    on several samples without having to manually construct individual sample
    commands.

    Args:
        args (Namespace object): User-specific arguments.
        samp_mani_info (dict): Dictionary where keys are FASTQ prefix
                               descriptors and values are FASTQ prefixes.

    Returns:
        Str containing modified Nextflow command with appropriate sample-level
        information.
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
    Part of run-workflow mode.

    Generates a generic Nextflow command. This is intended for running Nextflow
    commands that do not operate on single samples. This specifically adds the
    --analysis_dir parameter.

    Args:
        args (Namespace object): User-provided arguments.

    Return:
        Str containing modified Nextflow command.
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
    Creates a random 5mer.
    """
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(size))


def load_raft_cfg():
    """
    Part of several modes.

    This function reads the RAFT configuration file and provides a dictionary
    with configuration information.

    Returns:
        Dictionary with configuration information.
    """
    cfg = {}
    cfg_path = os.path.join(os.getcwd(), '.raft.cfg')
    with open(cfg_path) as fo:
        cfg = json.load(fo)
    return cfg


def dump_to_auto_raft(args):
    """
    Part of several modes.

    Called anytime RAFT is called with non-administrative commands. This copies
    commands to auto.raft file.

    Args:
        args (Namespace object): User-specified arguments.
    """
    if args.command not in ['init-analysis', 'run-auto', 'package-analysis',
                            'load-analysis', 'setup']:
        raft_cfg = load_raft_cfg()
        auto_raft_path = pjoin(raft_cfg['filesystem']['analyses'],
                               args.analysis,
                               '.raft',
                               'auto.raft')
        with open(auto_raft_path, 'a') as fo:
            fo.write("{}\n".format(' '.join(sys.argv)))


def package_analysis(args):
    """
    Part of package-analysis mode.
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


#https://stackoverflow.com/a/3431835
def file_as_bytes(file):
    with file:
        return file.read()

def load_analysis(args):
    """
    Part of load-analysis mode.
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
        setup(args)
    elif args.command == 'init-analysis':
        init_analysis(args)
    elif args.command == 'load-manifest':
        load_manifest(args)
    elif args.command == 'load-metadata':
        load_metadata(args)
    elif args.command == 'load-component':
        load_component(args)
    elif args.command == 'add-step':
        add_step(args)
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
