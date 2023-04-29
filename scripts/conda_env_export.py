"""
Adapted from:
https://gist.github.com/gwerbin/dab3cf5f8db07611c6e0aeec177916d8

Export a Conda environment with --from-history, but also append
Pip-installed dependencies

Exports only manually-installed dependencies, excluding build versions, but
including Pip-installed dependencies.

Lots of issues requesting this functionality in the Conda issue tracker, no
sign of progress (as of March 2020).

TODO (?): support command-line flags -n and -p
"""
import re
import subprocess
import sys
import getopt

import yaml


def export_env(history_only=False, include_builds=False):
    """ Capture `conda env export` output """
    cmd = ['conda', 'env', 'export']
    if history_only:
        cmd.append('--from-history')
        if include_builds:
            raise ValueError('Cannot include build versions with "from history" mode')
    if not include_builds:
        cmd.append('--no-builds')
    cp = subprocess.run(cmd, stdout=subprocess.PIPE)
    try:
        cp.check_returncode()
    except:
        raise
    else:
        return yaml.safe_load(cp.stdout)


def _is_history_dep(d, history_deps):
    if not isinstance(d, str):
        return False
    d_prefix = re.sub(r'=.*', '', d)
    return d_prefix in history_deps


def _get_pip_deps(full_deps):
    for dep in full_deps:
        if isinstance(dep, dict) and 'pip' in dep:
            return dep


def _combine_env_data(env_data_full, env_data_hist):
    deps_full = env_data_full['dependencies']
    deps_hist = env_data_hist['dependencies']
    # deps = [dep for dep in deps_full if _is_history_dep(dep, deps_hist)]
    # in my case, I wanted this from-history dependencies, not the full dependencies
    # the from-history dependencies tend to platform agnostic and easier to move cross-platform
    deps = deps_hist

    pip_deps = _get_pip_deps(deps_full)

    env_data = {}
    env_data['channels'] = env_data_full['channels']
    env_data['dependencies'] = deps
    env_data['dependencies'].append(pip_deps)

    return env_data


def main():
    filename = ''
    argumentList = sys.argv[1:]
    options = "o:"
    long_options = ['output']
    arguments, values = getopt.getopt(argumentList, options, long_options)
    for opt, arg in arguments:
      if opt in ['-o', '--output']:
        filename=arg

    if filename=='':
      raise RuntimeError('filename not set, set it with flag -o [filename] or --output [filename]')

    env_data_full = export_env(history_only=False)
    env_data_hist = export_env(history_only=True)
    env_data = _combine_env_data(env_data_full, env_data_hist)
    with open(filename, 'w') as outfile:
        yaml.dump(env_data, outfile)
    print('Warning: this output might contain packages installed from non-public sources, e.g. a Git repository. '
          'You should review and test the output to make sure it works with `conda env create -f`, '
          'and make changes as required.\n'
          'For example, `conda-env-export` itself is not currently uploaded to PyPI, and it must be removed from '
          'the output file, or else `conda create -f` will fail.', file=sys.stderr)


if __name__ == '__main__':
    main()
