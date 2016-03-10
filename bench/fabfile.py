from fabric.api import run, env
from fabric.context_managers import cd, shell_env

env.user = 'ubuntu'


def clone_dagger():
    run('')
    run('git clone https://github.com/nsaje/dagger')


def build_dagger():
    with cd('$GOPATH/dagger'):
        run('git pull origin master')
        run('go install ./...')
