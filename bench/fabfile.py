# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

from fabric.api import run, env, put
from fabric.context_managers import cd, shell_env
from fabric.contrib.files import exists


env.user = 'ubuntu'
env.forward_agent = True

def runbg(cmd, sockname="dtach"):
    return run('DAGGER_PLUGIN_PATH=/home/ubuntu dtach -n `mktemp -u /tmp/%s.XXXX` %s'  % (sockname,cmd))


def clone_dagger():
    run('')
    run('git clone https://github.com/nsaje/dagger')


def build_dagger():
    with cd('$GOPATH/dagger'):
        run('git pull origin master')
        run('go install ./...')

def update_dagger():
	put('~/bin/dagger', '/home/ubuntu/', mirror_local_mode=True)
	put('~/bin/computation-*', '/home/ubuntu/', mirror_local_mode=True)

def restart_workers():
    #if exists('dagger.pid'):
    #    run('kill -9 `cat dagger.pid` || true')
    run('killall dagger || true')
    runbg('./dagger w -consul 172.31.56.90:8500 --iface eth0 --log dagger.log; echo $! > dagger.pid')
