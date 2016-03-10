import datetime
import subprocess
import argparse
import multiprocessing

parser = argparse.ArgumentParser(description='Benchmark Dagger.')
parser.add_argument('--deadline', dest='deadline', default=10, type=int)
parser.add_argument('--runfor', dest='runfor', default=10, type=int)
parser.add_argument('--publishers', dest='publishers', default=1, type=int)
parser.add_argument('--subscribers', dest='subscribers', default=1, type=int)
parser.add_argument('--url', dest='url', default='http://127.0.0.1:46666')

if __name__ == '__main__':
    args = parser.parse_args()

    dt = datetime.datetime.utcnow()
    deadline = dt + datetime.timedelta(args.deadline)

    pub_procs = []
    for p_num in range(args.publishers):
        pub_procs.append(subprocess.popen([
            'go', 'run', 'bench.go',
            '-role=publish'
            '-deadline=%s' % args.deadline,
            '-runfor=%s' % args.runfor,
            '-deadline=%s' % deadline.strftime('%y-%m-%d %h:%m:%s'),
            '-url=%s' % args.url,
        ], stdout=subprocess.PIPE))

    sub_procs = []
    for s_num in range(args.subscribers):
        sub_procs.append(subprocess.popen([
            'go', 'run', 'bench.go',
            '-role=subscribe'
            '-deadline=%s' % args.deadline,
            '-runfor=%s' % args.runfor,
            '-deadline=%s' % deadline.strftime('%y-%m-%d %h:%m:%s'),
            '-url=%s' % args.url,
        ], stdout=subprocess.PIPE))

    print "publishers:"
    for p in pub_procs:
        p.wait()
        print p.stdout.read()

    print "subscribers:"
    for p in sub_procs:
        p.wait()
        print p.stdout.read()
