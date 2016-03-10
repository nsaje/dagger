import datetime
import time
import subprocess
import argparse
import multiprocessing

parser = argparse.ArgumentParser(description='Benchmark Dagger.')
parser.add_argument('--deadline', dest='deadline', default=2, type=int)
parser.add_argument('--runfor', dest='runfor', default=5, type=int)
parser.add_argument('--publishers', dest='publishers', default=1, type=int)
parser.add_argument('--pubstream', dest='pubstream', default='bench')
parser.add_argument('--subscribers', dest='subscribers', default=1, type=int)
parser.add_argument('--substream', dest='substream', default='bench')
parser.add_argument('--test', dest='test', default='direct')
parser.add_argument('--consul', dest='consul', default='172.17.0.2:8500')


def pub_worker(chan, consul, deadline, runfor, stream):
    dagger = subprocess.Popen(['dagger', 'p', '-consul', consul, '-s', stream], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    now = datetime.datetime.utcnow()
    sleep_for = (deadline - now).seconds
    print "pub worker sleeping for ", sleep_for
    time.sleep(sleep_for)

    end = deadline + datetime.timedelta(seconds=runfor)
    # for i in xrange(100):
    count = 0
    while datetime.datetime.utcnow() < end:
        dagger.stdin.write('a%d\n' % count)
        dagger.stdout.readline()
        count += 1
    time.sleep(2.0)
    dagger.terminate()

    chan.send(count)
    chan.close()


def sub_worker(chan, consul, deadline, runfor, stream, i=0):
    dagger = subprocess.Popen(['dagger', 's', '-consul', consul, '-i', 'virbr0',
                               '-p', '%s' % (46667 + i), stream], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    now = datetime.datetime.utcnow()
    sleep_for = (deadline - now).seconds
    print "sub worker sleeping for ", sleep_for
    time.sleep(sleep_for)

    count = 0
    end = deadline + datetime.timedelta(seconds=runfor)
    while datetime.datetime.utcnow() < end:
        dagger.stdout.readline()
        count += 1
    time.sleep(2.0)
    dagger.terminate()

    chan.send(count)
    chan.close()


def start_worker(target, args):
    parent_chan, child_chan = multiprocessing.Pipe()
    proc = multiprocessing.Process(target=target, args=(child_chan,) + args)
    proc.start()
    return (proc, parent_chan)


if __name__ == '__main__':
    args = parser.parse_args()

    dt = datetime.datetime.utcnow()
    deadline = dt + datetime.timedelta(seconds=args.deadline)
    print 'deadline ', deadline

    stream = 'bench'
    pubs = []
    subs = []
    if args.test == 'direct':
        for i in range(args.publishers):
            pubs.append(start_worker(target=pub_worker, args=(args.consul, deadline, args.runfor, stream)))
        subs.append(start_worker(target=sub_worker, args=(args.consul, deadline, args.runfor, stream)))

    if args.test == 'task':
        for i in range(args.publishers):
            pubs.append(start_worker(target=pub_worker, args=(args.consul, deadline, args.runfor, stream)))
        subs.append(start_worker(target=sub_worker, args=(args.consul, deadline, args.runfor, 'foo(%s)' % stream)))

    if args.test == 'many-many':
        for i in range(args.publishers):
            pubs.append(start_worker(target=pub_worker, args=(args.consul, deadline, args.runfor, '%s%d' % (stream, i))))
        for i in range(args.subscribers):
            subs.append(start_worker(target=sub_worker, args=(args.consul, deadline, args.runfor, 'foo(%s%d)' % (stream, i), i)))

    if args.test == 'many-one':
        for i in range(args.publishers):
            pubs.append(start_worker(target=pub_worker, args=(args.consul, deadline, args.runfor, '%s{hostname=%d}' % (stream, i))))
        subs.append(start_worker(target=sub_worker, args=(args.consul, deadline, args.runfor, 'foo(%s)' % (stream,))))

    count = 0
    for proc, chan in pubs:
        count += chan.recv()
        proc.join()
    print 'write throughput %f msgs/s' % (count / args.runfor)

    count = 0
    for proc, chan in subs:
        count += chan.recv()
        proc.join()
    print 'read throughput %f msgs/s' % (count / args.runfor)
