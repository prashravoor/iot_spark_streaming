import sys
import os
import random
from time import sleep
from datetime import datetime, timedelta

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 5:
        print('Usage: cmd <Out directory> <cpu> <disk> <network>')
        exit()

    outdir = args[1] 
    cpu = args[2]
    disk = args[3]
    net = args[4]
    #random.seed(8192) # works for 2 sec window
    random.seed(420)

    max_lines_batch = 1000
    sleep_time = 2
    with open(cpu) as infile:
        cpulines = [x.split(',')[1].strip() for x in infile.readlines()]

    with open(disk) as infile:
        disklines = [x.split(',')[1].strip() for x in infile.readlines()]

    with open(net) as infile:
        netlines = [x.split(',')[1].strip() for x in infile.readlines()]

    # Pad all lines to max len
    maxlines = max(len(cpulines), len(disklines), len(netlines))
    print('Total lines: {} (Org: CPU: {}, Disk: {}, Net: {}'.format(maxlines, len(cpulines), len(disklines), len(netlines)))

    cpulines.extend(['0' for _ in range(maxlines - len(cpulines))])
    disklines.extend(['0' for _ in range(maxlines - len(disklines))])
    netlines.extend(['0' for _ in range(maxlines - len(netlines))])

    #lines = list(zip(cpulines, disklines, netlines)) # Create tuples of the form (cpuval, diskval, netval)

    i = 0
    while i < len(cpulines):
        end = random.randint(i, min(len(cpulines), i+max_lines_batch))
        curdate = datetime.today()
        print('Writing {} lines...'.format(end - i))
        with open('{}_cpu/{}.csv'.format(outdir, random.randint(1,10000)), 'w') as f:
            vals = cpulines[i:end]
            towrite = ['\n'.join(['cpu,{},{}'.format(curdate.strftime('%Y-%m-%d %H:%M:%S'), x) for x in vals])]
            f.writelines(towrite)

        with open('{}_disk/{}.csv'.format(outdir, random.randint(1,10000)), 'w') as f:
            vals = disklines[i:end]
            towrite = ['\n'.join(['disk,{},{}'.format(curdate.strftime('%Y-%m-%d %H:%M:%S'), x) for x in vals])]
            f.writelines(towrite)

        with open('{}_net/{}.csv'.format(outdir, random.randint(1,10000)), 'w') as f:
            vals = netlines[i:end]
            towrite = ['\n'.join(['net,{},{}'.format(curdate.strftime('%Y-%m-%d %H:%M:%S'), x) for x in vals])]
            f.writelines(towrite)

        curdate += timedelta(seconds=3)

        i = end
        print('Sleeping...')
        sleep(sleep_time)
