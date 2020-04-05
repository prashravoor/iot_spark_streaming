import sys
import os
import random
from time import sleep

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 5:
        print('Usage: cmd <Out directory> <cpu> <disk> <network>')
        exit()

    outdir = args[1] 
    cpu = args[2]
    disk = args[3]
    net = args[4]
    random.seed(8192) # works for 2 sec window
    #random.seed(42)

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

    lines = list(zip(cpulines, disklines, netlines)) # Create tuples of the form (cpuval, diskval, netval)

    i = 0
    while i < len(lines):
        end = random.randint(i, min(len(lines), i+max_lines_batch))
        print('Writing {} lines...'.format(end - i))
        with open('{}/{}.csv'.format(outdir, random.randint(1,10000)), 'w') as f:
            vals = lines[i:end]
            towrite = [','.join(x) for x in vals]
            f.writelines('\n'.join(towrite))

        i = end
        print('Sleeping...')
        sleep(sleep_time)
