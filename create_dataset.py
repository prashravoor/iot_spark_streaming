import sys
import numpy as np
import os

args = sys.argv
if not len(args) == 3:
    print('Usage: cmd <in folder> <out file>')
    exit()

infolder = args[1]
outfile = args[2]
types = ['cpu', 'disk', 'network']

num_values = 5000

files = [os.path.join(infolder,x) for x in os.listdir(infolder) if x.endswith('.csv')]
cpufiles = [x for x in files if types[0] in x]
diskfiles = [x for x in files if types[1] in x]
netfiles = [x for x in files if types[2] in x]

print('Total files: {}, CPU files: {}, Disk files: {}, Net files: {}'.format(len(files), len(cpufiles), len(diskfiles), len(netfiles)))

cpuvals = []
for f in cpufiles:
    with open(f) as a:
        cpuvals.extend([x.split(',')[1].strip() for x in a.readlines()[1:]])

diskvals = []
for f in diskfiles:
    with open(f) as a:
        diskvals.extend([x.split(',')[1].strip() for x in a.readlines()[1:]])

netvals = []
for f in netfiles:
    with open(f) as a:
        netvals.extend([x.split(',')[1].strip() for x in a.readlines()[1:]])

print('Found {} CPU recs, {} for disk and {} for network'.format(len(cpuvals), len(diskvals), len(netvals)))
print('Random sampling {} records each'.format(num_values))

np.random.seed(42)
newcpuvals = np.random.choice(cpuvals, num_values, replace=False)
newdiskvals = np.random.choice(diskvals, num_values, replace=False)
newnetvals = np.random.choice(netvals, num_values, replace=False)

with open(outfile, 'w') as f:
    f.writelines('\n'.join([','.join(list(x)) for x in zip(newcpuvals, newdiskvals, newnetvals)]))
    
