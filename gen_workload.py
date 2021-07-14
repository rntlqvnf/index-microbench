import sys
import os
import random

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

#####################################################################################

def reverseHostName ( email ) :
    name, sep, host = email.partition('@')
    hostparts = host[:-1].split('.')
    r_host = ''
    for part in hostparts :
        r_host = part + '.' + r_host
    return r_host + sep + name

#####################################################################################

if (len(sys.argv) != 2) :
    print bcolors.WARNING + 'Usage:'
    print 'workload file' + bcolors.ENDC

config_file = sys.argv[1]

args = []
f_config = open (config_file, 'r')
for line in f_config :
    args.append(line[:-1])

ycsb_dir = 'YCSB/'
workload_dir = 'workload_spec/'
output_dir='workloads/'

workload = args[0]
key_type = args[1]
test_type = sys.argv[2] # [performance, coverage]

print bcolors.OKGREEN + 'workload = ' + workload
print 'key type = ' + key_type + bcolors.ENDC

email_list = 'list.txt'
email_list_size = 100000

out_ycsb_load = output_dir + 'ycsb_load_' + key_type + '_' + workload
out_ycsb_txn = output_dir + 'ycsb_txn_' + key_type + '_' + workload
out_load_ycsbkey = output_dir + 'load_' + 'ycsbkey' + '_' + workload
out_txn_ycsbkey = output_dir + 'txn_' + 'ycsbkey' + '_' + workload
out_load = output_dir + test_type + '_' + 'load_' + key_type + '_' + workload
out_txn = output_dir + test_type + '_' + 'txn_' + key_type + '_' + workload

cmd_ycsb_load = ycsb_dir + 'bin/ycsb load basic -P ' + workload_dir + workload + ' -s > ' + out_ycsb_load
cmd_ycsb_txn = ycsb_dir + 'bin/ycsb run basic -P ' + workload_dir + workload + ' -s > ' + out_ycsb_txn

os.system(cmd_ycsb_load)
os.system(cmd_ycsb_txn)

#####################################################################################

f_load = open (out_ycsb_load, 'r')
f_load_out = open (out_load_ycsbkey, 'w')
for line in f_load :
    cols = line.split()
    if len(cols) > 0 and cols[0] == "INSERT":
        f_load_out.write (cols[0] + " " + cols[2][4:] + "\n")
f_load.close()
f_load_out.close()

f_txn = open (out_ycsb_txn, 'r')
f_txn_out = open (out_txn_ycsbkey, 'w')
for line in f_txn :
    cols = line.split()
    if (cols[0] == 'SCAN') or (cols[0] == 'INSERT') or (cols[0] == 'READ') or (cols[0] == 'UPDATE'):
        startkey = cols[2][4:]
        if cols[0] == 'SCAN' :
            numkeys = cols[3]
            f_txn_out.write (cols[0] + ' ' + startkey + ' ' + numkeys + '\n')
        else :
            f_txn_out.write (cols[0] + ' ' + startkey + '\n')
f_txn.close()
f_txn_out.close()

cmd = 'rm -f ' + out_ycsb_load
os.system(cmd)
cmd = 'rm -f ' + out_ycsb_txn
os.system(cmd)

#####################################################################################

eof_bof = ['EOF', 'BOF']
operators = ['EQ', 'LT', 'LE', 'GT', 'GE']

if key_type == 'randint' :
    f_load = open (out_load_ycsbkey, 'r')
    f_load_out = open (out_load, 'w')
    for line in f_load :
        f_load_out.write (line)

    f_txn = open (out_txn_ycsbkey, 'r')
    f_txn_out = open (out_txn, 'w')
    for line in f_txn :
        if cols[0] == 'UPDATE' :
            f_txn_out.write ('DELETE' + ' ' + cols[1] + '\n')
        elif cols[0] == 'READ' :
            f_txn_out.write ('SCAN' + ' ' + random.choice(eof_bof) + ' ' + random.choice(eof_bof) + '\n')
        else :
            f_txn_out.write (line)

elif key_type == 'monoint' :
    keymap = {}
    f_load = open (out_load_ycsbkey, 'r')
    f_load_out = open (out_load, 'w')
    count = 0
    for line in f_load :
        cols = line.split()
        keymap[int(cols[1])] = count
        f_load_out.write (cols[0] + ' ' + str(count) + '\n')
        count += 1

    f_txn = open (out_txn_ycsbkey, 'r')
    f_txn_out = open (out_txn, 'w')
    for line in f_txn :
        cols = line.split()
        if cols[0] == 'INSERT' :
            keymap[int(cols[1])] = count
            f_txn_out.write (cols[0] + ' ' + str(count) + '\n')
            count += 1
        elif cols[0] == 'UPDATE' :
            if test_type == 'performance' :
                f_txn_out.write ('DELETE' + ' ' + str(keymap[int(cols[1])]) + '\n')
                f_txn_out.write ('INSERT' + ' ' + str(keymap[int(cols[1])]) + '\n')
            elif test_type == 'coverage' :
                f_txn_out.write ('DELETE' + ' ' + str(keymap[int(cols[1])]) + '\n')
        elif cols[0] == 'SCAN' :
            if test_type == 'performance' :
                f_txn_out.write (cols[0] + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + ' ' + 'EOF' + ' ' + cols[2] + '\n')
            elif test_type == 'coverage' :
                wraparound_second_key = (int(cols[1]) + int(cols[2])) % count
                f_txn_out.write (cols[0] + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + ' ' +
                                random.choice(operators) + ' ' + str(wraparound_second_key) + '\n')
        elif cols[0] == 'READ' :
            if test_type == 'performance' :
                f_txn_out.write ('SCAN' + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + '\n')
            elif test_type == 'coverage' :
                if random.randint(0,1) == 0 :
                    f_txn_out.write ('SCAN' + ' ' + random.choice(eof_bof) + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + '\n')
                else :
                    f_txn_out.write ('SCAN' + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + ' ' + random.choice(eof_bof) + '\n')
        else :
            f_txn_out.write (cols[0] + ' ' + str(keymap[int(cols[1])]) + '\n')

elif key_type == 'email' :
    keymap = {}
    f_email = open (email_list, 'r')
    emails = f_email.readlines()

    f_load = open (out_load_ycsbkey, 'r')
    f_load_out = open (out_load, 'w')

    sample_size = len(f_load.readlines())
    gap = email_list_size / sample_size

    f_load.close()
    f_load = open (out_load_ycsbkey, 'r')
    count = 0
    for line in f_load :
        cols = line.split()
        email = emails[count * gap].rstrip()
        keymap[int(cols[1])] = email
        f_load_out.write (cols[0] + ' ' + email + '\n')
        count += 1

    tot_count = count
    count = 0
    f_txn = open (out_txn_ycsbkey, 'r')
    f_txn_out = open (out_txn, 'w')
    for line in f_txn :
        cols = line.split()
        if cols[0] == 'INSERT' :
            email = emails[count * gap + 1].rstrip()
            keymap[int(cols[1])] = email
            f_txn_out.write (cols[0] + ' ' + email + '\n')
            count += 1
        elif cols[0] == 'UPDATE' :
            if test_type == 'performance' :
                f_txn_out.write ('DELETE' + ' ' + str(keymap[int(cols[1])]) + '\n')
                f_txn_out.write ('INSERT' + ' ' + str(keymap[int(cols[1])]) + '\n')
            elif test_type == 'coverage' :
                f_txn_out.write ('DELETE' + ' ' + str(keymap[int(cols[1])]) + '\n')
        elif cols[0] == 'SCAN' :
            if test_type == 'performance' :
                f_txn_out.write (cols[0] + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + ' ' + 'EOF' + ' ' + cols[2] + '\n')
            elif test_type == 'coverage' :
                wraparound_second_key = (int(cols[1]) + int(cols[2])) % tot_count
                f_txn_out.write (cols[0] + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + ' ' +
                                random.choice(operators) + ' ' + str(wraparound_second_key) + '\n')
        elif cols[0] == 'READ' :
            if test_type == 'performance' :
                f_txn_out.write ('SCAN' + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + ' ' + 'EQ' + ' ' + str(keymap[int(cols[1])]) + '\n')
            elif test_type == 'coverage' :
                if random.randint(0,1) == 0 :
                    f_txn_out.write ('SCAN' + ' ' + random.choice(eof_bof) + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + '\n')
                else :
                    f_txn_out.write ('SCAN' + ' ' + random.choice(operators) + ' ' + str(keymap[int(cols[1])]) + ' ' + random.choice(eof_bof) + '\n')
        else :
            f_txn_out.write (cols[0] + ' ' + keymap[int(cols[1])] + '\n')

f_load.close()
f_load_out.close()
f_txn.close()
f_txn_out.close()

cmd = 'rm -f ' + out_load_ycsbkey
os.system(cmd)
cmd = 'rm -f ' + out_txn_ycsbkey
os.system(cmd)
