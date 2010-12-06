#!/usr/bin/env python

import sys, os, re, operator


BASE_DIR = "mr_output"
OUT_FN = "timings.tsv"

def get_n_mappers(d):
    N = 0
    for fn in os.listdir(d):
        if not fn.startswith("part"):
            continue
        fn = os.path.join(d, fn)
        f = open(fn)
        records = f.readlines()
        f.close()
        n = sum(1 for r in records if r.strip())
        N += n
    return N


def get_total_time(d):
    launch_pattern = re.compile(r'Job \S+ LAUNCH_TIME="(\d+)"')
    finish_pattern = re.compile(r'Job \S+ FINISH_TIME="(\d+)"')
    logd = os.path.join(d, "_logs", "history")
    logfn = [fn for fn in os.listdir(logd) if not fn.endswith(".xml")]
    assert len(logfn) == 1
    logfn = os.path.join(logd, logfn[0])
    f = open(logfn)
    for line in f:
        m = launch_pattern.match(line)
        if m is not None:
            launch_time = int(m.groups()[0])
            break
    last_line = get_last_line(f)
    f.close()
    try:
        finish_time = int(finish_pattern.match(last_line).groups()[0])
    except AttributeError:
        print line
        raise
    return (finish_time - launch_time) / 1000.


def get_last_line(f, offset=-1000):
    f.seek(offset, os.SEEK_END)
    lines = [l for l in f if l.strip()]
    if len(lines) < 2:
        return get_last_line(f, 2*offset)
    return lines[-1]


def main(argv):
    dir_pattern = re.compile(r"(\d+)bit_chr(\d+)_mr_output")
    data = []
    dir_list = os.listdir(BASE_DIR)
    for i, d in enumerate(dir_list):
        print "Processing %s (%d of %d)" % (d, i+1, len(dir_list))
        m = dir_pattern.match(d)
        try:
            bits, chr = map(int, m.groups())
        except AttributeError:
            continue
        d = os.path.join(BASE_DIR, d)
        n = get_n_mappers(d)
        t = get_total_time(d)
        data.append([chr, bits, t/n])
    data.sort(key=operator.itemgetter(1))
    data.sort()
    outf = open(OUT_FN, "w")
    for chr, bits, t in data:
        outf.write("%d\t%d\t%.3f\n" % (chr, bits, t))
    outf.close()


if __name__ == "__main__":
    main(sys.argv)
