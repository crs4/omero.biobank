import csv, sys, argparse

from bl.vl.utils import LOG_LEVELS, get_logger
from bl.vl.kb import KnowledgeBase as KB
import bl.vl.utils.ome_utils as vlu

DIAGNOSIS_ARCHETYPE = 'openEHR-EHR-EVALUATION.problem-diagnosis.v1'
EXCL_DIAG_ARCHETYPE = 'openEHR-EHR-EVALUATION.exclusion-problem_diagnosis.v1'

def make_parser():
    parser = argparse.ArgumentParser(description='dump diagnosis data')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices=LOG_LEVELS,
                        help='logging level', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname')
    parser.add_argument('-U', '--user', type=str, help='omero user')
    parser.add_argument('-P', '--passwd', type=str, help='omero password')
    parser.add_argument('--ofile', type=str, required=True,
                        help='output file')
    return parser

def extract_csv_row(ehr_record, actions_map):
    if ehr_record['archetype'] == DIAGNOSIS_ARCHETYPE:
        return {'study' : actions_map[ehr_record['a_id']],
                'individual' : ehr_record['i_id'],
                'timestamp' : ehr_record['timestamp'],
                'diagnosis' : ehr_record['fields']['at0002.1']}
    elif ehr_record['archetype'] == EXCL_DIAG_ARCHETYPE:
        return {'study' : actions_map[ehr_record['a_id']],
                'individual' : ehr_record['i_id'],
                'timestamp' : ehr_record['timestamp'],
                'diagnosis' : 'exclusion-problem_diagnosis'}
    else:
        raise ValueError('Cannot handle archetype: %s' % ehr_record['archetype'])

def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    logger = get_logger("main", level=args.loglevel, filename=args.logfile)

    try:
        host   = args.host or vlu.ome_host()
        user   = args.user or vlu.ome_user()
        passwd = args.passwd or vlu.ome_passwd()
    except ValueError, ve:
        logger.critical(ve)
        sys.exit(ve)

    kb = KB(driver='omero')(host, user, passwd)

    logger.info('Retrieving diagnosis records')
    ehr_records = kb.get_ehr_records('(archetype == "%s") & (valid == True)' %
                                     DIAGNOSIS_ARCHETYPE)
    ehr_records.extend(kb.get_ehr_records('(archetype == "%s") & (valid == True)' %
                                          EXCL_DIAG_ARCHETYPE))
    logger.info('%d records retrieved' % len(ehr_records))

    logger.info('Loading actions')
    actions = kb.get_objects(kb.Action)
    act_map = {}
    for act in actions:
        act_map[act.id] = act.context.label
    logger.info('%d actions loaded' % len(act_map))

    with open(args.ofile, 'w') as out_file:
        writer = csv.DictWriter(out_file, ['study', 'individual',
                                           'timestamp', 'diagnosis'],
                                delimiter='\t')
        writer.writeheader()
        for rec in ehr_records:
            writer.writerow(extract_csv_row(rec, act_map))

    logger.info('Job completed')
    

if __name__ == '__main__':
    main(sys.argv[1:])
