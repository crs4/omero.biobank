#======================================= 
# This tool moves all informations related to an individual (source) to
# another (target). Moved informations are:
# * children (Individual objects)
# * ActionOnInvidual
# * Enrollments
# * EHR records
#
# At the end of the merge procedure, the script will try to delete the
# "source" individual
#
# The tool expects as input a TSV file like this
#   source                               target
#   V0468D2D96999548BF9FC6AD24C055E038   V060BAA01C662240D181BB98A51885C498
#   V029CC0A614E2D42D0837602B15193EB58   V01B8122A7C75A452E9F80381CEA988557
#   V0B20C93E8A88D43EFB87A7E6911292A05   V0BED85E8E76A54AA7AB0AFB09F95798A8
#   ...
#
# NOTE WELL:
# * Parents of the "source" indivudal WILL NOT BE ASSIGNED to the "target" individual
# * For the Enrollmnent objects, if "target" individual has already a
#   code in the same study of "source" individual, the script will try
#   to move the Enrollment to the "duplicated" study (this will be
#   fixed when a proper ALIASES manegement will be introduced)
# =======================================

import sys, argparse, logging, traceback, csv

from bl.vl.kb import KnowledgeBase as KB
from bl.vl.kb import KBError


LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


def make_parser():
    parser = argparse.ArgumentParser(description='merge informations related to an individual ("source") to another one ("destination")')
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    parser.add_argument('--loglevel', type=str, choices = LOG_LEVELS,
                        help='logging level (default=INFO)', default='INFO')
    parser.add_argument('-H', '--host', type=str, help='omero hostname',
                        default='localhost')
    parser.add_argument('-U', '--user', type=str, help='omero user',
                        default='root')
    parser.add_argument('-P', '--passwd', type=str, required = True,
                        help='omero password')
    parser.add_argument('--in_file', type=str, required = True,
                        help='input TSV file')
    return parser

def update_children(source_ind, target_ind, kb):
    logger = logging.getLogger()
    if source_ind.gender.enum_label() == kb.Gender.MALE.enum_label():
        parent_type = 'father'
    elif source_ind.gender.enum_label() == kb.Gender.FEMALE.enum_label():
        parent_type = 'mother'
    else:
        raise ValueError('%s is not a valid gender value' % (source_ind.gender.enum_label()))
    query = '''
            SELECT ind FROM Individual ind
            JOIN ind.{0} AS {0}
            WHERE {0}.vid = :parent_vid
            '''.format(parent_type)
    children = kb.find_all_by_query(query, {'parent_vid' : source_ind.id})
    logger.info('Retrieved %d children for source individual' % len(children))
    for child in children:
        logger.debug('Changing %s for individual %s' % (parent_type,
                                                        child.id))
        setattr(child, parent_type, target_ind)
    kb.save_array(children)

def update_action_on_ind(source_ind, target_ind, kb):
    logger = logging.getLogger()
    query = '''SELECT act FROM ActionOnIndividual act
               JOIN act.target AS ind
               WHERE ind.vid = :ind_vid
            '''
    acts = kb.find_all_by_query(query, {'ind_vid' : source_ind.id})
    logger.info('Retrieved %d actions for source individual' % len(acts))
    for sa in src_acts:
        sa.target = target
        kb.save(sa)
        logger.debug('Action %s target updated' % sa.id)

def update_enrollments(source_ind, target_ind, kb):
    logger = logging.getLogger()
    query = '''SELECT en FROM Enrollment en
               JOIN en.individual AS ind
               WHERE ind.vid = :ind_vid
            '''
    enrolls = kb.find_all_by_query(query, {'ind_vid' : source_ind.id})
    logger.info('Retrieved %d enrollments for source individual' % len(enrolls))
    for en in enrolls:
        try:
            en.individual = target_ind
            logger.debug('Changing individual for enrollment %s in study %s' % (sren.studyCode,
                                                                                sren.study.label))
            kb.save(en)
            logger.info('Changed individual for enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                              sren.studyCode,
                                                                                              sren.study.label))
        except KBError, kbe:
            logger.warning('Unable to update enrollment %s (study code %s -- study %s)' % (sren.id,
                                                                                           sren.studyCode,
                                                                                           sren.study.label))
            move_to_duplicated(en, kb)

def update_ehr_records(source_ind, target_ind, kb):
    kb.update_table_rows(kb.eadpt.EAV_EHR_TABLE, '(i_vid == "%s")' % source_ind.id,
                         {'i_vid' : target_ind.id})
                                    

# This method should be considered as a temporary hack that will be
# used untill a proper ALIAS management will be introduced into the
# system
def move_to_duplicated(enrollment, kb):
    logger = logging.getLogger()
    old_st = enrollment.study
    dupl_st = kb.get_study('%s_DUPLICATI' % old_st.label)
    if not dupl_st:
        logger.warning('No "duplicated" study ({0}_DUPLICATI) found for study {0}'.format(old_st.label))
        return
    enrollment.study = dupl_st
    try:
        kb.save(enrollment)
        logger.info('Enrollmnet %s moved from study %s to study %s' % (enrollment.studyCode,
                                                                       old_st.label, dupl_st.label))
    except:
        logger.error('An error occurred while moving enrollment %s from study %s to %s' % (enrollment.studyCode,
                                                                                           old_st.label,
                                                                                           dupl_st.label))


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.loglevel)
    kwargs = {'format' : LOG_FORMAT,
              'datefmt' : LOG_DATEFMT,
              'level' : log_level}
    if args.logfile:
        kwargs['filename'] = args.logfile
    logging.basicConfig(**kwargs)
    logger = logging.getLogger()

    kb = KB(driver='omero')(args.host, args.user, args.passwd)

    logger.debug('Retrieving Individuals')
    individuals = kb.get_objects(kb.Individual)
    logger.debug('Retrieved %d Individuals' % len(individuals))
    ind_lookup = {}
    for i in individuals:
        ind_lookup[i.id] = i

    with open(args.in_file) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        for row in reader:            
            try:
                source = ind_lookup[row['source']]
                logger.info('Selected as source individual with ID %s' % source.id)
                target = ind_lookup[row['target']]
                logger.info('Selected as destination individual with ID %s' % target.id)
            except KeyError, ke:
                logger.warning('Unable to retrieve individual with ID %s, skipping row' % ke)
                continue

            logger.info('Updating children connected to source individual')
            update_children(source, target, kb)
            logger.info('Children update complete')

            logger.info('Updating ActionOnIndividual related to source individual')
            update_action_on_ind(source, target, kb)
            logger.info('ActionOnIndividual update completed')

            logger.info('Updating enrollments related to source individual')
            update_enrollments(source, target, kb)
            logger.info('Enrollments update completed')
            
            logger.info('Updating EHR records related to source individual')
            update_ehr_records(source, target, kb)
            logger.info('EHR records update completed')
    
            try:
                kb.delete(source)
                ind_lookup.pop(source.id)
                logger.info('Individual %s deleted' % source.id)
            except KBError, kb:
                logger.error('Unable to delete individual %s' % source.id)
                logger.error(kb)

if __name__ == '__main__':
    main(sys.argv[1:])
