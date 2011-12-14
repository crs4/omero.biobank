import csv, os, sys, optparse

from bl.vl.kb import KnowledgeBase as KB

def make_parser():
    parser = optparse.OptionParser(usage='%prog [OPTIONS] OUT_FILE')
    parser.add_option('--host', type='str', metavar='STRING',
                      help='omero host [%default]',
                      default='localhost')
    parser.add_option('--user', type='str', metavar='STRING',
                      help='omero user [%default]',
                      default='root')
    parser.add_option('--passwd', type='str', metavar='STRING',
                      help='omero password [%defualt]',
                      default='omero')
    return parser

def main(argv):
    parser = make_parser()
    opt, args = parser.parse_args()

    try:
        out_file_path = args[0]
    except IndexError:
        print 'Mandatory field missing.'
        print parser.usage()
        sys.exit(2)

    # Create the KnowledgeBase object
    kb = KB(driver='omero')(opt.host, opt.user, opt.passwd)

    # Retrieve all studies from omero
    studies = kb.get_objects(kb.Study)
    print 'Retrieved %d studies from database' % len(studies)

    csv_header = ['individual_uuid']
    enrolls_map = {}
    # For each study, retrieve all enrollments
    for s in studies:
        print 'Retrieving enrollments for study %s' % s.label
        enrolls = kb.get_enrolled(s)
        print '%s enrollments retrieved' % len(enrolls)
        if len(enrolls) > 0:
            print 'Building lookup dictionary....'
            csv_header.append(s.label) # Add study label to CSV header
            for e in enrolls:
                enrolls_map.setdefault(e.individual.omero_id, {})['individual_uuid'] = e.individual.id
                enrolls_map[e.individual.omero_id][s.label] = e.studyCode
        else:
            print 'No enrollments found, skip study %s' % s.label
            
    # Write to CSV file
    print 'Writing CSV file %s' % out_file_path
    with open(out_file_path, 'w') as f:
        writer = csv.DictWriter(f, csv_header, delimiter='\t', quotechar='"')
        writer.writeheader()
        for k, v in enrolls_map.iteritems():
            writer.writerow(v)

if __name__ == '__main__':
    main(sys.argv)
