# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=W0105, C0103

"""
    ..

Running  Galaxy workflows
=========================

Run a workflow on a biobank object
----------------------------------

In this example we will run a simple galaxy workflow on biobank data
and save the results back in biobank.

FIXME Details on the workflow that will be used should go here.


Initialization
..............

"""

import sys, os
from bl.vl.kb.galaxy import GalaxyInstance
from bl.vl.kb import KnowledgeBase
import uuid
import time

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')
CHECK_OME_VERSION = False

GLX_API_KEY = 'a53cff7189d8bcacfdf3871d5cc0f3bb'
GLX_URL = 'http://localhost:8080'

kb = KnowledgeBase(driver='omero')(OME_HOST, OME_USER, OME_PASSWD,
                                   check_ome_version=CHECK_OME_VERSION)
gi = GalaxyInstance(kb, GLX_URL, GLX_API_KEY)

"""
    ..


Select workflow
...............

First of all we check if we have the galaxy workflow we need and the
related omero.biobank device. We then create a study object to define
the context of our interaction with omero.biobank.

"""

WORKFLOW_NAME='Bacterial_assembly_paired_end'

workflow = gi.get_workflow_by_name(name=WORKFLOW_NAME)
device = kb.get_device(WORKFLOW_NAME)
if device is None:
    device = kb.create_device(WORKFLOW_NAME,
                              'CRS4', 'Bacterial_assembly_paired_end', '001')

"""
    ..

Creating an input object in biobank
...................................

This specific workflow needs as an input the following contents:

  * 'contigs': SeqDataSample with the sequences of the contigs,

  * 'reads': read sequences

  * 'mates': mate sequences

with corresponding reads and mates paired.

FIXME It is actually more general, but for the time being we leave as it is.

"""

d = '/home/omero/metagenomics/anterior_nares/SRS015450/'
input_paths  = dict([
    ('read1',   (d + 'SRS015450.1.fastq', 'x-vl/fastq')),
    ('read2',   (d + 'SRS015450.2.fastq', 'x-vl/fastq'))
    ])
STUDY_LABEL = 'Metagenomics-%s' % uuid.uuid1().hex

def create_study():
    conf = {'label': STUDY_LABEL,
            'description': 'this is a test'}
    study = kb.factory.create(kb.Study, conf)
    to_be_killed.append(study.save())
    return study

def create_flowcell_and_samples(study):
    action = kb.create_an_action(study)
    to_be_killed.append(action.save())
    tubes = []
    for x in xrange(2):
        tube_label = 'a-test-tube-%s' % uuid.uuid1().hex
        conf = {'label': tube_label,
                'action': action,
                'currentVolume': 1.0,
                'initialVolume': 1.0,
                'content': kb.VesselContent.DNA,
                'status': kb.VesselStatus.CONTENTUSABLE}
        tube = kb.factory.create(kb.Tube, conf)
        print ' Created tube %s' % tube.label
        tubes.append(tube.save())
        to_be_killed.append(tube)
    flowcell_label = 'a-test-flowcell-%s' % uuid.uuid1().hex
    conf = {'action': action,
            'numberOfSlots': 8,
            'status': kb.ContainerStatus.READY,
            'label': flowcell_label}
    flowcell = kb.factory.create(kb.FlowCell, conf)
    print ' Created flowcell %s' % flowcell.label
    to_be_killed.append(flowcell.save())
    for x in xrange(1):
        lane_label = 'a-test-lane-%s' % uuid.uuid1().hex
        conf = {'action': action,
                'label': lane_label,
                'flowCell': flowcell,
                'slot': x+1,
                'status': kb.ContainerStatus.READY}
        lane = kb.factory.create(kb.Lane, conf)
        print ' Created lane %s' % lane.label
        to_be_killed.append(lane.save())
        for y in xrange(2):
            lsaction = kb.create_an_action(study, tubes[x+y])
            to_be_killed.append(lsaction.save())
            conf = {'action': lsaction,
                    'lane': lane,
                    'tag': uuid.uuid1().hex, # I only need a unique string
                    'content': kb.VesselContent.DNA}
            laneslot = kb.factory.create(kb.LaneSlot, conf)
            print ' Created laneslot for lane %s' % lane.label
            to_be_killed.append(laneslot.save())
    return flowcell

def create_input_object(flowcell, study):
    action = kb.create_an_action(study, flowcell)
    to_be_killed.append(action.save())
    seq_out_label = 'a-test-sequencer-output-%s' % uuid.uuid1().hex
    conf = {'label': seq_out_label,
            'action': action,
            'status': kb.DataSampleStatus.USABLE}
    sequencer_output = kb.factory.create(kb.SequencerOutput, conf)
    print ' Created sequencer output %s' % sequencer_output.label
    to_be_killed.append(sequencer_output.save())
    sequencer_output.unload()
    sequencer_output.reload()
    action = kb.create_an_action(study)
    to_be_killed.append(action.save())
    dc_label = 'a-test-data-collection-%s' % uuid.uuid1().hex
    conf = {'label': dc_label, 'action': action}
    data_collection = kb.factory.create(kb.DataCollection, conf)
    print " Created collection %s" % dc_label
    to_be_killed.append(data_collection.save())
    action = kb.create_an_action(study, sequencer_output)
    to_be_killed.append(action.save())
    for name, desc  in input_paths.iteritems():
        conf = {'label': '%s.%s' % (dc_label, name),
                'status': kb.DataSampleStatus.USABLE,
                'action': action}
        data_sample = kb.factory.create(kb.DataSample, conf)
        print " Created datasample %s" % data_sample.label
        to_be_killed.append(data_sample.save())
        data_sample.unload()
        data_sample.reload()
        conf = {'dataSample': data_sample,
                'dataCollection': data_collection,
                'role': name}
        dci = kb.factory.create(kb.TaggedDataCollectionItem, conf)
        to_be_killed.append(dci.save())
        conf = {'sample': data_sample,
                'path': desc[0],
                'mimetype': desc[1],
                'sha1': 'fake-sha1',
                'size': 10} # fake size
        data_object = kb.factory.create(kb.DataObject, conf)
        to_be_killed.append(data_object.save())
    return data_collection

def cleanup():
    while to_be_killed:
        kb.delete(to_be_killed.pop())

to_be_killed=[]
try:
    study = create_study()
    flowcell = create_flowcell_and_samples(study)
    input_data = create_input_object(flowcell, study)
except StandardError as e:
    print 'intercepted an exception, %s, cleaning up.' % e.message
    cleanup()
    raise e

trigger = raw_input("Press Enter to continue...")

# """
#    ..

# Run the actual workflow
# .......................

# Now that we have an input we can run the workflow.

# """
print "Workflow %s is running " % workflow.name
print "The reference db is %s" % '16SMicrobial-20130611'
history = gi.run_workflow(study, workflow, input_data)

# """
#    ..

# Save results
# ............

# Now that we have a history we can save it in the biobank.

# """
print "saving history in the biobank"
gi.save(history)

trigger = raw_input("Press Enter to continue...")

# """
#    ..

# List known workflows run
# ------------------------

# Find all the workflows known to galaxy that are related to
# device. That is, were obtained obtained by changing a
# parameter with respect to the reference workflow which is represented
# by device.

# Collect info on related histories and what has been saved in biobank.

# """

def find_workflow(device, gi):
    print "\n *** "
    ws = gi.get_workflows(device)
    for w in ws:
        print '\nWorkflow %s' % w.name
        hs = gi.get_histories(w)
        print '%d histories on this workflow' % len(hs)
        h_by_obj = {}
        for h in hs:
            if gi.is_biobank_compatible(h):
                label = gi.get_input_object(h).label
                #print 'label:%r, type:%r'% (label, type(label))
                h_by_obj.setdefault(label, []).append(h)
        for label, hlist in h_by_obj.iteritems():
            #print '\t%s' % label
            for h in hlist:
                in_biobank = gi.is_mapped_in_biobank(h)
                print ('%s:\n{status: %s, in_biobank: %s}'
                       % (h.name, h.state, in_biobank))
    print " *** "

find_workflow(device,gi)
trigger = raw_input("Press Enter to continue...")

# """
#    ..

# Run a parameter search
# ----------------------

# Simple variant, no checks on what has already been done.

# """
workflow = gi.get_workflows(device)[0]
input_object = gi.get_input_object(gi.get_histories(workflow)[0])
study = input_object.action.context

#ref_dbs = ('16SMicrobial-20130511', '16SMicrobial-20130611',
 #          '16SMicrobial-20130711')
ref_db = '16SMicrobial-20130711'
#for db in ref_dbs:
w = workflow.clone()
x = w.steps[2].tool['genomeSource']
x['indices'] = ref_db
w.steps[2].tool['genomeSource'] = x
print "Saved a clone of Workflow %s with modified parameters " % workflow.name
w = gi.register(w)
print "Running workflow %s " % w.name
print "The reference db is %s" % ref_db
h = gi.run_workflow(study, w, input_object, wait=True)
gi.save(h)
print "saving history in the biobank"

find_workflow(device,gi)
trigger = raw_input("Press Enter to continue...")

# """
#    ..

# Rerun an analysis with different parameters
# -------------------------------------------

# """

device = kb.get_device(WORKFLOW_NAME)
study = kb.get_study(STUDY_LABEL)
print "Quering Omero to obtain a previous history from this study %s" % study.label
query = """
select action from ActionOnCollection action
where action.device.id = :dev_id and action.context.id = :std_id
"""

actions = kb.find_all_by_query(
    query, {'dev_id': device.omero_id, 'std_id': study.omero_id}
    )
a = actions[0]
a.reload()
h = gi.get_history(action=a)
print "Retrieved the history %s " % h.name
study, workflow, input_object = gi.get_initial_conditions(h)
w2 = workflow.clone()
x = w2.steps[2].tool['genomeSource']
x['indices'] = "16SMicrobial-20130911"
w2.steps[2].tool['genomeSource'] = x
w2 = gi.register(w2)
print "Running workflow %s " % w2.name
print "The reference db is %s" % "16SMicrobial-20130911"
h2 = gi.run_workflow(study, w2, input_object)
gi.save(h2)
find_workflow(device,gi)
trigger = raw_input("Press Enter to continue...")
