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
from bl.vl.kb.galaxy import GalaxyInstance
from bl.vl.kb import KnowledgeBase
import uuid

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')
CHECK_OME_VERSION = False

GLX_API_KEY = 'cc23224aabcc44132c25b4e23f03580f'
GLX_URL = 'http://localhost:8070'

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
WORKFLOW_NAME='WKF-SSPACE-SCAFFOLDING-002'

workflow = gi.get_workflow_by_name(name=WORKFLOW_NAME)
device = kb.get_device(WORKFLOW_NAME)
if device is None:
    device = kb.create_device(WORKFLOW_NAME,
                              'CRS4', 'WKF-SSPACE-SCAFFOLDING', '002')

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

d = '/home/zag/work/vs/hg/galaxy-dist/sspace/SSPACE-BASIC-2.0_linux-x86_64/example/'
input_paths  = dict([
    ('contigs', (d + 'contigs_abyss.fasta', 'x-vl/fasta')),
    ('reads',   (d + 'SRR001665_red_1.fastq', 'x-vl/fastq')),
    ('mates',   (d + 'SRR001665_red_2.fastq', 'x-vl/fastq'))
    ])

STUDY_LABEL = 'a-test-study-%s' % uuid.uuid1().hex

def create_input_object():
    conf = {'label': STUDY_LABEL,
            'description': 'this is a test.'}
    study = kb.factory.create(kb.Study, conf)
    to_be_killed.append(study.save())
    action = kb.create_an_action(study)
    to_be_killed.append(action.save())
    dc_label = 'a-test-data-collection-%s' % uuid.uuid1().hex
    conf = {'label': dc_label, 'action': action}
    data_collection = kb.factory.create(kb.DataCollection, conf)
    to_be_killed.append(data_collection.save())
    for name, desc  in input_paths.iteritems():
        conf = {'label': '%s.%s' % (dc_label, name),
                'status': kb.DataSampleStatus.USABLE,
                'action': action}
        data_sample = kb.factory.create(kb.DataSample, conf)
        to_be_killed.append(data_sample.save())
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

to_be_killed=[]
try:
    input_data = create_input_object()
except StandardError as e:
    print 'intercepted an exception, %s, cleaning up.' % e
    while to_be_killed:
        kb.delete(to_be_killed.pop())
    raise e

"""
   ..

Run the actual workflow
.......................

Now that we have an input we can run the workflow.

"""

history = gi.run_workflow(study, workflow, input_data)

"""
   ..

Save results
............

Now that we have a history we can save it in the biobank.

"""

gi.save(history)

"""
   ..

List known workflows run
------------------------

Find all the workflows known to galaxy that are related to
device. That is, were obtained obtained by changing a
parameter with respect to the reference workflow which is represented
by device.

Collect info on related histories and what has been saved in biobank.

"""
ws = gi.get_workflows(device)
for w in ws:
    print 'Workflow %s' % w.name
    hs = gi.get_histories(w)
    print '\t%d histories on this workflow' % len(hs)
    h_by_obj = {}
    for h in hs:
        if gi.is_biobank_compatible(h):
            label = gi.get_input_object(h).label
            print 'label:%r, type:%r'% (label, type(label))
            h_by_obj.setdefault(label, []).append(h)
    for label, hlist in h_by_obj.iteritems():
        print '\t%s' % label
        for h in hlist:
            in_biobank = gi.is_mapped_in_biobank(h)
            print ('\t\t%s: {status: %s, in_biobank: %s}'
                   % (h.name, h.state, in_biobank))

"""
   ..

Run a parameter search
----------------------

Simple variant, no checks on what has already been done.

"""
workflow = gi.get_workflows(device)[0]
input_object = gi.get_input_object(gi.get_histories(workflow)[0])
study = input_object.action.context

for i in range(1, 4):
    w = workflow.clone()
    w.steps[3].tool['error'] = str(0.25*i)
    w = gi.register(w)
    h = gi.run_workflow(study, w, input_object, wait=True)
    gi.save(h)

"""
   ..

Rerun an analysis with different parameters
-------------------------------------------

"""

device = kb.get_device(WORKFLOW_NAME)
study = kb.get_study(STUDY_LABEL)
query = """
select action from Action action
where action.device.id = :dev_id and action.context.id = :std_id
"""
actions = kb.find_all_by_query(
    query, {'dev_id': device.omero_id, 'std_id': study.omero_id}
    )
h = gi.get_history(action=actions[0])
study, workflow, input_object = gi.get_initial_conditions(h)
w2 = workflow.clone()
w2.steps[1].tool['cols'] = "c2"  # "Change Case" tool
w2 = gi.register(w2)
h2 = gi.run_workflow(study, w2, input_object)
