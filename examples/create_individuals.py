# BEGIN_COPYRIGHT
# END_COPYRIGHT

# pylint: disable=W0105, C0103

""" ..

This example shows how to define individuals and enroll them in a
study.  Open a connection to OMERO through the KB:

"""

import os
from bl.vl.kb import KnowledgeBase as KB

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)

""" ..

Define the list of individuals:

"""

individual_defs = [
    #label   gender    father  mother
    ('I001', 'MALE',    None,   None),
    ('I002', 'FEMALE',  None,   None),
    ('I003', 'MALE',   'I001', 'I002'),
    ('I004', 'FEMALE', 'I001', 'I002'),
    ('I005', 'MALE',   'I003', 'I004'),
    ('I006', 'MALE',   'I003', 'I004'),
    ]

""" ..

Create a Study instance: in this context, this is used as a way of
handling groups of individuals.  In the following we will assume that
the study's label has not been assigned to any other existing study.

"""

config = {'label': 'KB_EXAMPLES', 'description': 'dummy'}
study = kb.factory.create(kb.Study, config).save()

""" ..

To proceed we need to instantiate an Action object.  In OMERO.biobank,
actions are used to store information on the chains of events that
lead to the creation of KB objects.  In their full-blown
configuration, actions may contain very detailed descriptions of what
happened; in this example, we will use a shortcut that creates a "vanilla"
action with minimal information:

"""

action = kb.create_an_action(study)

""" ..

Now we can create Individual objects and enroll them in the study:

"""

gender_map = {'MALE': kb.Gender.MALE, 'FEMALE': kb.Gender.FEMALE}
by_label = {}
for label, gender, father, mother in individual_defs:
    config = {'gender': gender_map[gender], 'action': action}
    if father:
        config['father'] = by_label[father]
    if mother:
        config['mother'] = by_label[mother]
    i = kb.factory.create(kb.Individual, config).save()
    by_label[label] = i
    config = {'study': study, 'individual': i, 'studyCode': label}
    e = kb.factory.create(kb.Enrollment, config).save()

""" ..

Note that studyCode is the code assigned to each individual in a
specific study.  We can run a consistency check as follows:

"""

for e in kb.get_enrolled(study):
    assert e.individual == by_label[e.studyCode]
