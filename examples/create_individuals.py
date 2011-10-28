""" ..

The goal of this examples is to show how one can define individuals
and enroll them in a study, that we will call 'TEST01'.

**Note:** DO NOT run this example against a production
  database. Also, what will be shown here is supposed to be
  didactically clear, not efficient. See the implementation of the
  importer tools for more efficient solutions.

First, as usual, we open the connection to the KnowledgeBase.
"""

from bl.vl.kb import KnowledgeBase as KB
import numpy as np

import os

OME_HOST   = os.getenv('OME_HOST', 'localhost')
OME_USER   = os.getenv('OME_USER', 'test')
OME_PASSWD = os.getenv('OME_PASSWD', 'test')

kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASSWD)


""" ..

The following is the list of the individuals that we would like to
load.

"""

individuals_defs = [
  # label  gender    father   mother
  ('I001', 'MALE',   None,    None),
  ('I002', 'FEMALE', None,    None),
  ('I003', 'MALE',   'I001', 'I002'),
  ('I004', 'FEMALE', 'I001', 'I002'),
  ('I005', 'MALE',   'I003', 'I004'),
  ('I006', 'MALE',   'I003', 'I004'),
  ]

""" ..

we first create a Study instance, which, we will see below, is used in this
context as an handle on groups of individuals; we then need an Action
object. In general. Action(s) instances are used to mantain
information on the process that produced objects, such as Individual
instances, that have a counterpart in the physical world. in their
full blown configuration, they contain very detailed descriptions of
what happened. In this simplistic example, we are using a convenience
method provided by KB to give us a vanilla action object with default,
minimal, information.

Of course, we are assuming that this is the first time that someone is
creating a study with that label: do not blindly cut&paste this code
to production artifacts.

.. todo::

  we should add links to reference material on the objects defined
  (e.g., Action)

"""
study = kb.factory.create(kb.Study, {'label'       : 'TEST01',
                                     'description' : 'some desc.'}).save()
action = kb.create_an_action(study)

""" ..

We can now proceed with the actual Individual definition and their
enrollment in the study.

"""

gender_map = {'MALE' : kb.Gender.MALE, 'FEMALE' : kb.Gender.FEMALE}
by_label = {}
for label, gender, father, mother in individuals_defs:
  conf = {'gender' : gender_map[gender], 'action' : action}
  if father:
    conf['father'] = by_label[father]
  if mother:
    conf['mother'] = by_label[mother]
  i = kb.factory.create(kb.Individual, conf).save()
  by_label[label] = i
  e = kb.factory.create(kb.Enrollment, {'study' : study,
                                        'individual' : i,
                                        'studyCode'  : label}).save()

""" ..

As a test, we now loop on all the individual enrolled in the study and
check if they are who we think they should be.
"""

for e in kb.get_enrolled(study):
  assert e.individual == by_label[e.studyCode]

