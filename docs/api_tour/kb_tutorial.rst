Knowledge Base Tutorial
=======================

The Knowledge Base (KB for short) provides a uniform access to all
available meta-information stored in the backend (e.g., Omero).

.. code-block:: python

  from bl.vl.kb import KnowledgeBase as KB
  kb = KB(driver='omero')(host='localhost', user='root', passwd='xxx')

For instance, in the code snippet above, we have established a
connection to an Omero metadata bank.


Objects that the KB can handle
------------------------------

Currently, the KB handles the following object types:

 * Study
 * Individual
 * Demographic information about individuals
 * Vessel (e.g., Tube, PlateWell, ...) -- a generic object that contains fluid
 * Collection of objects (e.g., TiterPlate is a collection of PlateWell(s))
 * DataSample
 * DataObject
 * Marker
 * ... FIXME


Basic Object operations
-----------------------

The simplest possible object is a Study object.

.. code-block:: python

  >>> conf = {'label': 'foo'}
  >>> s = kb.factory.create(kb.Study, conf)
  >>> s.is_mapped()
  False

We first created a configuration dictionary, then used the KB's
factory to create a study object.  As shown in the above snippet, this
object is not "mapped" to an actual object in the database (i.e., it's
not stored yet).  Here is how you save it:

.. code-block:: python

  >>> s.save()
  >>> s.is_mapped()
  True

And here is how you get a previously saved study:

.. code-block:: python

  >>> x = kb.get_study('foo')
  >>> x == s
  True
  >>> y = kb.get_study('bar')
  >>> y is None
  True

What has been done above is a general pattern for object creation:

 #. Create a dictionary with values for, at least, the required fields;

 #. Use the create method of the kb.factory object providing as
    arguments the object type and the configuration dictionary;

 #. Use the save method of the object to register it with the database.


What happens if you try to register the same object twice:

.. code-block:: python

  >>> conf = {'label': 'foo'}
  >>> s = kb.factory.create(kb.Study, conf)
  >>> s.save()
  ERROR:proxy_core:omero.ValidationException: could not insert:
  [ome.model.vl.Study]; SQL [insert into study (description,
  creation_id, external_id, group_id, owner_id, permissions,
  update_id, endDate, label, startDate, version, vid, id) values (?,
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)]; constraint [study_label_key];
  nested exception is
  org.hibernate.exception.ConstraintViolationException: could not
  insert: [ome.model.vl.Study] ...

How to get the available classes:

The clean way is to look up the model definitions. A quick trick to
get them by introspection is:

.. code-block:: python

  >>> [x for x in dir(kb) if hasattr(getattr(kb, x), "is_mapped")]

How to get the available fields for each class:

Again, the clean way would be to look up the model definitions. As a
quick trick, you can do:

.. code-block:: python

  >>> kb.Study.__fields__
  {'description': ('string', 'optional'),
   'endDate': ('timestamp', 'optional'),
   'label': ('string', 'required'),
   'startDate': ('timestamp', 'required'),
   'vid': ('vid', 'required')}

NOTE: this should be used as a reminder, since this is a low-level
view of the available fields, and they are not all supposed to be
user-settable (e.g., startDate and vid are automatically generated).

How to delete an object:

.. code-block:: python

  >>> kb.delete(s)
  >>> s = kb.get_study('foo')
  >>> s is None
  True


Using the KB
------------

Import an Individual:

.. code-block:: python

  >>> conf = {'label': 'foo'}
  >>> s = kb.factory.create(kb.Study, conf)
  >>> s.save()
  <bl.vl.kb.drivers.omero.action.Study object at 0xb732c92c>
  >>> conf = {'operator': 'pippo', 'context': s, 'actionCategory': kb.ActionCategory.IMPORT}
  >>> a = kb.factory.create(kb.Action, conf)
  >>> conf = {'action': a, 'gender': kb.Gender.MALE}
  >>> i = kb.factory.create(kb.Individual, conf)
  >>> i.save()
  <bl.vl.kb.drivers.omero.individual.Individual object at 0x90fef2c>

Note that an Individual (in general, any object that has a counterpart
in the real world) needs an action to be created.

Enroll an individual into a study:

.. code-block:: python

  >>> conf = {'study': s, 'individual': i, 'studyCode': 'I001'}
  >>> e = kb.factory.create(kb.Enrollment, conf)
  >>> e.save()

To check which individuals are enrolled in a specific study:

.. code-block:: python

  >>> v = kb.get_enrolled(s)
  >>> v
  [<bl.vl.kb.drivers.omero.individual.Enrollment object at 0x911018c>]
  >>> v[0].individual == i
  True
  >>> v[0].study == s
  True
  >>> v[0].studyCode 
  'I001'

.. todo::

   missing everything beyond Individual
