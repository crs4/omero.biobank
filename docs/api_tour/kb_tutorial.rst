Knowledge Base tutorial
=======================

The Knowledge Base (KB) provides uniform access to all available
meta-information stored in the Biobank backend (only OMERO is
supported at this time).

.. code-block:: python

  >>> from bl.vl.kb import KnowledgeBase as KB
  >>> kb = KB(driver='omero')('localhost', 'test', 'test')


Objects that the KB can handle
------------------------------

Currently, the main object types handled by the KB are:

 * Study
 * Individual
 * Demographic information about individuals
 * Vessel (e.g., Tube, PlateWell, ...) -- a generic object that contains fluid
 * Collection of objects (e.g., TiterPlate, a collection of PlateWell(s))
 * DataSample
 * DataObject
 * Marker


Basic operations
----------------

The simplest possible object is a Study object.

.. code-block:: python

  >>> conf = {'label': 'foo'}
  >>> s = kb.factory.create(kb.Study, conf)
  >>> s.is_mapped()
  False

We first created a configuration dictionary, then used the KB's
factory to create a Study object. As shown in the above snippet, this
object is not yet **mapped** to an actual object in the
backend. Mapping is a result of actively saving an object:

.. code-block:: python

  >>> s.save()
  <bl.vl.kb.drivers.omero.action.Study object at 0xb73836cc>
  >>> s.is_mapped()
  True

The following snippet shows how to retrieve a previously saved study:

.. code-block:: python

  >>> x = kb.get_study('foo')
  >>> x == s
  True
  >>> y = kb.get_study('bar')
  >>> y is None
  True

The one shown above is a general pattern for object creation:

 #. Create a configuration dictionary with values for, at least, the
    required fields;

 #. Use the ``create`` method of the kb.factory object providing as
    arguments the object type and the configuration dictionary;

 #. Use the ``save`` method of the object to save it to the backend.

The clean way to get the available classes is to look up the model
definitions. A quick trick to get them by introspection is:

.. code-block:: python

  >>> [c for c in dir(kb) if hasattr(getattr(kb, c), "is_mapped")]
  ['Action', 'ActionCategory', 'ActionOnAction', ...]

To get the available fields for each class without looking at the
model definitions, you can do the following:

.. code-block:: python

  >>> kb.Study.__fields__
  {'description': ('string', 'optional'),
   'endDate': ('timestamp', 'optional'),
   'label': ('string', 'required'),
   'startDate': ('timestamp', 'required'),
   'vid': ('vid', 'required')}

NOTE: this is a low-level view of the available fields: not all of
them are user-settable (e.g., ``startDate`` and ``vid`` are
automatically generated).

To delete an object, do the following:

.. code-block:: python

  >>> kb.delete(x)

To check that it's actually been deleted:

  >>> x = kb.get_study('foo')
  >>> x is None
  True


Working with KB objects
-----------------------

The following code snippet shows how to create and manipulate basic KB
objects; specifically, it shows how to import an individual and enroll
it in a study.  More details on individual and enrollments can be
found in the :ref:`examples <kb_examples>`.

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
  >>> conf = {'study': s, 'individual': i, 'studyCode': 'I001'}
  >>> e = kb.factory.create(kb.Enrollment, conf)
  >>> e.save()
  <bl.vl.kb.drivers.omero.individual.Enrollment object at 0x91df30c>
  >>> v = kb.get_enrolled(s)
  >>> v
  [<bl.vl.kb.drivers.omero.individual.Enrollment object at 0x911018c>]
  >>> v[0].individual == i
  True
  >>> v[0].study == s
  True
  >>> v[0].studyCode
  'I001'
