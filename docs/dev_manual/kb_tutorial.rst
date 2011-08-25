Knowledge Base Tutorial
=======================

The Knowledge Base (KB for short) provides a uniform access to all
available meta-information stored in the backend (e.g., Omero).

.. code-block:: python

  from bl.vl.kb import KnowledgeBase as KB
  kb = KB(driver='omero')(host='biobank05', user='root', passwd='xxx')

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
 * SnpMarker
 * ... FIXME


Creating a Study
................

The first thing one needs to create is a Study object.

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
