A collection of examples
========================

This collection of examples is supposed to show the following:

 * how to develop scripts/modules that use omero/vl in a production context
 * how to extend omero/vl

In other words, what it is shown here is how one is supposed to do
things like loading in one go 20,000 individuals and related
information.

More specifically, the first example will show how one generates
importable data starting from a collection of datasets coming from a given
acquisition technology (TaqMan). The second ...

.. todo::

   only one example.

Processing a collection of TaqMan results
-----------------------------------------

.. lit-prog:: ../examples/import_taqman_results.py




quick repairs
-------------

.. todo::

 this is probably not the best place for this.... Missing pieces too.


.. code-block:: ipython

   In [8]: for d in dss:
             if d.label.find(' ') > -1:
                print '%s has an hole' % d.label
		d.label = d.label.replace(' '. '_')
		d.save()

   7051_MS CA_673-2.CEL has an hole
   7022_MS CA_1851-3.CEL has an hole
   7026_MS CA_2626-1.CEL has an hole
   7053_MS CA_673-3.CEL has an hole
