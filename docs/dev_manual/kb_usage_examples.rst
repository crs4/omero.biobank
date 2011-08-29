Knowledge Base Usage Examples
=============================

Retrieving data samples for consistency checks
----------------------------------------------

.. code-block:: python

  >>> from bl.vl.kb import KnowledgeBase as KB
  >>> kb = KB(driver='omero')(host='biobank05', user='root', passwd='romeo')
  >>> dss = kb.get_objects(kb.AffymetrixCel)
  >>> len(dss)
  7138
  >>> dss[0].label
  'CT_lab5673.CEL'
  >>> dss_labels = set(x.label for x in dss)
  >>> fn = '/home/simleo/hadoop_work/chipal/old_to_cels.tsv'
  >>> f = open(fn)
  >>> import csv
  >>> reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
  >>> records = [r for r in reader]
  >>> f.close()
  >>> records[0]
  {'mimetype': 'x-vl/affymetrix-cel', 'data_sample_label':
  'CT_SS_0440.CEL', 'study': 'TEST', 'sha1':
  '3c42592e0fda07fbd7d157098fde2410952a6912', 'path':
  'file:/SHARE/USERFS/bigspace/acdc/CELs/data/7FC4EAD61BAA4D10A0A704CBE390472A.CEL',
  'size': '69064847'}
  >>> dss2 = set(r['data_sample_label'] for r in records)
  >>> len(dss2)
  7051
  >>> diff = dss_labels - dss2
  >>> len(diff)
  123
  >>> diff2 = dss2 - dss_labels
  >>> len(diff2)
  36
