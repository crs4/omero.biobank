"""
Import of Data samples
======================

Will read in a tsv file with the following columns::

  study  label source device device_type scanner options             status
  ASTUDY foo01 v03909 v9309  Chip        v99020  celID=0009099090    USABLE
  ASTUDY foo02 v03909 v99022 Scanner     v99022  conf1=...,conf2=... UNKNOWN
  ....

and instantiate specialized DataSample derived classes that correspond
to the specific lines.

In this example, the first line corresponds to a dataset obtained by
using chip v9309 on scanner v99020, while the second datasample has
been obtained using a technology directly using a scanner, e.g., an
Illumina HiSeq 2000. The '''scanner''' column is there as a
convenience to support a more detailed description of a chip based
acquisition.

The general strategy is to decide what data objects should be
instantiated by looking at the chip column and to its corresponding
maker,model,release.

The optional column '''scanner''', the vid of the scanner device, is
used in cases, such as affymetrix genotyping where it is relevant.

The optional column '''data_sample_type''' over-rides all of the
automatic decisions that could be taken by the importer.

It is also possible to import DataSample(s) that are the results of
processing on other DataSample(s). This is an example::

  study  label source device device_type     options
  ASTUDY foo01 v03909 v99021 SoftwareProgram conf1=...,conf2=...
  ASTUDY foo02 v03909 v99021 SoftwareProgram conf1=...,conf2=...
  ....

A special case are GenotypeDataSample where it is mandatory to assign
a SNPMarkerSet using the column  'markers_set' with the vid of
the relevant SNPMarkersSet. As an example::

  study  label source device device_type     data_sample_type   markers_set status
  ASTUDY foo01 v03909 v99021 SoftwareProgram GenotypeDataSample V20202      USABLE
  ASTUDY foo02 v03909 v99021 SoftwareProgram GenotypeDataSample V20202      USABLE
  ....


Usage
-----

.. code-block:: bash

  bash> cat data_sample.tsv
  study label sample_label  device_label  options
  BSTUDY  foobar-00 P001:A01  chip001 celID=829898,scanner=pula01
  BSTUDY  foobar-01 P001:A02  chip002 celID=320093,scanner=pula01
  BSTUDY  foobar-02 P002:A03  chip003 celID=320094,scanner=pula01
  BSTUDY  foobar-03 P003:E04  chip004 celID=320095,scanner=pula01
  BSTUDY  foobar-04 P004:A05  chip005 celID=320096,scanner=pula01
  BSTUDY  foobar-05 P004:B06  chip006 celID=320097,scanner=pula01
  bash> ${KB_QUERY} -o data_sample_mapped_1.tsv \
               map_vid -i data_sample.tsv \
                   --column sample_label \
                   --source-type PlateWell \
                   --study BSTUDY

  bash> ${KB_QUERY} -o data_sample_mapped_2.tsv \
               map_vid -i data_sample_mapped_1.tsv \
                   --column device_label,device \
                   --source-type Chip \
                   --study BSTUDY

  bash> SCANNER=`grep pula01 devices_mapping.tsv | perl -ane "print @F[3];"`
  bash> ${IMPORTER} -i data_sample_mapped_2.tsv -o data_sample_mapping.tsv \
               data_sample \
               --study BSTUDY --source-type PlateWell \
               --device-type Chip --scanner ${SCANNER}

  bash> cat data_dample_mapping.tsv
  study label type  vid
  BSTUDY  foobar-00 AffymetrixCel V078132A1404484D4C90DFC509495FD5C6
  BSTUDY  foobar-01 AffymetrixCel V07FD6BF58EE0E4823B63E34D81776A706
  BSTUDY  foobar-02 AffymetrixCel V087ED477FF57344C985694A622F18CD7A
  BSTUDY  foobar-03 AffymetrixCel V0052FD03AAB0C4B50BE79AE97486BEA9C
  BSTUDY  foobar-04 AffymetrixCel V0CE60F590239D4072B95D15201DDB40F2
  BSTUDY  foobar-05 AffymetrixCel V0A7EA20CF3A0D4DC392062BA4DE4AEAE4
"""

from core import Core
import csv, json, time
import itertools as it


def conf_affymetrix_cel_6(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'arrayType' : kb.AffymetrixCelArrayType.GENOMEWIDESNP_6,
          }
  if 'celID' in options:
    conf['celID'] = options['celID']
  return kb.factory.create(kb.AffymetrixCel, conf)


def conf_illumina_beadchip_1m_duo(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'assayType' : kb.IlluminaBeadChipAssayType.HUMAN1M_DUO
          }
  return kb.factory.create(kb.IlluminaBeadChipAssay, conf)


def conf_illumina_beadchip_immuno(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'assayType' : kb.IlluminaBeadChipAssayType.IMMUNOCHIP
          }
  return kb.factory.create(kb.IlluminaBeadChip, conf)


def conf_crs4_genotyper_by_device(kb, r, a, device, options, status_map):
  device.reload()
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'snpMarkersSet' : device.snpMarkersSet,
          }
  return kb.factory.create(kb.GenotypeDataSample, conf)


def conf_crs4_genotyper_by_markers_set(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'snpMarkersSet' : r['markers_set'],
          }
  return kb.factory.create(kb.GenotypeDataSample, conf)


def get_status_map(kb):
  return {'UNKNOWN'   : kb.DataSampleStatus.UNKNOWN,
          'DESTROYED' : kb.DataSampleStatus.DESTROYED,
          'CORRUPTED' : kb.DataSampleStatus.CORRUPTED,
          'USABLE'    : kb.DataSampleStatus.USABLE}


data_sample_configurator = {
  ('Affymetrix', 'Genome-Wide Human SNP Array', '6.0') : conf_affymetrix_cel_6,
  ('Illumina', 'BeadChip', 'HUMAN1M_DUO') : conf_illumina_beadchip_1m_duo,
  ('Illumina', 'BeadChip', 'IMMUNOCHIP') : conf_illumina_beadchip_1m_duo,
  ('CRS4', 'Genotyper', 'by_device') : conf_crs4_genotyper_by_device,
  ('CRS4', 'Genotyper', 'by_markers_set') : conf_crs4_genotyper_by_markers_set,
  }


class Recorder(Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf
    self.operator = operator
    self.preloaded_devices  = {}
    self.preloaded_scanners = {}
    self.preloaded_sources  = {}
    self.preloaded_data_samples = {}
    self.preloaded_markers_sets = {}

  def record(self, records, otsv):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if len(records) == 0:
      self.logger.warn('no records')
      return
    study = self.find_study(records)
    self.source_klass = self.find_source_klass(records)
    self.device_klass = self.find_device_klass(records)
    self.preload_scanners()
    self.preload_devices()
    self.preload_sources()
    self.preload_markers_sets()
    self.preload_data_samples()
    records = self.do_consistency_checks(records)
    if not records:
      self.logger.warn('no records')
      return
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study)
      self.logger.info('done processing chunk %d' % i)

  def find_source_klass(self, records):
    return self.find_klass('source_type', records)

  def find_device_klass(self, records):
    return self.find_klass('device_type', records)

  def preload_devices(self):
    self.preload_by_type('devices', self.device_klass, self.preloaded_devices)

  def preload_scanners(self):
    self.preload_by_type('scanners', self.kb.Scanner, self.preloaded_scanners)

  def preload_markers_sets(self):
    self.preload_by_type('markers_sets', self.kb.SNPMarkersSet,
                         self.preloaded_markers_sets)

  def preload_sources(self):
    self.preload_by_type('sources', self.source_klass, self.preloaded_sources)

  def preload_data_samples(self):
    self.logger.info('start preloading data_samples')
    objs = self.kb.get_objects(self.kb.DataSample)
    for o in objs:
      assert not o.label in self.preloaded_data_samples
      self.preloaded_data_samples[o.label] = o
    self.logger.info('done preloading data_samples')

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    k_map = {}
    good_records = []
    mandatory_fields = ['label', 'source', 'device', 'status']
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
      if self.missing_fields(mandatory_fields, r):
        f = reject + 'missing mandatory field.'
        self.logger.error(f)
        continue
      if r['status'] not in ['UNKNOWN', 'DESTROYED', 'CORRUPTED', 'USABLE']:
        f = reject + 'unknown status value.'
        self.logger.error(f)
        continue
      if r['label'] in self.preloaded_data_samples:
        f = reject + 'there is a pre-existing DataSample with label %s.'
        self.logger.warn(f % r['label'])
        continue
      if r['label'] in k_map:
        f = (reject +
             'there is a pre-existing record with label %s.(in this batch).')
        self.logger.error(f % r['label'])
        continue
      if r['source'] not in self.preloaded_sources:
        f = reject + 'there is no known source for DataSample with label %s.'
        self.logger.error(f % r['label'])
        continue
      if r['device'] not in self.preloaded_devices:
        f = reject + 'there is no known device for DataSample with label %s.'
        self.logger.error(f % r['label'])
        continue
      if r['scanner'] and r['scanner'] not in self.preloaded_scanners:
        f = reject + 'there is no known scanner for DataSample with label %s.'
        self.logger.error(f % r['label'])
        continue
      if (r['data_sample_type']
          and r['data_sample_type'] == 'GenotypeDataSample'):
        device = self.preloaded_devices[r['device']]
        if not isinstance(device, self.kb.GenotypingProgram):
          if not r['markers_set']:
            f = reject + 'no markers_set vid for GenotypeDataSample %s.'
            self.logger.error(f % r['label'])
            continue
          elif not r['markers_set'] in self.preloaded_markers_sets:
            f = reject + 'illegal markers_set vid for GenotypeDataSample %s.'
            self.logger.error(f % r['label'])
            continue
      if r['options'] :
        try:
          kvs = r['options'].split(',')
          for kv in kvs:
            k,v = kv.split('=')
        except ValueError, e:
          f = reject + 'illegal options string.'
          self.logger.error(f)
          continue
      k_map[r['label']] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records


  def process_chunk(self, otsv, chunk, study):
    def get_options(r):
      options = {}
      if r['options']:
        kvs = r['options'].split(',')
        for kv in kvs:
          k, v = kv.split('=')
          options[k] = v
      return options
    data_samples_status_map = get_status_map(self.kb)
    actions = []
    for r in chunk:
      target = self.preloaded_sources[r['source']]
      device = self.preloaded_devices[r['device']]
      options = get_options(r)
      if isinstance(device, self.kb.Chip) and r['scanner']:
        options['scanner_label'] = self.preloaded_scanners[r['scanner']].label

      # FIXME: the following is an hack. In principle, it should not
      # be possible to reload the same data_sample twice, so if are
      # trying to get an ActionSetup with a label that already exists
      # it means that the previous attemps aborted before that the
      # data_sample could be saved. The data_sample import should
      # clean after itself and remove the debries of failed
      # imports. However, it currently does not do it, so we had to
      # put in a workaround.

      alabel = ('importer.data_sample.%s-%f' % (r['label'], time.time()))
      asetup = self.kb.factory.create(self.kb.ActionSetup,
                                      {'label' : alabel,
                                       'conf' : json.dumps(options)})

      if issubclass(self.source_klass, self.kb.Vessel):
        a_klass = self.kb.ActionOnVessel
        acat = self.kb.ActionCategory.MEASUREMENT
      elif issubclass(self.source_klass, self.kb.DataSample):
        a_klass = self.kb.ActionOnDataSample
        acat = self.kb.ActionCategory.PROCESSING
      elif issubclass(self.source_klass, self.kb.Individual):
        a_klass = self.kb.ActionOnIndividual
        acat = self.kb.ActionCategory.MEASUREMENT
      elif issubclass(self.source_klass, self.kb.DataCollectionItem):
        a_klass = self.kb.ActionOnDataCollectionItem
        acat = self.kb.ActionCategory.PROCESSING
      else:
        assert False
      conf = {'setup' : asetup,
              'device': device,
              'actionCategory' : acat,
              'operator' : self.operator,
              'context'  : study,
              'target' : target}
      actions.append(self.kb.factory.create(a_klass, conf))
    self.kb.save_array(actions)
    data_samples = []
    for a, r in it.izip(actions, chunk):
      device = a.device
      if isinstance(device, self.kb.GenotypingProgram):
        k = ('CRS4', 'Genotyper', 'by_device')
      elif 'markers_set' in r:
        r['markers_set'] = self.preloaded_markers_sets[r['markers_set']]
        k = ('CRS4', 'Genotyper', 'by_markers_set')
      else:
        k = (device.maker, device.model, device.release)
      a.unload()  # FIXME we need to do this, otherwise the next save will choke
      d = data_sample_configurator[k](self.kb, r, a, device, get_options(r),
                                      data_samples_status_map)
      data_samples.append(d)
    assert len(data_samples) == len(chunk)
    self.kb.save_array(data_samples)
    for d in data_samples:
      otsv.writerow({'study' : study.label,
                     'label' : d.label,
                     'type'  : d.get_ome_table(),
                     'vid'   : d.id })


def canonize_records(args, records):
  fields = ['study', 'scanner', 'source_type', 'device_type',
            'data_sample_type', 'markers_set', 'status']
  for f in fields:
    v = getattr(args, f, None)
    if v is not None:
      for r in records:
        r[f] = v
  for r in records:
    if 'scanner' in r and 'device' not in r:
      r['device'] = r['scanner']
      r['device_type'] = 'Scanner'
    if 'data_sample_type' not in r:
      r['data_sample_type'] = None
    for t in ['options', 'scanner']:
      if not (t in r and r[t].upper() != 'NONE'):
        r[t] = None


def make_parser_data_sample(parser):
  parser.add_argument('--study', type=str,
                      help="""default study assumed as context for the
                      import action.  It will
                      over-ride the study column value, if any.""")
  parser.add_argument('--source-type', type=str,
                      choices=['Tube', 'PlateWell', 'DataSample', 'Individual',
                               'DataCollectionItem'],
                      help="""default source type.  It will
                      over-ride the source_type column value, if any.
                      """)
  parser.add_argument('--data-sample-type', type=str,
                      choices=['GenotypeDataSample',
                               # FIXME this is a temporary
                               # solution. It should be something that
                               # checks that the required type is
                               # derived from DataSample. Use this
                               # flag only if GenotypeDataSample is
                               # needed.
                               ],
                      help="""default data sample type.  It will
                      over-ride the data_sample_type column value, if any.
                      """)
  parser.add_argument('--device-type', type=str,
                      choices=['Device', 'Chip', 'Scanner', 'SoftwareProgram',
                               'GenotypingProgram'],
                      help="""default device type.  It will
                      over-ride the device_type column value, if any""")
  parser.add_argument('--scanner', type=str,
                      help="""default scanner.
                      It will over-ride the scanner column value, if
                      any. If a record does not provide a device, it will be
                      set to be a Scanner with this vid. """)
  parser.add_argument('--markers-set', type=str,
                      help="""default markers set vid for GenotypeDataSample.
                      It will over-ride the
                      markers_set column value, if any.""")
  parser.add_argument('--batch-size', type=int,
                      help="""Size of the batch of objects
                      to be processed in parallel (if possible)""",
                      default=1000)


def import_data_sample_implementation(logger, args):
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=args.host, user=args.user, passwd=args.passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonize_records(args, records)
  if len(records) > 0:
    o = csv.DictWriter(args.ofile,
                       fieldnames=['study', 'label', 'type', 'vid'],
                       delimiter='\t')
    o.writeheader()
    recorder.record(records, o)
  else:
    logger.info('empty file')
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new data sample definitions into an omero/vl system and attach
them to previously registered samples.
"""


def do_register(registration_list):
  registration_list.append(('data_sample', help_doc,
                            make_parser_data_sample,
                            import_data_sample_implementation))
