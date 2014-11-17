# BEGIN_COPYRIGHT
# END_COPYRIGHT

"""
Import data_sample
==================

Will read in a tsv file with the following columns::

  study  label source  device  device_type scanner options             status
  ASTUDY foo01 V039090 V093090 Chip        v099020 celID=0009099090    USABLE
  ASTUDY foo02 V039090 V099022 Scanner     v099022 conf1=...,conf2=... UNKNOWN
  ...

and instantiate a specialized DataSample-derived class for each line.

In the above example, the first data sample has been obtained by using
chip V093090 on scanner V099020, while the second one has been
obtained using a direct scanning technology, e.g., an Illumina HiSeq
2000. The optional scanner column, the vid of the scanner device, is
used in cases, such as Affymetrix genotyping, where it is relevant.

The general strategy is to decide what DataSample subclasses should be
instantiated by retrieving the Device and look at its maker, model,
release attributes. The optional data_sample_type column overrides
all automatic decisions.

It is also possible to import DataSample(s) that are the results of
processing other DataSample(s). Here is an example::

  study  label source  device  device_type     options             status
  ASTUDY foo01 V039090 V099021 SoftwareProgram conf1=...,conf2=... USABLE
  ASTUDY foo02 V039090 V099021 SoftwareProgram conf1=...,conf2=... USABLE
  ...

A special case is the GenotypeDataSample, where it is mandatory to
assign a SNPMarkerSet by listing its VID in the markers_set column. As
an example::

  study  label source device   device_type     data_sample_type   markers_set status
  ASTUDY foo01 V039090 V099021 SoftwareProgram GenotypeDataSample V020202      USABLE
  ASTUDY foo02 V039090 V099021 SoftwareProgram GenotypeDataSample V020202      USABLE
  ...
"""

import os, csv, json, time, copy
import itertools as it

import core


SUPPORTED_SOURCES = [
  'Tube',
  'PlateWell',
  'DataSample',
  'Individual',
  'DataCollectionItem',
  'IlluminaBeadChipArray',
  'IlluminaBeadChipMeasures',
  ]
SUPPORTED_DEVICES = [
  'Device',
  'Chip',
  'Scanner',
  'SoftwareProgram',
  'GenotypingProgram',
  ]
SUPPORTED_DATA_SAMPLE_TYPES = [
  'GenotypeDataSample',
  'IlluminaBeadChipMeasure',
  'GenomeVariationsDataSample'
]


def conf_affymetrix_cel_6(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'arrayType' : kb.AffymetrixCelArrayType.GENOMEWIDESNP_6}
  if 'celID' in options:
    conf['celID'] = options['celID']
  return kb.factory.create(kb.AffymetrixCel, conf)


def conf_crs4_genotyper_by_device(kb, r, a, device, options, status_map):
  device.reload()
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'snpMarkersSet' : device.snpMarkersSet}
  return kb.factory.create(kb.GenotypeDataSample, conf)


def conf_crs4_genotyper_by_markers_set(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'snpMarkersSet' : r['markers_set']}
  return kb.factory.create(kb.GenotypeDataSample, conf)

def conf_genome_variant_data_sample(kb, r, a, device, options, status_map):
  conf = {'label' : r['label'],
          'status' : status_map[r['status']],
          'action' : a,
          'referenceGenome' : r['genome_reference']}
  return kb.factory.create(kb.GenomeVariationsDataSample, conf)

def conf_illumina_bead_chip_measure(kb, r, a, device, options, status_map):
  conf = {
    'label': r['label'],
    'status': status_map[r['status']],
    'action': a,
  }
  return kb.factory.create(kb.IlluminaBeadChipMeasure, conf)

def get_status_map(kb):
  return {'UNKNOWN'   : kb.DataSampleStatus.UNKNOWN,
          'DESTROYED' : kb.DataSampleStatus.DESTROYED,
          'CORRUPTED' : kb.DataSampleStatus.CORRUPTED,
          'USABLE'    : kb.DataSampleStatus.USABLE}


data_sample_configurator = {
  ('Affymetrix', 'Genome-Wide Human SNP Array', '6.0'): conf_affymetrix_cel_6,
  ('CRS4', 'Genotyper', 'by_device'): conf_crs4_genotyper_by_device,
  ('CRS4', 'Genotyper', 'by_markers_set'): conf_crs4_genotyper_by_markers_set,
  ('Illumina', 'generic_illumina_scanner', 'generic'): conf_illumina_bead_chip_measure,
  ('GenomeVariationsDataSample'): conf_genome_variant_data_sample,
  }


class Recorder(core.Core):
  def __init__(self, study_label=None,
               host=None, user=None, passwd=None, keep_tokens=1,
               batch_size=1000, operator='Alfred E. Neumann',
               action_setup_conf=None, logger=None):
    super(Recorder, self).__init__(host, user, passwd, keep_tokens=keep_tokens,
                                   study_label=study_label, logger=logger)
    self.batch_size = batch_size
    self.action_setup_conf = action_setup_conf
    self.operator = operator

  def record(self, records, otsv, rtsv, blocking_validation):
    def records_by_chunk(batch_size, records):
      offset = 0
      while len(records[offset:]) > 0:
        yield records[offset:offset+batch_size]
        offset += batch_size
    if len(records) == 0:
      msg = 'No records are going to be imported'
      self.logger.critical(msg)
      raise core.ImporterValidationError(msg)
    study = self.find_study(records)
    self.source_klass = self.find_source_klass(records)
    self.device_klass = self.find_device_klass(records)
    self.data_sample_klass = self.data_sample_klass(records)
    records, bad_records = self.do_consistency_checks(records)
    for br in bad_records:
      rtsv.writerow(br)
    if blocking_validation and len(bad_records) >= 1:
      raise core.ImporterValidationError('%d invalid records' % len(bad_records))
    for i, c in enumerate(records_by_chunk(self.batch_size, records)):
      self.logger.info('start processing chunk %d' % i)
      self.process_chunk(otsv, c, study)
      self.logger.info('done processing chunk %d' % i)

  def find_source_klass(self, records):
    return self.find_klass('source_type', records)

  def find_device_klass(self, records):
    return self.find_klass('device_type', records)

  def find_data_sample_klass(self, records):
    return self.find_klass('data_sample_type', records)

  def do_consistency_checks(self, records):
    self.logger.info('start consistency checks')
    k_map = {}
    good_records = []
    bad_records = []
    mandatory_fields = ['label', 'source', 'device', 'status']
    for i, r in enumerate(records):
      reject = 'Rejecting import of row %d: ' % i
      mf = self.missing_fields(mandatory_fields, r)
      if mf:
        f = 'missing mandatory field %s' % mf
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['status'] not in ['UNKNOWN', 'DESTROYED', 'CORRUPTED', 'USABLE']:
        f = 'unknown status value.'
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if self.is_known_object_label(r['label'], self.kb.DataSample):
        f = 'there is a pre-existing DataSample with label %s' % r['label']
        self.logger.warn(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['label'] in k_map:
        f = 'there is a pre-existing record with label %s.(in this batch)' % r['label']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if not self.is_known_object_id(r['source'], self.source_klass):
        f = 'there is no known source with ID %s' % r['source']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if not self.is_known_object_id(r['device'], self.device_klass):
        f = 'there is no known device with ID %s' % r['device']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if r['scanner'] and not self.is_known_object_id(r['scanner'], self.kb.Scanner):
        f = 'there is no known scanner with ID %s' % r['scanner']
        self.logger.error(reject + f)
        bad_rec = copy.deepcopy(r)
        bad_rec['error'] = f
        bad_records.append(bad_rec)
        continue
      if (r['data_sample_type']
          and r['data_sample_type'] == 'GenotypeDataSample'):
        device = self.kb.get_by_vid(self.kb.Device, r['device'])
        if not isinstance(device, self.kb.GenotypingProgram):
          if not r['markers_set']:
            f = 'no markers set specified for data sample %s' % r['label']
            self.logger.error(reject + f)
            bad_rec = copy.deepcopy(r)
            bad_rec['error'] = f
            bad_records.append(bad_rec)
            continue
          elif not self.is_known_object_id(r['markers_set'], self.kb.SNPMarkersSet):
            f = 'there is no known markers set with ID %s' % r['markers_set']
            self.logger.error(reject + f)
            bad_rec = copy.deepcopy(r)
            bad_rec['error'] = f
            bad_records.append(bad_rec)
            continue

      if (r['data_sample_type'] and r['data_sample_type'] == 'GenomeVariationsDataSample'):
          if r['genome_reference'] and not self.is_known_object_id(r['genome_reference'], self.kb.ReferenceGenome):
                m = 'unknown reference genome with ID %s. ' % r['genome_reference']
                self.logger.warning(m + reject)
                bad_rec = copy.deepcopy(r)
                bad_rec['error'] = m
                bad_records.append(bad_rec)
                continue
          #continue

      if r['options'] :
        try:
          kvs = r['options'].split(',')
          for kv in kvs:
            k,v = kv.split('=')
        except ValueError, e:
          f = 'illegal options string'
          self.logger.error(reject + f)
          bad_rec = copy.deepcopy(r)
          bad_rec['error'] = f
          bad_records.append(bad_rec)
          continue
      k_map[r['label']] = r
      good_records.append(r)
    self.logger.info('done consistency checks')
    return good_records, bad_records

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
      target = self.kb.get_by_vid(self.source_klass, r['source'])
      device = self.kb.get_by_vid(self.kb.Device, r['device'])
      options = get_options(r)
      if isinstance(device, self.kb.Chip) and r['scanner']:
        options['scanner_label'] = self.kb.get_by_vid(self.kb.Scanner, r['scanner']).label

      # FIXME: the following is a hack. In principle, it should not
      # be possible to reload the same data_sample twice, therefore
      # trying to get an ActionSetup with a label that already exists
      # means that the previous attemps aborted before the
      # data_sample could be saved. The data_sample import should
      # clean after itself and remove the garbage from failed
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
      elif issubclass(self.source_klass, self.kb.VLCollection):
        a_klass = self.kb.ActionOnCollection
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
        r['markers_set'] = self.kb.get_by_vid(self.kb.SNPMarkersSet, r['markers_set'])
        k = ('CRS4', 'Genotyper', 'by_markers_set')
      else:
        k = (device.maker, device.model, device.release)

      #if self.data_sample_klass == self.kb.GenomeVariationsDataSample:
      if 'data_sample_type' in r and r['data_sample_type'] == 'GenomeVariationsDataSample':
          k = ('GenomeVariationsDataSample')
      a.unload()  # FIXME we need to do this, or the next save will choke
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


class RecordCanonizer(core.RecordCanonizer):

  def canonize(self, r):
    super(RecordCanonizer, self).canonize(r)
    if 'scanner' in r and 'device' not in r:
      r['device'] = r['scanner']
      r['device_type'] = 'Scanner'
    r.setdefault('data_sample_type')
    for f in 'options', 'scanner':
      if r.get(f, 'NONE').upper() == 'NONE':
        r[f] = None
  

def make_parser(parser):
  parser.add_argument('--study', metavar="STRING",
                      help="overrides the study column value")
  parser.add_argument('--source-type', metavar="STRING",
                      choices=SUPPORTED_SOURCES,
                      help="overrides the source_type column value")
  # FIXME the following is a temporary solution. It should be
  # something that checks that the required type is derived from
  # DataSample. Use this flag only if GenotypeDataSample is needed.
  parser.add_argument('--data-sample-type', metavar="STRING",
                      choices=SUPPORTED_DATA_SAMPLE_TYPES,
                      help="overrides the data_sample_type column value")
  parser.add_argument('--device-type', metavar="STRING",
                      choices=SUPPORTED_DEVICES,
                      help="overrides the device_type column value")
  parser.add_argument('--scanner', metavar="STRING",
                      help="""overrides the scanner column value.
                      It is also used to set the device if a record does
                      not provide one""")
  parser.add_argument('--markers-set', metavar="STRING",
                      help="overrides the markers_set column value")
  parser.add_argument('--batch-size', type=int, metavar="INT", default=1000,
                      help="n. of objects to be processed at a time")


def implementation(logger, host, user, passwd, args, close_handles):
  fields_to_canonize = [
    'study',
    'scanner',
    'source_type',
    'device_type',
    'data_sample_type',
    'markers_set',
    'status',
    ]
  action_setup_conf = Recorder.find_action_setup_conf(args)
  recorder = Recorder(args.study,
                      host=host, user=user, passwd=passwd,
                      operator=args.operator,
                      action_setup_conf=action_setup_conf,
                      logger=logger)
  f = csv.DictReader(args.ifile, delimiter='\t')
  logger.info('start processing file %s' % args.ifile.name)
  records = [r for r in f]
  canonizer = RecordCanonizer(fields_to_canonize, args)
  canonizer.canonize_list(records)
  o = csv.DictWriter(args.ofile,
                     fieldnames=['study', 'label', 'type', 'vid'],
                     delimiter='\t', lineterminator=os.linesep)
  o.writeheader()
  report_fnames = copy.deepcopy(f.fieldnames)
  report_fnames.append('error')
  report = csv.DictWriter(args.report_file, report_fnames,
                          delimiter='\t', lineterminator=os.linesep,
                          extrasaction='ignore')
  report.writeheader()
  try:
    recorder.record(records, o, report,
                    args.blocking_validator)
  except core.ImporterValidationError as ve:
    logger.critical(ve.message)
    raise
  finally:
    close_handles(args)
  logger.info('done processing file %s' % args.ifile.name)


help_doc = """
import new data sample definitions into the knowledge base and link
them to previously registered samples.
"""


def do_register(registration_list):
  registration_list.append(('data_sample', help_doc, make_parser,
                            implementation))
