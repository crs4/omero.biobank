# pylint: disable=E1101

import time
import itertools as it
import numpy as np

from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
import bl.vl.utils as vlu

from kb_object_creator import KBObjectCreator
from bl.vl.kb.drivers.omero.proxy_core import convert_from_numpy

from bl.vl.kb.drivers.omero.genomics import MARKER_LABEL_SIZE, MARKER_MASK_SIZE
PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'

MSET_TABLE_COLS_DTYPE  = [('label', '|S%d' % MARKER_LABEL_SIZE),
                          ('index', 'i8'),
                          ('mask',  '|S%d' % MARKER_MASK_SIZE),
                          ('permutation', '?')]

class UTCommon(KBObjectCreator):
    
    def create_markers_set_from_stream(self, N):
        label = 'ams-%f' % time.time()
        maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
        rows = np.array([('M%d' % i, i, 'AC[A/G]GT', False) 
                         for i in xrange(N)],
                         dtype=MSET_TABLE_COLS_DTYPE)
        def stream(rows):
            dtype = rows.dtype
            for r in rows:
                yield dict([(k, convert_from_numpy(r[k])) for k in dtype.names])
        mset = self.kb.genomics.create_markers_array(
            label, maker, model, release, stream(rows), self.action
            )
        return mset, rows

    def create_markers_set(self, N):
        label = 'ams-%f' % time.time()
        maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
        rows = np.array([('M%d' % i, i, 'AC[A/G]GT', False) 
                         for i in xrange(N)],
                         dtype=MSET_TABLE_COLS_DTYPE)
        mset = self.kb.genomics.create_markers_array(
            label, maker, model, release, rows, self.action
            )
        return mset, rows

    def create_reference_genome(self, action):
        conf = {'nChroms' : 10, 
                'maker': vlu.make_random_str(),
                'model': vlu.make_random_str(),
                'release' : vlu.make_random_str(),
                'label': vlu.make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        reference_genome = self.kb.factory.create(self.kb.ReferenceGenome,
                                                  conf).save()
        return reference_genome

    @staticmethod
    def make_fake_data(n, add_nan=False):
        probs = 0.5 * np.cast[np.float32](np.random.random((2, n)))
        confs = np.cast[np.float32](np.random.random(n))
        if add_nan:
            rand_indices = np.random.random_integers(
            0, len(probs[0]) - 1, len(probs[0]) / 2
            )
            for x in set(rand_indices):
                probs[0][x] = np.nan
                probs[1][x] = np.nan
        return probs, confs

    @staticmethod
    def make_fake_ssc(mset, labels, sample_id, probs, conf, fn):
        header = {'markers_set' : mset.label, 'sample_id':  sample_id}
        stream = MessageStreamWriter(fn, PAYLOAD_MSG_TYPE, header)
        for l, p_AA, p_BB, c in  it.izip(labels, probs[0], probs[1], conf):
            p_AB = 1.0 - (p_AA + p_BB)
            w_aa, w_ab, w_bb = p_AA, p_AB, p_BB
            stream.write({
                'sample_id': sample_id,
                'snp_id': l,
                'call': SnpCall.NOCALL, # we will not test this anyway
                'confidence': float(c),
                'sig_A': float(p_AA),
                'sig_B': float(p_BB),
                'w_AA': float(w_aa),
                'w_AB': float(w_ab),
                'w_BB': float(w_bb),
                })
        stream.close()

    def create_data_sample(self, mset, label, action):
        conf = {
        'label': label,
        'status': self.kb.DataSampleStatus.USABLE,
        'action': action,
        'snpMarkersSet': mset,
        }
        data_sample = self.kb.factory.create(self.kb.GenotypeDataSample,
                                             conf).save()
        return data_sample

    def create_data_object(self, data_sample, action, add_nan=False):
        n = self.kb.genomics.get_number_of_markers(data_sample.snpMarkersSet)
        probs, confs = self.make_fake_data(n, add_nan)
        do = self.kb.genomics.add_gdo_data_object(action, 
                                data_sample, probs, confs).save()
        return do, probs, confs

    def create_variant_call_support(self, mset, reference_genome, action,
                                    chromosome=1):
        VariantCallSupport = self.kb.VariantCallSupport
        N = self.kb.genomics.get_number_of_markers(mset)
        mset_vid = mset.id
        nodes = np.array([(chromosome, 10 * i) for i in xrange(N)], 
                         dtype=VariantCallSupport.NODES_DTYPE)
        field = np.array([(i, mset_vid, i) for i in range(len(nodes))],
                         dtype=VariantCallSupport.ATTR_ORIGIN_DTYPE)
        label = vlu.make_random_str()
        conf = {'referenceGenome' : reference_genome,
                'label' : label,
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}  
        vcs = self.kb.factory.create(VariantCallSupport, conf)
        vcs.define_support(nodes)
        vcs.define_field('origin', field)
        vcs.save()
        return vcs
