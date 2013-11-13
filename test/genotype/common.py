import unittest, time, os, random
import itertools as it
import tempfile
import numpy as np

from bl.core.io import MessageStreamWriter
import bl.core.gt.messages.SnpCall as SnpCall
import bl.vl.utils as vlu

from kb_object_creator import KBObjectCreator

from bl.vl.kb.drivers.omero.genomics import MSET_TABLE_COLS_DTYPE

PAYLOAD_MSG_TYPE = 'core.gt.messages.SampleSnpCall'


class UTCommon(KBObjectCreator):

    def create_markers_set(self, N):
        label = 'ams-%f' % time.time()
        maker, model, release = 'FOO', 'FOO1', '%f' % time.time()
        vid = vlu.make_vid()
        rows = np.array([('M%d' % i, i, 'AC[A/G]GT', False, vid) 
                         for i in xrange(N)],
                         dtype=MSET_TABLE_COLS_DTYPE)
        mset = self.kb.genomics.create_markers_array(
            label, maker, model, release, rows, self.action
            )
        return mset, rows

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
                                                  data_sample, probs, confs)
        return do, probs, confs
