# BEGIN_COPYRIGHT
# END_COPYRIGHT

import os, unittest, logging, uuid
import numpy as np
logging.basicConfig(level=logging.ERROR)

from bl.vl.kb import KnowledgeBase as KB
from kb_object_creator import KBObjectCreator


OME_HOST = os.getenv("OME_HOST", "localhost")
OME_USER = os.getenv("OME_USER", "root")
OME_PASS = os.getenv("OME_PASS", "romeo")

def make_random_str():
    return uuid.uuid4().hex

class TestVCS(KBObjectCreator):

    def __init__(self, name):
        super(TestVCS, self).__init__(name)
        self.kill_list = []

    def create_reference_genome(self, action):
        conf = {'nChroms' : 10, 
                'maker': make_random_str(),
                'model': make_random_str(),
                'release' : make_random_str(),
                'label': make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        reference_genome = self.kb.factory.create(self.kb.ReferenceGenome,
                                                  conf).save()
        self.kill_list.append(reference_genome)
        return reference_genome

    def create_action(self):
        _, action = super(TestVCS, self).create_action()
        action = action.save()
        self.kill_list.append(action)
        return action

    def setUp(self):
        self.kb = KB(driver='omero')(OME_HOST, OME_USER, OME_PASS)

    def tearDown(self):
        while self.kill_list:
            self.kb.delete(self.kill_list.pop())

    def test_creation(self):
        VariantCallSupport = self.kb.VariantCallSupport
        nodes = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3)], 
                         dtype=VariantCallSupport.NODES_DTYPE)
        vcs = VariantCallSupport(None, None)
        vcs.define_support(nodes)
        self.assertEqual(len(vcs), len(nodes))
        self.assertTrue(np.alltrue(nodes == vcs.get_nodes()))
        self.assertEqual(vcs.get_fields(), {})
	
    def test_selection(self):
        VariantCallSupport = self.kb.VariantCallSupport
        nodes = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3)], 
                        dtype=VariantCallSupport.NODES_DTYPE)
        action = self.create_action()
        reference_genome = self.create_reference_genome(action)
        conf = {'referenceGenome' : reference_genome,
                'label' : make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}  
        vcs = self.kb.factory.create(VariantCallSupport, conf)
        vcs.define_support(nodes)
        s = vcs.selection((tuple(nodes[1]), tuple(nodes[-1])))
        self.assertTrue(np.alltrue(nodes[1:-1] == s.get_nodes()))
        
    def test_union(self):
        action = self.create_action()        
        reference_genome = self.create_reference_genome(action)
        
        VariantCallSupport = self.kb.VariantCallSupport
        conf = {'referenceGenome' : reference_genome,
                'label' : make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        vcs1 = self.kb.factory.create(VariantCallSupport, conf)

        conf['label'] = make_random_str()
        vcs2 = self.kb.factory.create(VariantCallSupport, conf)
        
        nodes1 = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3), (2,4)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        nodes2 = np.array([(1, 3), (2, 1), (2,2), (2, 3), (3, 1), (3, 2)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        merged = np.hstack([nodes1[:4], nodes2[2:3], nodes1[4:6], nodes2[4:]])
        vcs1.define_support(nodes1)
        vcs2.define_support(nodes2)
        vcs3 = vcs1.union(vcs2)
        vcs4 = vcs2.union(vcs1)
        vcs5 = vcs1.selection((tuple(nodes1[2]), tuple(nodes1[4])))
        vcs6 = vcs1.union(vcs5)
        vcs7 = vcs1.union(vcs1)
        vcs8 = vcs5.union(vcs1)
        self.assertTrue(np.alltrue(vcs3.get_nodes() == merged))
        self.assertTrue(np.alltrue(vcs3.get_nodes() == vcs4.get_nodes()))
        self.assertTrue(np.alltrue(vcs6.get_nodes() == vcs1.get_nodes()))
        self.assertTrue(np.alltrue(vcs6.get_nodes() == vcs1.get_nodes()))
        self.assertTrue(np.alltrue(vcs7.get_nodes() == vcs1.get_nodes()))
        self.assertTrue(np.alltrue(vcs8.get_nodes() == vcs1.get_nodes()))

    def test_intersection(self):
        action = self.create_action()        
        reference_genome = self.create_reference_genome(action)
        VariantCallSupport = self.kb.VariantCallSupport
        conf = {'referenceGenome' : reference_genome,
                'label' : make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        vcs1 = self.kb.factory.create(VariantCallSupport, conf)
        conf['label'] = make_random_str()
        vcs2 = self.kb.factory.create(VariantCallSupport, conf)
        nodes1 = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3), 
                    (2,4), (3,5)], dtype=VariantCallSupport.NODES_DTYPE)
        nodes2 = np.array([(1, 2), (2, 1), (2, 4), (3, 1), (3, 2)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        intersected =  np.array([(1, 2), (2, 1), (2,4)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        vcs1.define_support(nodes1)
        vcs2.define_support(nodes2)
        vcs3 = vcs1.intersection(vcs1)
        self.assertTrue(np.alltrue(vcs1.get_nodes() == vcs3.get_nodes()))
        vcs4 = vcs1.intersection(vcs2)
        vcs5 = vcs2.intersection(vcs1)
        self.assertTrue(np.alltrue(vcs4.get_nodes() == intersected))
        self.assertTrue(np.alltrue(vcs4.get_nodes() == vcs5.get_nodes()))
        vcs6 = vcs1.selection((tuple(nodes1[2]), tuple(nodes1[5])))
        vcs7 = vcs1.intersection(vcs6)
        self.assertTrue(np.alltrue(vcs7.get_nodes() == vcs6.get_nodes()))

    def test_intersection(self):
        action = self.create_action()        
        reference_genome = self.create_reference_genome(action)
        VariantCallSupport = self.kb.VariantCallSupport
        conf = {'referenceGenome' : reference_genome,
                'label' : make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        vcs1 = self.kb.factory.create(VariantCallSupport, conf)
        conf['label'] = make_random_str()
        vcs2 = self.kb.factory.create(VariantCallSupport, conf)
        nodes1 = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3), 
                    (2,4), (3,5)], dtype=VariantCallSupport.NODES_DTYPE)
        nodes2 = np.array([(1, 2), (2, 1), (2, 4), (3, 1), (3, 2)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        intersected =  np.array([(1, 2), (2, 1), (2,4)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        vcs1.define_support(nodes1)
        vcs2.define_support(nodes2)
        vcs3 = vcs1.intersection(vcs1)
        self.assertTrue(np.alltrue(vcs1.get_nodes() == vcs3.get_nodes()))
        vcs4 = vcs1.intersection(vcs2)
        vcs5 = vcs2.intersection(vcs1)
        self.assertTrue(np.alltrue(vcs4.get_nodes() == intersected))
        self.assertTrue(np.alltrue(vcs4.get_nodes() == vcs5.get_nodes()))
        vcs6 = vcs1.selection((tuple(nodes1[2]), tuple(nodes1[5])))
        vcs7 = vcs1.intersection(vcs6)
        self.assertTrue(np.alltrue(vcs7.get_nodes() == vcs6.get_nodes()))

    def test_complement(self):
        action = self.create_action()        
        reference_genome = self.create_reference_genome(action)
        VariantCallSupport = self.kb.VariantCallSupport
        conf = {'referenceGenome' : reference_genome,
                'label' : make_random_str(),
                'status' : self.kb.DataSampleStatus.USABLE,
                'action': action}
        vcs1 = self.kb.factory.create(VariantCallSupport, conf)
        conf['label'] = make_random_str()
        vcs2 = self.kb.factory.create(VariantCallSupport, conf)
        nodes1 = np.array([(1, 1), (1, 2), (1, 3), (2, 1), (2, 3), 
                    (2,4), (3,5)], dtype=VariantCallSupport.NODES_DTYPE)
        nodes2 = np.array([(1, 2), (2, 1), (2, 4), (3, 1), (3, 2)], 
                          dtype=VariantCallSupport.NODES_DTYPE)
        complement = np.array([(1, 1), (1, 3), (2, 3), (3, 5)],
                              dtype=VariantCallSupport.NODES_DTYPE)
        vcs1.define_support(nodes1)
        vcs2.define_support(nodes2)
        vcs3 = vcs1.complement(vcs1)
        self.assertEqual(len(vcs3), 0)
        vcs4 = vcs1.complement(vcs2)
        self.assertTrue(np.alltrue(vcs4.get_nodes() == complement))
        

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestVCS('test_creation'))
    suite.addTest(TestVCS('test_selection'))    
    suite.addTest(TestVCS('test_union'))        
    suite.addTest(TestVCS('test_intersection'))        
    suite.addTest(TestVCS('test_complement'))            
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))
