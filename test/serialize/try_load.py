from bl.vl.kb import KnowledgeBase
import bl.vl.kb.serialize.deserialize as ds
from bl.vl.kb.serialize.utils import Error

import yaml
import logging
import sys, os
import itertools as it

OME_HOST = os.getenv('OME_HOST', 'localhost')
OME_USER = os.getenv('OME_USER', 'root')
OME_PASSWD = os.getenv('OME_PASSWD', 'romeo')

CHECK_OME_VERSION = False

fname = sys.argv[1] #'plates.yml'


BaseProxy = KnowledgeBase(driver='omero')

class Proxy(BaseProxy):
  def get_objects_dict(self, klass):
    return dict((o.label, o) for o in super(Proxy, self).get_objects(klass))

kb = Proxy(OME_HOST, OME_USER, OME_PASSWD, check_ome_version=CHECK_OME_VERSION)
kb.logger.setLevel(logging.DEBUG)

kill_list = []

def clean_up():
    kb.logger.info('starting clean up, there are %d objects to delete.' %
                   len(kill_list))
    while kill_list:
        kb.delete(kill_list.pop())
    kb.logger.info('done with clean up.')        

N=1000
kb.logger.info('opening file %s' % fname)        
with open(fname) as f:
    try:
        limbo = ds.ObjectsLimbo(kb, kb.logger)
        for ref, conf in yaml.load(f).iteritems():
            limbo.add_object(ref, conf)
        for t, group in limbo.groupbytype():
            kb.logger.info('saving objects of type %s' % t)
            block = [x for x in it.imap(lambda x: x[1], it.islice(group, 0, N))]
            while block:
                kb.logger.info('saving a block with %d elements' % len(block))
                kill_list += kb.save_array(block)
                kb.logger.info('reloading block')                
                block = [x for x in it.imap(lambda x: x[1], 
                                            it.islice(group, 0, N))]
    except Exception, e:
        kb.logger.fatal('%s, %s' % (e, e.args))
    finally:
        clean_up()

