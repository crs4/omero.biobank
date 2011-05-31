import time

from bl.vl.kb import KnowledgeBase as KB
kb = KB('omero')('localhost', 'root' , 'romeo')


study  = kb.factory.create(kb.Study, {'label' : 'FOOSTUDY-%s' % time.time()})
study.save()

device = kb.factory.create(kb.Device,
                           {'label' : 'FOODEVICE-%s' % time.time(),
                            'maker' : 'makeroo',
                            'model' : 'modeloo',
                            'release' : '0.1',
                            'physicalLocation' : 'foo-place'})
device.save()

asetup = kb.factory.create(kb.ActionSetup,
                           {'label' : 'ASETUP-%s' % time.time(),
                            'conf'  : 'a=2'})
asetup.save()

action = kb.factory.create(kb.Action,
                           {'setup' : asetup,
                            'device' : device,
                            'actionCategory' : kb.ActionCategory.IMPORT,
                            'operator' : 'foo',
                            'context'  : study})
action.save()



