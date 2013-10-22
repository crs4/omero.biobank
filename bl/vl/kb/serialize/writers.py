import uuid

def protect(string):
    return '\'%s\'' % string

def by_ref(ref):
    return {'by_ref': str(ref)}

def by_vid(vid):
    return {'by_vid': str(vid)}

def by_label(label):
    return {'by_label': str(label)}

def write_object(ostream, oid, otype, configuration, vid=None):
    ostream.write('"%s":\n type: %s\n configuration:\n' % (oid, otype))
    for k, v in configuration.iteritems():
        if type(v) == str:
            v = protect(v)
        ostream.write('  %s: %s\n' % (k, v))
    if not vid is None:
        ostream.write('  "%s": %s\n' % ('vid', vid))
    ostream.write('\n')

def write_action_setup(ostream, oid, label, conf={}, vid=None):
    write_object(ostream, oid, 'ActionSetup',
                 {'label' : label, 'conf' : conf}, vid=vid)

def write_device(ostream, oid, label, maker, model, release, vid=None):
    write_object(ostream, oid, 'Device', 
                 {'label': label, 
                  'maker': maker, 'model': model, 'release': release}, vid=vid)

def write_study(ostream, oid, label, description='', vid=None):
    label = protect(label)
    write_object(ostream, oid, 'Study', 
                 {'label': label, 'description': description}, vid=vid)

def write_action(ostream, oid, setup, device, category, 
                 operator, context, target=None, target_class=None,
                 description='', vid=None):  
    configuration = {'setup': setup, 'device': device, 
                     'actionCategory': category, 'operator': operator,
                     'context': context, 'description': description}
    if not target is None:
        configuration['target'] = target
    if not target_class is None:
        action_class = 'ActionOn%s' % target_class
    else:
        action_class = 'Action'
    write_object(ostream, oid, action_class, configuration, vid=vid)

def write_tube(ostream, oid, label, barcode, content, status, action, vid=None):
    label = protect(label)    
    barcode = protect(barcode)
    write_object(ostream, oid, 'Tube', 
                 {'label': label, 'barcode': barcode, 
                  'currentVolume': 1.0, 'initialVolume' : 1.0,
                  'content' : content, 'status': status, 'action': action}, 
                  vid=vid)

def write_titer_plate(ostream, oid, label, barcode, status, 
                      rows, columns, action, vid=None):
    label = protect(label)    
    barcode = protect(barcode)
    write_object(ostream, oid, 'TiterPlate', 
                 {'label': label, 'barcode': barcode, 'status': status,
                  'rows' : rows, 'columns': columns, 'action': action}, vid=vid)

def write_plate_well(ostream, oid, label, container, content, status, action, 
                     vid=None):
    label = protect(label)    
    write_object(ostream, oid, 'PlateWell', 
                 {'label': label, 'container': container, 
                  'currentVolume': 1.0, 'initialVolume' : 1.0,
                  'content' : content, 'status': status, 'action': action}, 
                  vid=vid)

def write_illumina_array_of_arrays(ostream, oid, label, barcode, status,
                                   rows, columns, atype, aclass, 
                                   assay_type, action, vid=None):
    label = protect(label)    
    barcode = protect(barcode)
    write_object(ostream, oid, 'IlluminaArrayOfArrays',
                 {'label': label, 'barcode': barcode, 'status': status,
                  'rows': rows, 'columns': columns, 
                  'action': action, 'type': atype, 
                  'arrayClass': aclass,  'assayType': assay_type}, vid=vid)
    

    
def write_illumina_bead_chip_array(ostream, oid, label, container, 
                                   content, status, assay_type, action, 
                                   vid=None):
    label = protect(label)    
    write_object(ostream, oid, 'IlluminaBeadChipArray',
                 {'label': label,  'container': container, 
                  'currentVolume': 1.0, 'initialVolume': 1.0,
                  'content': content, 'status': status, 'assayType': assay_type,
                  'action': action}, vid=vid)

    
def write_action_pack(ostream, oid, target=None, target_class=None, vid=None):
      asetup_label  = str(uuid.uuid1())
      adevice_label = str(uuid.uuid1())
      astudy_label = str(uuid.uuid1())
      amaker_label = str(uuid.uuid1())
      
      write_action_setup(ostream, asetup_label, asetup_label)
      write_device(ostream, adevice_label, adevice_label, 
                   amaker_label, 'amodel', 'arelease')
      write_study(ostream, astudy_label, astudy_label)
      write_action(ostream, oid, 
                   by_ref(asetup_label), 
                   by_ref(adevice_label), 
                   "IMPORT", "Alfred E. Neumann",
                   by_ref(astudy_label),
                   target=target, target_class=target_class,
                   vid=vid)

    
