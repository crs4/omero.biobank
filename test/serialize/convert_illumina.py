import sqlite3
import csv, os
import sys

from bl.vl.kb.serialize.writers import *

dbname = sys.argv[1] # 'test_illumina_chips.db'
fname =  sys.argv[2] #'test_illumina_load.yml'

def fetch_source_plates_wells_and_target_chip(dbname):
    sql_q = """select Sample_Well, Sample_ID, SentrixBarcode_A, 
                      SentrixPosition_A, Chip_Type from summaries"""
    with sqlite3.connect(dbname) as conn:
        c = conn.cursor()
        c.execute(sql_q)
        res = c.fetchall()
    plates = {}
    links = []
    for r in res:
        plate_label = r[1].split(':')[0]
        if plate_label == 'Empty':
            continue
        well_oid = plate_label+':'+r[0]
        plates.setdefault(plate_label, set()).add((well_oid, r[0]))
        links.append((well_oid, (r[2], r[3], r[4])))
    return plates, links

def fetch_slides(dbname):
    sql_q = "select SentrixBarcode_A, Type, Class, Assay_Type, Rows, Cols from sentrix"
    with sqlite3.connect(dbname) as conn:
        c = conn.cursor()
        c.execute(sql_q)
        res = c.fetchall()
    slides = {}
    for r in res:
        slides[r[0]] = {'barcode': r[0],
                        'type': r[1].replace(' ', '_'), 
                        'class': r[2], 
                        'assayType': r[3].replace(' ', '_'),
                        'rows': int(r[4]),
                        'columns': int(r[5])}
    return slides

plates, links = fetch_source_plates_wells_and_target_chip(dbname)
slides = fetch_slides(dbname)


rows = 8
columns = 12
aref = 'act_01'
with open(fname, "w") as o:
    write_action_setup(o, 'asetup_01', 'asetup_01')
    write_device(o, 'device_01', 'device_01', 
                 'maker_01', 'model_01', 'release_01')
    write_study(o, 'study_01', 'study_01')
    write_action(o, 'act_01', setup=by_ref('asetup_01'), 
                 device=by_ref('device_01'), category="IMPORT", 
                 operator='Alfred E. Neumann', context=by_ref('study_01'))
    for l in plates:
        write_titer_plate(o, l, l, 'b-'+l, 'READY', rows, columns,
                          by_ref('act_01'))
        for oid, w in plates[l]:
            write_plate_well(o, oid, w, by_ref(l), 
                             'DNA', 'CONTENTUSABLE', by_ref('act_01'))
    
    for oid in slides:
        slide = slides[oid]
        # FIXME we force to UNKNOWN because we are still missing some possible 
        # array and assay types.
        slide['assayType'] = 'UNKNOWN'
        slide['type'] = 'UNKNOWN'        
        write_illumina_array_of_arrays(o, oid, 
                                       'label-'+slide['barcode'],
                                       slide['barcode'],
                                       'READY',
                                       slide['rows'], slide['columns'],
                                       slide['type'],
                                       slide['class'],
                                       slide['assayType'],
                                       by_ref('act_01'))
    for sample_oid, (slide_barcode, slide_well, well_type) in links:
        aoid = sample_oid + '.action'
        oid = slide_barcode +':'+ slide_well
        write_action(o, aoid, setup=by_ref('asetup_01'), 
                     device=by_ref('device_01'), category="IMPORT", 
                     operator='Alfred E. Neumann', context=by_ref('study_01'),
                     target=by_ref(sample_oid), target_class="Vessel")
        write_illumina_bead_chip_array(o, oid, label=slide_well, 
                                       container=by_ref(slide_barcode), 
                                       content='DNA',
                                       status='CONTENTUSABLE', 
                                       assay_type=well_type,
                                       action=by_ref(aoid))












