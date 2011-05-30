from distutils.core import setup
import os, sys, re

setup(name='bl_vl_genotype',
      version='0.3',
      description='BioLand VirgiL module',
      author='Alfred E. Neumann',
      author_email='aen@what_me_worry.ask',
      url='http://www.what_me_worry.ask',
      packages=['bl',
                'bl.vl',
                #-
                'bl.vl.utils',
                #-
                'bl.vl.kb',
                'bl.vl.kb.drivers',
                'bl.vl.kb.drivers.omero',
                #-
                'bl.vl.individual',
                #-
                'bl.vl.genotype',
                #-
                'bl.vl.app',
                'bl.vl.app.importer',
                'bl.vl.app.exporter',
                'bl.vl.app.kb_query',
                ],
      )
