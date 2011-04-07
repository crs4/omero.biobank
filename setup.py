from distutils.core import setup
import os, sys, re

setup(name='bl_vl_genotype',
      version='0.0',
      description='BioLand VirgiL module',
      author='Alfred E. Neumann',
      author_email='aen@what_me_worry.ask',
      url='http://www.what_me_worry.ask',
      packages=['bl',
                'bl.vl',
                #-
                'bl.vl.utils',
                #-
                'bl.vl.sample',
                'bl.vl.sample.kb',
                'bl.vl.sample.kb.drivers',
                'bl.vl.sample.kb.drivers.omero',
                #-
                'bl.vl.individual',
                'bl.vl.individual.kb',
                'bl.vl.individual.kb.drivers',
                'bl.vl.individual.kb.drivers.omero',
                #-
                'bl.vl.genotype',
                'bl.vl.genotype.kb',
                'bl.vl.genotype.kb.drivers',
                'bl.vl.genotype.kb.drivers.omero',
                ],
      )
