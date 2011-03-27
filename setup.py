from distutils.core import setup
import os, sys, re

setup(name='bl_vl_genotype',
      version='0.0',
      description='BioLand Genotype virgil module',
      author='Alfred E. Neumann',
      author_email='aen@what_me_worry.ask',
      url='http://www.what_me_worry.ask',
      packages=['bl',
                'bl.lib',
                #-
                'bl.lib.sample',
                'bl.lib.sample.kb',
                'bl.lib.sample.kb.drivers',
                'bl.lib.sample.kb.drivers.omero',
                #-
                'bl.lib.individual',
                'bl.lib.individual.kb',
                'bl.lib.individual.kb.drivers',
                'bl.lib.individual.kb.drivers.omero',
                #-
                'bl.lib.genotype',
                'bl.lib.genotype.kb',
                'bl.lib.genotype.kb.drivers',
                'bl.lib.genotype.kb.drivers.omero',
                ],
      )
