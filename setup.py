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
                'bl.lib.genotype',
                ],
      )
