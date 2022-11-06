#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov  5 14:29:48 2022

@author: xieruoting
"""
from setuptools import setup, find_packages

 
classifiers = [
  'Development Status :: 5 - Production/Stable',
  'Intended Audience :: Education',
  'Operating System :: Microsoft :: Windows :: Windows 10',
  'License :: OSI Approved :: MIT License',
  'Programming Language :: Python :: 3'
]
 
setup(
  name='hw1mainfunction',
  version='0.0.1',
  description='main function from Max to call polygon api',
  long_description=open('README.txt').read() + '\n\n' + open('CHANGELOG.txt').read(),
  url='',  
  author='Ruoting Xie',
  author_email='xrt200510@163.com',
  license='MIT', 
  classifiers=classifiers,
  keywords='mainfunction', 
  packages=find_packages(),
  install_requires=['datetime','time','polygon','sqlalchemy','pandas','math','matplotlib.pyplot','numpy'] 
)
