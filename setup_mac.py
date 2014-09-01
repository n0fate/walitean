# -*- coding: utf-8 -*-
from setuptools import setup

# name, description, version
setup(name="iWalitean",
      description="WAL Analyzer for SQLite by n0fate",
      version="0.1.0",
      # dependency
      setup_requires=["py2app"],
      app=["walgui.py"],
      options={
          "py2app": {
              'iconfile':'images/iWalitean.icns',
              'plist': {'CFBundleShortVersionString':'0.1.0', 'NSHumanReadableCopyright':'Copyright by n0fate'},
              "argv_emulation": True
          }
      })