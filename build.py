"""
  Copyright (C) QuaintScience Technologies Pvt. Ltd - All Rights Reserved
 
  Unauthorized copying of any code in this repository, via any medium is strictly prohibited
  
  Proprietary and confidential.

  This source code is protected under international copyright law.  All rights
  reserved and protected by the copyright holders.

  This file is confidential and only available to authorized individuals with the 
  permission of the copyright holders.  If you encounter this file and do not have
  permission, please contact the copyright holders and delete this file.
  
  Written by Gokul Thattaguppa Chittaranjan <gokul@quaintscience.com>, 01-11-2023
 """

import os
import json
from pybuilder.core import (init,
                            use_plugin,
                            Author)

use_plugin("python.core")
use_plugin("python.flake8")
use_plugin("python.pylint")
use_plugin("python.unittest")
# use_plugin("python.coverage")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")


@init
def initialize(project):
    """Initialize Project"""

    package_dir = project.get_property("package")
    project.basedir = os.path.abspath(package_dir)
    project.set_property("basedir", os.path.join(package_dir))
    # project.set_property("dir_source_unittest_python", os.path.join(package_dir, "src", "unittest", "python"))

    major_version = None
    with open(os.path.join(package_dir,
                           "PACKAGE.version"), "r", encoding='utf-8') as fid:
        major_version = fid.read().strip()

    minor_version = os.environ.get("BITBUCKET_BUILD_NUMBER", "dev")
    version = f"{major_version}.{minor_version}"

    version_filepath = os.path.join(package_dir, "VERSION")
    if os.path.exists(version_filepath):
        with open(version_filepath, "r", encoding='utf-8') as fid:
            version = fid.read().strip()

    package_name = None
    with open(os.path.join(package_dir,
                           "PACKAGE.name"), "r", encoding='utf-8') as fid:
        package_name = fid.read().strip()

    package_details = None
    with open(os.path.join(package_dir,
                           "PACKAGE.details"), "r", encoding='utf-8') as fid:
        package_details = json.load(fid)

    authors = [Author(*args)
               for args in package_details["authors"]]

    description = package_details["description"]

    lic = None
    with open(os.path.join(package_dir,
                           "PACKAGE.license"), "r", encoding='utf-8') as fid:
        lic = fid.read().strip()

    project.name = package_name
    project.version = version
    project.authors = authors
    project.description = description
    project.license = lic
    project.summary = package_details["summary"]
    project.home_page = package_details["home_page"]
    project.build_depends_on("mockito")
    project.set_property("coverage_break_build", False)

    project.depends_on_requirements(os.path.join(package_dir,
                                                 "requirements.txt"))
    includes_file = os.path.join(package_dir, "PACKAGE.includes")  
    if os.path.exists(includes_file):
        with open(includes_file, "r", encoding='utf-8') as fid:
            includes = json.load(fid) 
            for include in includes:
                project.include_file(include[0], include[1])
    project.set_property("dir_dist_scripts", ".")
    # project.set_property("dir_source_unittest_python", os.path.join(package_dir, "src"))
    project.set_property("dir_dist", f"$dir_target/dist/{project.name}-{project.version}")
    project.set_property("run_unit_tests_propagate_stdout", True)
    project.set_property("run_unit_tests_propagate_stderr", True)


default_task = ["install_dependencies", "analyze", "publish"]
