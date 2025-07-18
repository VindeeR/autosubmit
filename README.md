[![codecov](https://codecov.io/gh/BSC-ES/autosubmit/graph/badge.svg?token=WEDBP1A6LG)](https://codecov.io/gh/BSC-ES/autosubmit)
[![DOI](https://zenodo.org/badge/902494505.svg)](https://zenodo.org/badge/latestdoi/902494505)

Autosubmit is a lightweight workflow manager designed to meet climate research necessities. Unlike other workflow solutions in the domain, it integrates the capabilities of an experiment manager, workflow orchestrator and monitor in a self-contained application. The experiment manager allows for defining and configuring experiments, supported by a hierarchical database that ensures reproducibility and traceability. The orchestrator is designed to run complex workflows in research and operational mode by managing their dependencies and interfacing with local and remote hosts. These multi-scale workflows can involve from a few to thousands of steps and from one to multiple platforms.

Autosubmit facilitates easy and fast integration and relocation on new platforms. On the one hand, users can rapidly execute general scripts and progressively parametrize them by reading Autosubmit variables. On the other hand, it is a self-contained desktop application capable of submitting jobs to remote platforms without any external deployment.

Due to its robustness, it can handle different eventualities, such as networking or I/O errors. Finally, the monitoring capabilities extend beyond the desktop application through a REST API that allows communication with workflow monitoring tools such as the Autosubmit web GUI. 

Autosubmit is a Python package provided in PyPI. Conda recipes can also be found on the website. A containerized version for testing purposes is also available but not public yet.

It has contributed to various European research projects and runs different operational systems. During the following years, it will support some of the Earth Digital Twins as the Digital Twin Ocean.

Concretely, it is currently used at Barcelona Supercomputing Centre (BSC) to run models (EC-Earth, MONARCH, NEMO, CALIOPE, HERMES...), operational toolchains (S2S4E), data-download workflows (ECMWF MARS), and many other. Autosubmit has run these workflows in different supercomputers in BSC, ECMWF, IC3, CESGA, EPCC, PDC, and OLCF.

Get involved or contact us:

    Autosubmit GitHub:	        https://github.com/BSC-ES/autosubmit
    Autosubmit Support:	        support-autosubmit@bsc.es
    
How to cite Autosubmit:

    D. Manubens-Gil, J. Vegas-Regidor, C. Prodhomme, O. Mula-Valls and F. J. Doblas-Reyes,
    "Seamless management of ensemble climate prediction  experiments on HPC platforms," 
    2016 International Conference on High Performance Computing & Simulation (HPCS), 
    Innsbruck, 2016, pp. 895-900. doi: 10.1109/HPCSim.2016.7568429

AUTOSUBMIT IS MAINTAINED ON THE FOLLOWING MACHINES
==================================================

- bscesautosubmit{01/02}.bsc.es -- Autosubmit 3, BSC virtual machine
- bsceshub{02/03/04}.bsc.es -- Autosubmit 3 and Autosubmit 4, BSC virtual machine
- climate-dt -- Autosubmit 4, CSC virtual machine
- EDITO-infra -- Autosubmit 4, MOi and VLIZ cloud infrastructure (Docker, Kubernetes)
- AEMET -- Autosubmit 3 and Autosubmit 4, AEMET virtual machine

HOW TO DEPLOY/SETUP AUTOSUBMIT FRAMEWORK
========================================

- Autosubmit has been tested:

  with the following Operating Systems:
   * Linux Debian
   * Linux OpenSUSE
   * Ubuntu
   * WSL (Windows Subsystem for Linux)
   * MacOS* Not directly by Autosubmit team, but by users

  on the following HPC platforms:
   * Cirrus (AEMET machine)
   * MareNostrum (BSC machine)
   * MareNostrum3 (BSC machine)
   * MareNostrum4 (BSC machine)
   * MareNostrum5 (BSC machine)
   * Lumi (CSC machine)
   * Levante (DKRZ machine)
   * ATOS (ECMWF machine)
   * C2A (ECMWF machine)
   * CCA (ECMWF machine)
   * ARCHER (EPCC machine)
   * HECToR (EPCC machine)
   * Ithaca (IC3 machine)
   * Stargate (IC3 machine)
   * Juwels (Jülich machine)
   * MeluXina (LXP machine)
   * Lindgren (PDC machine)
   * Fugaku (RIKEN machine)

- Pre-requisites: These packages (bash, python2, sqlite3, git-scm > 1.8.2, subversion, pip >= 24.0, dialog*) must be available at local
  machine. These packages (argparse, dateutil, pyparsing, numpy, pydotplus, matplotlib, paramiko,
  python2-pythondialog*, mock, portalocker) must be available for python runtime. And the machine is also able to access
  HPC platforms via password-less ssh.
  Pip must be available with version `>= 24.0`.

  *: optional

- Install Autosubmit
   > pip install autosubmit
  or download, unpack, move to the folder and "pip install ."

- Create a repository for experiments: Say for example "/cfu/autosubmit" then "autosubmit configure" and follow
  instructions

- Create a blank database: "autosubmit install"

HOW TO USE AUTOSUBMIT
=====================
> autosubmit -h

> autosubmit expid --HPC ithaca --description "experiment is about..."

Say for example, "cxxx" is 4 character based expid generated by system automatically.
First character is a letter, the other three alfa-numeric characters allow to identify uniquely the experiment.

> vi /cfu/autosubmit/cxxx/conf/expdef_cxxx.yml

> vi /cfu/autosubmit/cxxx/conf/autosubmit_cxxx.yml

> vi /cfu/autosubmit/cxxx/conf/platforms_cxxx.yml

> vi /cfu/autosubmit/cxxx/conf/jobs_cxxx.yml

> autosubmit create cxxx

> nohup autosubmit run cxxx &

Cautions: 
- Before launching autosubmit check the following stuff:
> ssh marenostrum5 # (for example) check other HPC platforms where password-less ssh is feasible.
- After launching autosubmit, one must be aware of login expiry limit and policy (if applicable for any HPC)
and renew the login access accordingly (by using token/key etc) before expiry.

HOW TO MONITOR EXPERIMENT
=========================

> autosubmit monitor -h

> autosubmit monitor cxxx
or
> autosubmit monitor cxxx -o png

Above generated plot with date & time stamp can be found at:

/cfu/autosubmit/cxxx/plot/cxxx_date_time.pdf
or 
/cfu/autosubmit/cxxx/plot/cxxx_date_time.png


HOW TO RESTART EXPERIMENT
=========================

> autosubmit recovery -h

> autosubmit recovery  cxxx -s # saving the pickle file

> nohup autosubmit run cxxx &


FULL DOCUMENTATION AND HOW TOs
==============================

Check the Autosubmit documentation provided in the docs/ folder of the package, in PDF format.
Check the online documentation in the following web page: http://www.bsc.es/projects/earthscience/autosubmit/
