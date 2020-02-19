#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# autoExport.py
#
# This program is intended to be used to create csv data files which are
# suitable for export and data analysis.  It uses ftArchPostProc to process
# data files from SQL server and turn them into the resulting csv files.
#
# In the below description, the "raw" file is the source data file for ftArchPostProc,
# and it is created by SQL server.  SQL server will create these files and place
# them in the "unprocessed" folder as an indication that they need to be processed.
#
# Once ftArchPostProc runs, the exportable file is placed in the destination path,
# and the source file is moved from the unprocessed folder to the processed folder.
#
# **** Program operation
#   1)  The program will read in a config file. The file is either specified by
#       the -c/--configFile command line argument, or defaults to "autoExportconfig.ini"
#       if not specified.  The config file specifies:
#           a. The raw file unprocessed path
#           b. The raw file processed path
#           c. The destination path of the created file
#           d. The file name parameters of the created file (prefix, data code, suffix)
#           e. Command line arguments to use when calling ftArchPostProc
#           f. Which script file to use for ftArchPostProc
#           g. Which python binary to use when running ftArchPostProc
#
# The script using this file will assume this stucture:
# [rawFiles]
#     unprocessedPath=/.../...   Source (raw) file path
#     processedPath=/.../...     Where to put the source file when done
# [exportFile]
#    destinationPath=/.../...    Destination file path (the exportable file)
#    fileNamePrefix=...
#    fileNameSuffix=...
# [ftArchPostProc]
#    clArgs=...                  Command line arguments. Ignored if -a/--clArgs cmd line arg is specified.
#    scrptPath=/.../...          Which ftPostProc to run
#    python_bin=/.../...         Which python to run
#
#   2) The command line argument of -a/--clArgs is used to pass through arguments to ftArchPostProc.
#      Note that if the -a argument is used, the config file section, clArgs, is ignored.
#
#   3) If there are any *.csv files in the unprocessed folder, this program will
#      run ftArchPostProc on each *.csv file found, and then move the source
#      file to the procesed folder.  If there are no *.csv files in the
#      unprocessed folder, this program does nothing.
#
# **** Command line arguments
#   -a/--clArgs         command line arguments to pass through to ftArchPostProc. Takes priority
#                       over clArgs in the config file. The later is ignored if this argument is specified.
#
#   -c/--configFile     which config file to use. Looks for and uses autExportconfig.ini in the same path
#                       as the executable if none is specified.
#
#   -ce, --configEncoding (default 'UTF-8')
#
#   -v/--verbose        (default False). Provide diagnostic output. Otherwise, no output
#                       is provided, other than the resulting file (good for running from scrips or
#                       automation.)
#
# imports
#
# date and time stuff
from datetime import datetime, time

# os file related
# join combines path stirngs in a smart way (i.e. will insert '/' if missing,
# or remove a '/' if a join creates a repeat).
from os.path import join
from shutil import move # file rename/move

# allow subprocess to be used for ftpp
import subprocess

# regex stuff
import re

# config file parser
import configparser

# arg parser
import argparse

#shell lexical parser -- used to proces command line arguments from the config file
import shlex


# user libraries
# Note: May need PYTHONPATH (set in ~/.profile?) to be set depending
# on the location of the imported files
from bpsFile import listFiles
from bpsString import trimPrefixSuffix

# **** argument parsing
# define the arguments
# create an epilog string to further describe the input file
# TODO: Update INI Desc
eplStr="""Python program which uses ftArchPostProc to create csv files for
export and analysis. A configuration (ini) file is used to specify configuration
parameters, and this program facilitates the automated creation of these
files."""

descrStr="""Python program which uses ftArchPostProc to create csv files for
export and analysis. A configuration (ini) file is used to specify configuration
parameters, and this program facilitates the automated creation of these
files.

The souce data is normally a csv file from a database query or
automated job. A source (rawFiles >> unporcessedPath) is specified in the
configuraiton file, and any csv file in that path will be processed when this
program is run.



The program will read in a config file. The file is either specified by
the -c/--configFile command line argument, or defaults to "autoExportconfig.ini"
if not specified.  The config file specifies:
    a. The raw file unprocessed path
    b. The raw file processed path
    c. The destination path of the created file
    d. The file name parameters of the created file (prefix, data code, suffix)
    e. Command line arguments to use when calling ftArchPostProc
    f. Which script file to use for ftArchPostProc
    g. Which python binary to use when running ftArchPostProc

 The script using this file will assume this stucture:
 [rawFiles]
     unprocessedPath=/.../...   Source (raw) file path
     processedPath=/.../...     Where to put the source file when done
 [exportFile]
     destinationPath=/.../...    Destination file path (the exportable file)
     fileNamePrefix=...
     fileNameSuffix=...
  [ftArchPostProc]
     clArgs=...                  Command line arguments. Ignored if -a/--clArgs is specified
     scrptPath=/.../...          Which ftPostProc to run
     python_bin=/.../...         Which python to run

 If there are any *.csv files in the unprocessed folder, this program will
 run ftArchPostProc on each *.csv file found, and then move the source
 file to the procesed folder.  If there are no *.csv files in the
 unprocessed folder, this program does nothing.

Regarding file names:
    The source file is expected to have a file naming convention:
        prefix_datecode_suffix

Command line arguments are:
 -a/--clArgs            command line arguments to pass through to ftArchPostProc. Takes priority
                        over clArgs in the config file. The later is ignored if this argument is specified.

 -c/--configFile        (default 'autoExportConfig.ini')

 -ce/--configEncoding   (default 'UTF-8')


  -v/--verbose          (default False). Provide diagnostic output. Otherwise, no output
                        is provided, other than the resulting file (good for running from scrips or
                        automation.)

"""

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
                                 description=descrStr, epilog=eplStr)
parser.add_argument('-a', '--clArgs', default=None, metavar='', \
                   help='Command line arguments to pass through to ftArchPostProc.')
parser.add_argument('-c', '--configFile', default='autoExportConfig.ini', metavar='', \
                   help='Config file. Default is autoExportConfig.ini. A list of files \
may be specified ([\'file1.ini\',\'file2.ini\',...]) if the configuration is \
spread across multiple files. The resuling configuration will be a union of the \
information, so if keys are repeated, the key will have the value it had in the \
last file read.')
parser.add_argument('-ce', '--configEncoding', default='UTF-8', metavar='', \
                   help='Config file encoding. Default is UTF-8.')
parser.add_argument('-v', '--verbose', action='store_true', default=False, \
                    help='Increase output messages.')
# parse the arguments
args = parser.parse_args()

# At this point, the arguments will be:
# Argument              Values   Description
# args.clArgs           string   Optionsl. Default None
# args.configFile       string   Optional. Default 'autoExportConfig.ini'
# args.configEncoding   string   Optional. Default 'UTF-8'
# args.verbose          True/False Increase output messaging
# Put the begin mark here, after the arg parsing, so argument problems are
# reported first.

if args.verbose:
    print('**** Begin Processing ****')
    # get start processing time
    procStart = datetime.now()
    print('    Process start time: ' + procStart.strftime('%m/%d/%Y %H:%M:%S'))

# **** Get config info from config file
# bring in config data from config ini by default or from file specified
# with -c argument
config = configparser.ConfigParser()
cfgFile = config.read(args.configFile, args.configEncoding)
# bail out if no config file was read
if not cfgFile:
    if args.verbose:
        print('ERROR: The configuration file: ' + args.configFile + ' was not found. Exiting.')
    quit()
# if we get here, we have config data
if args.verbose:
    print('\nThe following config file(s) are used:')
    print(cfgFile)
    print('The resulting configuration has these settings:')
    for section in config:
        print(section)
        for option in config[section]:
            print('  ', option, ':', config[section][option])

# Get the stuff from the rawFiles section.
# Make sure config sections and options are present.
# Make sure the specified values are not empty strings or null.
if config.has_section('rawFiles'):
    # rawFiles section is present
    # unprocessed path
    if config.has_option('rawFiles', 'unprocessedPath'):
        unprocessedPath = config['rawFiles']['unprocessedPath']
        if unprocessedPath is None or unprocessedPath == '':
            if args.verbose:
                print('Configuration error: \'unprocessedPath\' option in the \
\'rawFiles\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
        else:
            # use os.path.join to add trailing slash if needed
            unprocessedPath = join(unprocessedPath, '')
    else:
        if args.verbose:
            print('Configuration error: No \'unprocessedPath\' option in the \
\'rawFiles\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # processed path
    if config.has_option('rawFiles', 'processedPath'):
        processedPath =config['rawFiles']['processedPath']
        if processedPath is None or processedPath == '':
            if args.verbose:
                print('Configuration error: \'processedPath\' option in the \
\'rawFiles\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
        else:
            # use os.path.join to add trailing slash if needed
            processedPath = join(processedPath, '')
    else:
        if args.verbose:
            print('Configuration error: No \'processedPath\' option in the \
\'rawFiles\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # raw file name prefix
    if config.has_option('rawFiles', 'fileNamePrefix'):
        rawFileNamePrefix = config['rawFiles']['fileNamePrefix']
        if rawFileNamePrefix is None or rawFileNamePrefix == '':
            if args.verbose:
                print('Configuration error: \'rawFileNamePrefix\' option in the \
\'rawFiles\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'"fileNamePrefix\' option in the \
\'rawFiles\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # raw file name suffix
    if config.has_option('rawFiles', 'fileNameSuffix'):
        rawFileNameSuffix = config['rawFiles']['fileNameSuffix']
        if rawFileNamePrefix is None or rawFileNamePrefix == '':
            if args.verbose:
                print('Configuration error: \'rawFileNameSuffix\' option in the \
\'rawFiles\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'"fileNameSuffix\' option in the \
\'rawFiles\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
else:
    # rawFiles section is not present
    if args.verbose:
        print('Configuration error: No \'rawFiles\' section in the configuration \
file: \'' + args.configFile + '\'.')
    quit()

# Get the stuff from the exportFile section.
# Make sure config sections and options are present.
# Make sure the specified values are not empty strings or null.
if config.has_section('exportFile'):
    # exportFile section is present
    # destinationPath
    if config.has_option('exportFile', 'destinationPath'):
        destinationPath = config['exportFile']['destinationPath']
        if destinationPath is None or destinationPath == '':
            if args.verbose:
                print('Configuration error: \'destinationPath\' option in the \
\'exportFile\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
        else:
            # use os.path.join to add trailing slash if needed
            destinationPath = join(destinationPath, '')
    else:
        if args.verbose:
            print('Configuration error: No \'destinationPath\' option in the \
\'exportFile\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # raw file name prefix
    if config.has_option('exportFile', 'fileNamePrefix'):
        exportFileNamePrefix = config['exportFile']['fileNamePrefix']
        if exportFileNamePrefix is None or exportFileNamePrefix == '':
            if args.verbose:
                print('Configuration error: \'fileNamePrefix\' option in the \
\'exportFile\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'"fileNamePrefix\' option in the \
\'exportFile\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # raw file name suffix
    if config.has_option('exportFile', 'fileNameSuffix'):
        exportFileNameSuffix = config['exportFile']['fileNameSuffix']
        if exportFileNameSuffix is None or exportFileNameSuffix == '':
            if args.verbose:
                print('Configuration error: \'fileNameSuffix\' option in the \
\'exportFile\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'"fileNameSuffix\' option in the \
\'exportFile\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
else:
    # rawFiles section is not present
    if args.verbose:
        print('Configuration error: No \'exportFile\' section in the configuration \
file: \'' + args.configFile + '\'.')
    quit()

# Get the stuff from the ftArchPostProc section.
# Make sure config sections and options are present.
# Make sure the specified values are not empty strings or null.
if config.has_section('ftArchPostProc'):
    # ftArchPostProc section is present
    # ftpp clArgs -- command line arguments
    if config.has_option('ftArchPostProc', 'clArgs') and args.clArgs is not None and args.clArgs != '':
        if args.verbose:
            print('Note: The \'clArgs\' option in the \
\'ftArchPostProc\' section was found, but will be ignored becuase the command line argument \
-a/--clArgs was specified. Configuration file: \'' + args.configFile + '\'.')
        ftppClArgs = args.clArgs
    elif config.has_option('ftArchPostProc', 'clArgs') and (args.clArgs is None or args.clArgs == ''):
        argStr = config['ftArchPostProc']['clArgs']
        if (argStr is None or argStr == ''): 
            if args.verbose:
                print('Configuration error: \'clArgs\' option in the \
\'ftArchPostProc\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
        else:
            # argStr config section is present, and option has something in it,
            # and args.clArgs is not specified (None or empty string)
            # Use shlex to parse the config info like the shell would
            ftppClArgs = shlex.split(argStr)
    elif not config.has_option('ftArchPostProc', 'clArgs') and args.clArgs is not None and args.clArgs != '':
        if args.verbose:
            print('The command line argument -a/--clArgs was specified: \'' + args.clArgs + '\'.')
        ftppClArgs = args.clArgs
    else :
        if args.verbose:
            print('Configuration error: No arguments specified with the -a/--clArgs command line \
option, and there is no  \'clArgs\' option in the \'ftArchPostProc\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # ftpp script
    if config.has_option('ftArchPostProc', 'script'):
        ftppExecPath = config['ftArchPostProc']['script']
        if ftppExecPath is None or ftppExecPath == '':
            if args.verbose:
                print('Configuration error: \'script\' option in the \
\'ftArchPostProc\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'script\' option in the \
\'ftArchPostProc\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
    # ftpp python_bin
    if config.has_option('ftArchPostProc', 'python_bin'):
        ftppPythonBin = config['ftArchPostProc']['python_bin']
        if ftppPythonBin is None or ftppPythonBin == '':
            if args.verbose:
                print('Configuration error: \'python_bin\' option in the \
\'ftArchPostProc\' section must contain a value. Configuration file: \'' + args.configFile + '\'.')
            quit()
    else:
        if args.verbose:
            print('Configuration error: No \'python_bin\' option in the \
\'ftArchPostProc\' section in the configuration file: \'' + args.configFile + '\'.')
        quit()
else:
    # rawFiles section is not present
    if args.verbose:
        print('Configuration error: No \'ftArchPostProc\' section in the configuration \
file: \'' + args.configFile + '\'.')
    quit()

# print the stuff from the config file if in verbose mode
if args.verbose:
    print('The following was retrieved from the configuration file \'' + args.configFile + '\':')
    print('[rawFiles] unprocessedPath: ' + unprocessedPath)
    print('[rawFiles] processedPath: ' + processedPath)
    print('[rawFiles] fileNamePrefix: ' + rawFileNamePrefix)
    print('[rawFiles] fileNameSuffix: ' + rawFileNameSuffix)
    print('[exportFile] destinationPath: ' + destinationPath)
    print('[exportFile] fileNamePrefix: ' + exportFileNamePrefix)
    print('[exportFile] fileNameSuffix: ' + exportFileNameSuffix)
    print('[ftArchPostProc] clArgs (command line arguments): ' + argStr)
    print('[ftArchPostProc] script: ' + ftppExecPath)
    print('[ftArchPostProc] python_bin: ' + ftppPythonBin)

# **** Check the unprocessed path for files which match the naming pattern:
#     prefix + 0 or more characters or spaces + suffix
# If there are files, get on with processing them. If no files matching then
# pattern are found, quit.
# Build regular expression.'[\w, ]*' means: Any alpha numeric character or zero or more times
pattern = rawFileNamePrefix + '[\w, ]*' + rawFileNameSuffix
for filename in listFiles(unprocessedPath):
    if re.search(pattern, filename):
        # File that needs processing has been found.
        # Strip off raw prefix and suffix so the rest can be used for
        # the destination file.
        coreName = trimPrefixSuffix(filename, rawFileNamePrefix, rawFileNameSuffix)
        # now construct the destination file name from the export prefix + core + suffix
        destFileName = exportFileNamePrefix + coreName + exportFileNameSuffix
        # Run ftpp in its virtual environment as a subprocess
        # First, create a list of arguments, and then call it
        # Besides the input and output file names, the command line arguments come
        # from the configuraiton file, and were pulled out into ftppClArgs above.
        # The Popen subprocess call expects a command as a list with the command first
        # followed by arguments, which in this case has the following pattern:
        # python3 ftPostProc.py arg1, arg2 ...
        ftppArgs = []
        ftppArgs.append(ftppPythonBin)
        ftppArgs.append(ftppExecPath)
        # append the command line arguments from the configuraiton file as list elements.
        ftppArgs.extend(ftppClArgs)
        # Append the input and output file names. Note these are positional arguments in ftpp,
        # so the order is important.
        ftppArgs.append(unprocessedPath + filename)
        ftppArgs.append(destinationPath + destFileName)
        try:
            # run ftpp in a sub-process. communicate() runs the subprocess sequentially (synchronously)
            subprocess.Popen(ftppArgs).communicate()
            # if we get here, ftpp rann. Move the raw file from the unprocessed folder to the processed folder.
            move(unprocessedPath + filename, processedPath + filename)
        except:
            pass # move along to the next file

if args.verbose:
    # get end processing time
    procEnd = datetime.now()
    print('\n**** End Processing ****')
    print('    Process end time: ' + procEnd.strftime('%m/%d/%Y %H:%M:%S'))
    print('    Duration: ' + str(procEnd - procStart) + '\n')

