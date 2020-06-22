#!/usr/bin/env python

import os, sys
import argparse
from collections import OrderedDict
from pprint import pprint

def space(len):
    return "   "*len

class Step(OrderedDict): # Is this a good idea? I do not know, but let us try anyway.
    def setSkipped(self, skipped):
        self.skipped = skipped
    
class Job(object): # Is this a good idea? I do not know, but let us try anyway.
    def __init__(self):
        self.skipped = "Unknown"
        self.input_files = []
        self.output_files = []
        self.dependency_jobs = []
    def setSkipped(self, skipped):
        self.skipped = skipped
    
def parseLog(in_path):
    with open(in_path, 'r') as handle:
        lines = handle.readlines()
        lines = [line.rstrip('\n') for line in lines] # remove newlines
#        lines = [line for line in lines if (not line == "")]  # remove empty lines

    steps = OrderedDict()
    for ii, line in enumerate(lines):
        if "INFO:core.pipeline:Create jobs for step " in line:
            step_name = line.split("INFO:core.pipeline:Create jobs for step ")[1].rstrip("...")
            steps[step_name] = Step()
            
            for jj, jj_line in enumerate(lines[ii+1:], ii+1):
                if "INFO:core.pipeline:Step" in jj_line: # This is the end of the step, where it says, e.g. "INFO:core.pipeline:Step picard_calculate_hs_metrics: 31 jobs created" or "INFO:core.pipeline:Step gatk_callable_loci: 0 job created... skipping"
                    if "... skipping" in jj_line:
                        steps[step_name].setSkipped(True)
                    else:
                        steps[step_name].setSkipped(False)
                    break
                elif "Job name: " in jj_line:
                    job_name = jj_line.split("Job name: ")[1]
                    steps[step_name][job_name] = Job()
                    for kk, kk_line in enumerate(lines[jj+1:], jj+1):
                        if ("DEBUG:core.pipeline:Job input files:" in kk_line):
                            for ll, ll_line in enumerate(lines[kk+1:], kk+1):
                                if ("DEBUG:core.pipeline:Job output files:" in ll_line) or (ll_line == ""): # if at end of list of input files
                                    break
                                else:
                                    input_file = ll_line.lstrip()
                                    steps[step_name][job_name].input_files.append(input_file)
                        elif ("DEBUG:core.pipeline:Job output files:" in kk_line):
                            for ll, ll_line in enumerate(lines[kk+1:], kk+1):
                                if (ll_line == ""): # if at end of list of input files
                                    break
                                else:
                                    output_file = ll_line.lstrip()
                                    steps[step_name][job_name].output_files.append(output_file)
                        elif (job_name in kk_line) and ("up to date" in kk_line):
                            if ("... skipping" in kk_line):
                                steps[step_name][job_name].skipped = True
                            else:
                                steps[step_name][job_name].skipped = False
                        elif ("DEBUG:core.job:Dependency jobs:" in kk_line):
                            for ll, ll_line in enumerate(lines[kk+1:], kk+1):
                                if (ll_line == ""): # if at end of list of input files
                                    break
                                else:
                                    dependency_job = ll_line.lstrip()
                                    steps[step_name][job_name].dependency_jobs.append(dependency_job)
                        elif ("DEBUG:core.pipeline:Job name: " in kk_line) or ("INFO:core.pipeline:Step " in kk_line): # if at end of job (found next job or next step)
                            break
                    
    return steps

def printLog(steps, hide_skipped=False, hide_jobs=False, hide_input=False, hide_output=False, hide_dependency_jobs=False, example_job=False):
    for step_name, step in steps.iteritems():
        if hide_skipped and step.skipped:
            continue
        print step_name + (" [SKIPPED]" if step.skipped else "")
        if (not hide_jobs):
            if example_job:
                job_types_printed = []
            for job_name, job in step.iteritems():
                if hide_skipped and job.skipped:
                    continue
                if example_job:
                    job_type = job_name.split(".")[0]
                    if (job_type in job_types_printed):
                        continue
                    else:
                        job_types_printed.append(job_type)
                print space(1) + job_name + (" [SKIPPED]" if job.skipped else "")
                if (not hide_input):
                    if len(job.input_files) > 0:
                        print space(2)+"Input:"
                        for input_file in job.input_files:
                            print space(3)+str(input_file)
                if (not hide_output):
                    if len(job.output_files) > 0:
                        print space(2)+"Output:"
                        for output_file in job.output_files:
                            print space(3)+str(output_file)
                if (not hide_dependency_jobs):
                    if len(job.dependency_jobs) > 0:
                        print space(2)+"Dependency jobs:"
                        for dependency_job in job.dependency_jobs:
                            print space(3)+str(dependency_job)

def readLog(in_path, hide_skipped=False, hide_jobs=False, hide_input=False, hide_output=False, hide_dependency_jobs=False, example_job=False):
    # Parse the log file.
    steps = parseLog(in_path)

    # Print the parsed log file.
    printLog(steps, hide_skipped, hide_jobs, hide_input, hide_output, hide_dependency_jobs, example_job)
    

if (__name__ == '__main__'):
    # Create argument parser
    description = """Description of function"""
    epilog = '' # """Text to follow argument explantion """
    parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawTextHelpFormatter)
    
    # Define positional arguments.
    parser.add_argument("in_path", help="path log file to be read.")
    
    # Define optional arguments.
    parser.add_argument("-s", "--hide_skipped", action="store_true", help="hide skipped steps and jobs")
    parser.add_argument("-j", "--hide_jobs", action="store_true", help="hide jobs")
    parser.add_argument("-i", "--hide_input", action="store_true", help="hide input files")
    parser.add_argument("-o", "--hide_output", action="store_true", help="hide output files")
    parser.add_argument("-d", "--hide_dependency_jobs", action="store_true", help="hide dependency jobs")
    parser.add_argument("-e", "--example_job", action="store_true", help="if a job is performed many times in a step, only show one example of that job")
    
    # Parse arguments.
    args = parser.parse_args()

    # Do stuff.
    readLog(args.in_path, hide_skipped=args.hide_skipped, hide_jobs=args.hide_jobs, hide_input=args.hide_input, hide_output=args.hide_output, hide_dependency_jobs=args.hide_dependency_jobs, example_job=args.example_job)
