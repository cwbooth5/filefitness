#!/usr/bin/env python

"""This will check .fit and .gpx files for integrity so they can be tested prior to
use/import into other programs. It looks for common issues, such as empty files, truncated
files,
"""

import argparse
from dataclasses import dataclass
import hashlib
import threading
import multiprocessing as mp
import concurrent.futures
import io
import logging

import sys
import os

from fitparse import FitFile
from fitparse.utils import FitCRCError, FitParseError, FitHeaderError, FitEOFError
import gpxpy

LOGLEVEL = 'DEBUG'
# LOGLEVEL = 'INFO'


LOG = logging.getLogger(__name__)
LOG.setLevel(LOGLEVEL)
HANDLER = logging.StreamHandler()
HANDLER.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
LOG.addHandler(HANDLER)


class ActivityDefective(Exception):
    """raised when a given file has some sort of problem rendering it unparsable by applications"""
    pass


@dataclass(unsafe_hash=True)  #TODO WAT
class Activity():
    """This encapsulates a ride file, regardless of the type of file."""
    name: str
    fileobj: io.BytesIO

    def extension(self) -> str:
        """Return the file extension sans period."""
        return self.name.split('.')[-1]

    def md5sum(self) -> str:
        """Compute an MD5sum of the string value of the entire file object.
        This is here to allow the file to be externally comparable to other versions/files.

        Hashability within this dataclass should be figured out with __hash__() eventually.
        """
        return hashlib.md5(self.fileobj.getvalue()).hexdigest()


def check_gpx(gpx_file):
    """GPX is just XML so this roughly sees if it parses.
    It also gathers an average power and heart rate because reading
    this data is an indicator the data is at least in the correct format.

    This only works with garmin-encoded GPX files right now.
    """
    try:
        gpx = gpxpy.parse(gpx_file.fileobj.getvalue().decode('utf-8'))
    except gpxpy.gpx.GPXXMLSyntaxException as err:
        raise ActivityDefective(f'{gpx_file.name} - {err}')

    power_readings = []
    hr_readings = []
    schema = '{http://www.garmin.com/xmlschemas/TrackPointExtension/v1}'
    try:
        for track in gpx.tracks:
            for segment in track.segments:
                for point in segment.points:
                    if 'ride' in gpx_file.name.lower():
                        # [print(x.attribute) for x in point.gpx_11_fields]
                        for extension in point.extensions:
                            if extension.tag == 'power':
                                power_readings.append(int(extension.text))
                            if extension.tag == f'{schema}TrackPointExtension':
                                for kid in list(extension):
                                    if kid.tag == f'{schema}hr':
                                        hr_readings.append(int(kid.text))

        power = round(sum(power_readings) / len(power_readings))
        hr = round(sum(hr_readings) / len(hr_readings))
        LOG.info(f'{gpx_file.name} Average Power: {power}, Average HR: {hr}')

    except ZeroDivisionError as err:
        # There just weren't any power or HR datapoints.
        pass


def check_fit(fit_file):
    """Look for blatant syntax errors, then read through each record in the
    .fit file to see if they can all be read properly.
    """
    try:
        fitfile = FitFile(fit_file.fileobj)
    except FitHeaderError:
        # Usually when the file is zero-length or truncated
        raise ActivityDefective(f'{fit_file.name} truncated')

    try:
        power_readings = []
        hr_readings = []
        for record in fitfile.get_messages('record'):
            for record_data in record:
                if record_data.name == 'power':
                    power_readings.append(record_data.value)
                elif record_data.name == 'heart_rate':
                    hr_readings.append(record_data.value)

        power = round(sum(power_readings) / len(power_readings))
        hr = round(sum(hr_readings) / len(hr_readings))
        LOG.info(f'{fit_file.name} Average Power: {power}, Average HR: {hr}')

    except (FitCRCError, FitParseError, FitHeaderError, FitEOFError) as err:
        raise ActivityDefective(err)



def integritycheck(activity):
    """Validate the integrity of an activity.
    The .fit and .gpx file formats are supported.
    """
    try:
        if activity.extension() == 'fit':
            check_fit(activity)
        elif activity.extension() == 'gpx':
            check_gpx(activity)
    except ActivityDefective as err:
        LOG.error(f'{activity.name} {err}')
        return False
    else:
        LOG.info(f'{activity.name} Integrity OK')
        return True


def main(targets):
    """I/O is the serial expensive operation.
    processing of groups of files can be done in parallel.
    We aren't going to try and guess a file by its contents. The extension needs
    to be correct.
    """
    streams = []
    LOG.debug(f'CPU count: {mp.cpu_count()}')
    pool = mp.Pool(mp.cpu_count())

    for file in targets:
        # ignore anything lacking the extensions we can't work with.
        if not any([file.lower().endswith('.fit'), file.lower().endswith('.gpx')]):
            # if not file.lower().endswith('.gpx'):
            LOG.debug(f'File skipped: {file}')
            continue
        # TODO: bite off smaller chunks so we aren't linearly eating memory here.
        # read the files into memory
        with open(file, 'rb') as f:
            g = io.BytesIO(f.read())
            streams.append(Activity(name=file, fileobj=g))

    # result = pool.map(integritycheck, streams)
    # print(result)
    for x in streams:
        integritycheck(x)



if __name__ == '__main__':
    # Allow parsing of a single file or a directory of files.
    arg = sys.argv[1:]
    files = []
    try:
        for a in arg:  # accept multiple arguments.
            for entry in os.scandir(a):
                files.append(entry.path)
    except NotADirectoryError:
        files = arg

    # print(f'{len(files)} files were found for processing.')
    LOG.info(f'{len(files)} files were found for processing.')
    main(targets=files)
