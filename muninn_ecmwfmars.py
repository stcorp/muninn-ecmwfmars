from __future__ import absolute_import, division, print_function
import contextlib
import datetime
import struct
import logging
import os

from muninn.schema import Mapping, Text, Integer
from muninn.struct import Struct
from muninn.exceptions import Error
from muninn.archive import Archive
from muninn.remote import RemoteBackend
from muninn import util


# This namespace contains mars request paremeters
class ECMWFMARSNamespace(Mapping):
    # MARS abbreviation from http://apps.ecmwf.int/codes/grib/format/mars/class/
    marsclass = Text(index=True)
    # MARS abbreviation from http://apps.ecmwf.int/codes/grib/format/mars/stream/
    stream = Text(index=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Identification+keywords#Identificationkeywords-expver
    expver = Text(index=True)
    # MARS abbreviation from http://apps.ecmwf.int/codes/grib/format/mars/type/
    type = Text(index=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Date+and+time+keywords#Dateandtimekeywords-date
    date = Text(index=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Date+and+time+keywords#Dateandtimekeywords-time
    time = Text(index=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Date+and+time+keywords#Dateandtimekeywords-step
    step = Integer(optional=True, index=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Post-processing+keywords#Post-processingkeywords-resol
    resol = Text(optional=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Post-processing+keywords#Post-processingkeywords-grid
    grid = Text(optional=True)
    # https://software.ecmwf.int/wiki/display/UDOC/Post-processing+keywords#Post-processingkeywords-area
    area = Text(optional=True)


NAMESPACES = {
    'ecmwfmars': ECMWFMARSNamespace
}


def namespaces():
    return NAMESPACES.keys()


def namespace(name):
    return NAMESPACES[name]


MARSCLASSES = {
    1: "od",
    2: "rd",
    3: "er",
    4: "cs",
    5: "e4",
    6: "dm",
    7: "pv",
    8: "el",
    9: "to",
    10: "co",
    11: "en",
    12: "ti",
    13: "me",
    14: "ei",
    15: "sr",
    16: "dt",
    17: "la",
    18: "yt",
    19: "mc",
    20: "pe",
    21: "em",
    22: "e2",
    23: "ea",
    24: "ep",
    25: "rm",
    26: "nr",
    27: "s2",
    28: "j5",
    29: "ur",
    30: "et",
    31: "c3",
    32: "yp",
    99: "te",
    100: "at",
    101: "be",
    102: "hr",
    103: "dk",
    104: "fi",
    105: "fr",
    106: "de",
    107: "gr",
    108: "hu",
    109: "is",
    110: "ie",
    111: "it",
    112: "nl",
    113: "no",
    114: "pt",
    115: "si",
    116: "es",
    117: "se",
    118: "ch",
    119: "tr",
    120: "uk",
    121: "ms",
    199: "ma",
}

MARSSTREAMS = {
    1022: "fsob",
    1023: "fsow",
    1024: "dahc",
    1025: "oper",
    1026: "scda",
    1027: "scwv",
    1028: "dcda",
    1029: "dcwv",
    1030: "enda",
    1032: "efho",
    1033: "enfh",
    1034: "efov",
    1035: "enfo",
    1036: "sens",
    1037: "maed",
    1038: "amap",
    1039: "efhc",
    1040: "efhs",
    1041: "toga",
    1042: "cher",
    1043: "mnth",
    1044: "supd",
    1045: "wave",
    1046: "ocea",
    1047: "fgge",
    1050: "egrr",
    1051: "kwbc",
    1052: "edzw",
    1053: "lfpw",
    1054: "rjtd",
    1055: "cwao",
    1056: "ammc",
    1070: "msdc",
    1071: "moda",
    1072: "monr",
    1073: "mnvr",
    1074: "msda",
    1075: "mdfa",
    1076: "dacl",
    1077: "wehs",
    1078: "ewho",
    1079: "enwh",
    1080: "wamo",
    1081: "waef",
    1082: "wasf",
    1083: "mawv",
    1084: "ewhc",
    1085: "wvhc",
    1086: "weov",
    1087: "wavm",
    1088: "ewda",
    1089: "dacw",
    1090: "seas",
    1091: "sfmm",
    1092: "swmm",
    1093: "mofc",
    1094: "mofm",
    1095: "wamf",
    1096: "wmfm",
    1097: "smma",
    1110: "seap",
    1200: "mnfc",
    1201: "mnfh",
    1202: "mnfa",
    1203: "mnfw",
    1204: "mfhw",
    1205: "mfaw",
    1206: "mnfm",
    1207: "mfhm",
    1208: "mfam",
    1209: "mfwm",
    1210: "mhwm",
    1211: "mawm",
    1220: "mmsf",
    1221: "msmm",
    1222: "wams",
    1223: "mswm",
    1224: "mmsa",
    1230: "mmaf",
    1231: "mmam",
    1232: "mmaw",
    1233: "mmwm",
    1240: "esmm",
    1241: "ehmm",
    1242: "edmm",
    1243: "edmo",
    1244: "ewmo",
    1245: "ewmm",
    1246: "espd",
    1247: "lwda",
    1248: "lwwv",
    1249: "elda",
    1250: "ewla",
    1251: "wamd",
    1252: "gfas",
    2231: "cnrm",
    2232: "mpic",
    2233: "ukmo",
}

MARSTYPES = {
    1: "fg",
    2: "an",
    3: "ia",
    4: "oi",
    5: "3v",
    6: "4v",
    7: "3g",
    8: "4g",
    9: "fc",
    10: "cf",
    11: "pf",
    12: "ef",
    13: "ea",
    14: "cm",
    15: "cs",
    16: "fp",
    17: "em",
    18: "es",
    19: "fa",
    20: "cl",
    21: "si",
    22: "s3",
    23: "ed",
    24: "tu",
    25: "ff",
    26: "of",
    27: "efi",
    28: "efic",
    29: "pb",
    30: "ep",
    31: "bf",
    32: "cd",
    33: "4i",
    34: "go",
    35: "me",
    36: "pd",
    37: "ci",
    38: "sot",
    40: "im",
    42: "sim",
    43: "wem",
    44: "wes",
    45: "cr",
    46: "ses",
    47: "taem",
    48: "taes",
    50: "sg",
    52: "sf",
    60: "pa",
    61: "icp",
    62: "sv",
    63: "as",
    64: "svar",
    65: "cv",
    70: "or",
    71: "fx",
    80: "fcmean",
    81: "fcmax",
    82: "fcmin",
    83: "fcstdev",
    84: "emtm",
    85: "estdtm",
    86: "hcmean",
    87: "ssd",
    88: "gsd",
    89: "ga",
    256: "ob",
    257: "fb",
    258: "ai",
    259: "af",
    260: "ab",
    261: "tf",
    262: "mfb",
    263: "ofb",
}


def extract_grib_metadata(gribfile):
    """
      this will return a tuple containing:
        - ecmwfmars properties struct
        - levtype_options struct (see set_remote_url())
    """
    import coda

    @contextlib.contextmanager
    def coda_open(filename):
        coda_handle = coda.open(filename)
        try:
            yield coda_handle
        finally:
            coda.close(coda_handle)

    ecmwfmars = Struct()
    levtype_options = {}  # TODO: add extraction of levtype_options

    with coda_open(gribfile) as coda_handle:
        cursor = coda.Cursor()
        coda.cursor_set_product(cursor, coda_handle)
        num_messages = coda.cursor_get_num_elements(cursor)
        coda.cursor_goto_first_array_element(cursor)
        for i in range(num_messages):
            index = coda.cursor_get_available_union_field_index(cursor)
            coda.cursor_goto_record_field_by_index(cursor, index)
            step = 0
            if index == 0:
                # grib1
                centuryOfReferenceTimeOfData = coda.fetch(cursor, "centuryOfReferenceTimeOfData")
                yearOfCentury = coda.fetch(cursor, "yearOfCentury")
                month = coda.fetch(cursor, "month")
                day = coda.fetch(cursor, "day")
                date = "%02d%02d-%02d-%02d" % (centuryOfReferenceTimeOfData - 1, yearOfCentury, month, day)
                hour = coda.fetch(cursor, "hour")
                minute = coda.fetch(cursor, "minute")
                time = "%02d:%02d:00" % (hour, minute)
                unitOfTimeRange = coda.fetch(cursor, "unitOfTimeRange")
                if unitOfTimeRange != 0:
                    P1 = coda.fetch(cursor, "P1")
                    if unitOfTimeRange == 1:
                        step = P1
                    elif unitOfTimeRange == 2:
                        step = 24 * P1
                    elif unitOfTimeRange == 10:
                        step = 3 * P1
                    elif unitOfTimeRange == 11:
                        step = 6 * P1
                    elif unitOfTimeRange == 13:
                        step = 12 * P1
                    else:
                        raise Error("unsupported unitOfTimeRange: %d" % (unitOfTimeRange,))
                local = coda.fetch(cursor, "local")
                try:
                    local = local[1:9].tobytes()
                except AttributeError:
                    # workaround for older numpy versions
                    local = local[1:9].tostring()
                marsclass, marstype, stream, expver = struct.unpack('>BBH4s', local)
            else:
                # grib2
                year = coda.fetch(cursor, "year")
                month = coda.fetch(cursor, "month")
                day = coda.fetch(cursor, "day")
                date = "%04d-%02d-%02d" % (year, month, day)
                hour = coda.fetch(cursor, "hour")
                minute = coda.fetch(cursor, "minute")
                second = coda.fetch(cursor, "second")
                time = "%02d:%02d:%02d" % (hour, minute, second)
                significanceOfReferenceTime = coda.fetch(cursor, "significanceOfReferenceTime")
                local = coda.fetch(cursor, "local[0]")
                try:
                    local = local[2:12].tobytes()
                except AttributeError:
                    # workaround for older numpy versions
                    local = local[2:12].tostring()
                marsclass, marstype, stream, expver = struct.unpack('>HHH4s', local)
                coda.cursor_goto_record_field_by_name(cursor, "data")
                num_data = coda.cursor_get_num_elements(cursor)
                coda.cursor_goto_first_array_element(cursor)
                prev_step = None
                for j in range(num_data):
                    forecastTime = coda.fetch(cursor, "forecastTime")
                    if forecastTime != 0:
                        indicatorOfUnitOfTimeRange = coda.fetch(cursor, "indicatorOfUnitOfTimeRange")
                        if indicatorOfUnitOfTimeRange == 0:
                            # minutes
                            step = 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 1:
                            # hours
                            step = 60 * 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 2:
                            # days
                            step = 24 * 60 * 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 10:
                            # 3 hours
                            step = 3 * 60 * 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 11:
                            # 6 hours
                            step = 6 * 60 * 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 12:
                            # 12 hours
                            step = 12 * 60 * 60 * forecastTime
                        elif indicatorOfUnitOfTimeRange == 13:
                            # seconds
                            step = forecastTime
                        step = int(step / 3600.)  # convert seconds to hours
                        if prev_step is None:
                            prev_step = step
                        elif step != prev_step:
                            raise Error("not all data has the same 'step' time (%d) (%d)" % (step, prev_step))
                    if j < num_data - 1:
                        coda.cursor_goto_next_array_element(cursor)
                coda.cursor_goto_parent(cursor)
                coda.cursor_goto_parent(cursor)
            if marsclass not in MARSCLASSES:
                raise Error("unsupported MARS class (%d)" % (marsclass,))
            marsclass = MARSCLASSES[marsclass]
            if marstype not in MARSTYPES:
                raise Error("unsupported MARS type (%d)" % (marstype,))
            marstype = MARSTYPES[marstype]
            if stream not in MARSSTREAMS:
                raise Error("unsupported MARS stream (%d)" % (stream,))
            stream = MARSSTREAMS[stream]
            if 'date' in ecmwfmars:
                if date != ecmwfmars.date:
                    raise Error("not all data is for the same date (%s) (%s)" % (date, ecmwfmars.date))
                if time != ecmwfmars.time:
                    raise Error("not all data is for the same time (%s) (%s)" % (time, ecmwfmars.time))
                if step != 0:
                    if 'step' in ecmwfmars:
                        if step != ecmwfmars.step:
                            raise Error("not all data has the same 'step' time (%d) (%d)" % (step, ecmwfmars.step))
                    else:
                        raise Error("not all data has the same 'step' time")
                else:
                    if 'step' in ecmwfmars and ecmwfmars.step != 0:
                        raise Error("not all data has the same 'step' time")
                if marsclass != ecmwfmars.marsclass:
                    raise Error("not all data has the same MARS class (%s) (%s)" % (marsclass, ecmwfmars.marsclass))
                if marstype != ecmwfmars.type:
                    raise Error("not all data has the same MARS type (%s) (%s)" % (marstype, ecmwfmars.type))
                if stream != ecmwfmars.stream:
                    raise Error("not all data has the same MARS stream (%s) (%s)" % (stream, ecmwfmars.stream))
                if expver != ecmwfmars.expver:
                    raise Error("not all data has the same MARS experiment version (%s) (%s)" %
                                (expver, ecmwfmars.expver))
            else:
                ecmwfmars.date = date
                ecmwfmars.time = time
                if step != 0:
                    ecmwfmars.step = step
                ecmwfmars.marsclass = marsclass
                ecmwfmars.type = marstype
                ecmwfmars.stream = stream
                ecmwfmars.expver = expver
            coda.cursor_goto_parent(cursor)
            if i < num_messages - 1:
                coda.cursor_goto_next_array_element(cursor)

    return ecmwfmars, levtype_options


def get_remote_url(filename, ecmwfmars, levtype_options):
    """
        levtype_options should be a dict with for each 'levtype' field a dict containing:
        - string 'param'
            '/' separated list of parameters for the given level type
            https://software.ecmwf.int/wiki/display/UDOC/Identification+keywords#Identificationkeywords-param
        - string 'levelist' (optional)
            '/' separated list of levels for the given level type
            https://software.ecmwf.int/wiki/display/UDOC/Identification+keywords#Identificationkeywords-levelist
    """

    if not levtype_options:
        raise Error("no parameters to construct remote_url")

    remote_url = "ecmwfapi:%s?" % (filename,)
    request = {
        'class': ecmwfmars.marsclass,
        'stream': ecmwfmars.stream,
        'expver': ecmwfmars.expver,
        'type': ecmwfmars.type,
        'date': ecmwfmars.date,
        'time': ecmwfmars.time,
    }
    if 'step' in ecmwfmars:
        request['step'] = str(ecmwfmars.step)
    if 'resol' in ecmwfmars:
        request['resol'] = ecmwfmars.resol
    if 'grid' in ecmwfmars:
        request['grid'] = ecmwfmars.grid
    if 'area' in ecmwfmars:
        request['area'] = ecmwfmars.area

    first = True
    for levtype in levtype_options:
        if first:
            first = False
        else:
            # The '&concatenate&' is a muninn-specific way of combining multiple requests in one
            remote_url += "&concatenate&"
        request['levtype'] = levtype
        request['param'] = levtype_options[levtype]['param']
        if 'levelist' in levtype_options[levtype]:
            request['levelist'] = levtype_options[levtype]['levelist']
        elif 'levelist' in request:
            del request['levelist']
        remote_url += "&".join(["%s=%s" % (key, str(request[key])) for key in request])

    return remote_url


def get_core_properties(product_type, ecmwfmars, levtype_options=None):
    date = datetime.datetime.strptime(ecmwfmars.date.replace('-', ''), "%Y%m%d")
    time = ecmwfmars.time.replace(':', '')
    if len(time) >= 2:
        if len(time) >= 4:
            date += datetime.timedelta(hours=int(time[0:2]), minutes=int(time[2:4]))
        else:
            date += datetime.timedelta(hours=int(time[0:2]))
    core = Struct()
    core.uuid = Archive.generate_uuid()
    core.active = True
    core.product_type = product_type
    core.product_name = "%s_%s_%s_%s_%s_%s" % (product_type, ecmwfmars.marsclass, ecmwfmars.stream, ecmwfmars.expver,
                                               ecmwfmars.type, date.strftime("%Y%m%dT%H%M%S"))
    if 'step' in ecmwfmars:
        core.product_name += "_%03d" % (ecmwfmars.step,)
    core.physical_name = "%s.grib" % (core.product_name,)
    core.validity_start = date
    if 'step' in ecmwfmars:
        core.validity_start += datetime.timedelta(hours=ecmwfmars.step)
    core.validity_stop = core.validity_start
    # the creation date is set to the base time of the model
    core.creation_date = date
    if levtype_options:
        core.remote_url = get_remote_url(core.physical_name, ecmwfmars, levtype_options)
    return core


class ECMWFBackend(RemoteBackend):
    """
    'ecmwfapi' urls are custom defined urls for retrieving data from ECMWF MARS.
    It uses the following format:

        ecmwfapi:<filename>?<query>

    where 'filename' should equal the core.physical_name metadata field and
    'query' is a '&' separated list of key/value pairs for the ECMWF MARS request.

    The backend will use the ecmwf-api-client library to retrieve the given product.
    Note that you either need a .ecmwfapirc file with a ECMWF KEY in your home directory or
    you need to set the ECMWF_API_KEY, ECMWF_API_URL, ECMWF_API_EMAIL environment variables.

    The interface supports MARS access as described at:
        https://software.ecmwf.int/wiki/display/WEBAPI/Access+MARS
    """

    def pull(self, archive, product, target_path):
        from ecmwfapi import ECMWFService
        marsservice = ECMWFService("mars", log=logging.info)

        requests = []
        for order in product.core.remote_url.split('?')[1].split('&concatenate&'):
            request = {}
            for param in order.split('&'):
                key, value = param.split('=')
                request[key] = value
            requests.append(request)

        file_path = os.path.join(target_path, product.core.physical_name)
        try:
            # Download first grib file.
            request = requests[0]
            marsservice.execute(request, file_path)
            # Download remaining grib files (if needed) and append to final product.
            if len(requests) > 1:
                tmp_file = os.path.join(target_path, "request.grib")
                with open(file_path, "ab") as combined_file:
                    for request in requests[1:]:
                        marsservice.execute(request, tmp_file)
                        with open(tmp_file, "rb") as result_file:
                            while True:
                                block = result_file.read(1048576)  # use 1MB blocks
                                if not block:
                                    break
                                combined_file.write(block)
                        os.remove(tmp_file)
        except EnvironmentError as _error:
            raise Error("unable to transfer product to destination path '%s' [%s]" % (file_path, _error))
        return [file_path]


REMOTE_BACKENDS = {
    'ecmwfapi': ECMWFBackend(prefix='ecmwfapi:'),
}


def remote_backends():
    return REMOTE_BACKENDS.keys()


def remote_backend(name):
    return REMOTE_BACKENDS[name]
