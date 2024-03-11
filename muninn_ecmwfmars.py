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


# This namespace contains mars request paremeters
class ECMWFMARSNamespace(Mapping):
    marsclass = Text(index=True)  # MARS abbreviation from http://apps.ecmwf.int/codes/grib/format/mars/class/
    stream = Text(index=True)  # MARS abbreviation from http://apps.ecmwf.int/codes/grib/format/mars/stream/
    expver = Text(index=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=124752178
    type = Text(index=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=127315300
    date = Text(index=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=118817289
    time = Text(index=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=118817378
    step = Integer(optional=True, index=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=118820050
    resol = Text(optional=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=171422484
    grid = Text(optional=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=123799065
    area = Text(optional=True)  # https://confluence.ecmwf.int/pages/viewpage.action?pageId=151520973


NAMESPACES = {
    'ecmwfmars': ECMWFMARSNamespace
}


def namespaces():
    return NAMESPACES.keys()


def namespace(name):
    return NAMESPACES[name]


# http://apps.ecmwf.int/codes/grib/format/mars/class/
MARSCLASSES = {
    1: "od",  # Operational archive
    2: "rd",  # Research department
    3: "er",  # 15 years reanalysis (ERA15)
    4: "cs",  # ECSN
    5: "e4",  # 40 years reanalysis (ERA40)
    6: "dm",  # DEMETER
    7: "pv",  # PROVOST
    8: "el",  # ELDAS
    9: "to",  # TOST
    10: "co",  # COSMO-LEPS
    11: "en",  # ENSEMBLES
    12: "ti",  # TIGGE
    13: "me",  # MERSEA
    14: "ei",  # ERA Interim
    15: "sr",  # Short-Range Ensemble Prediction System
    16: "dt",  # Data Targeting System
    17: "la",  # ALADIN-LAEF
    18: "yt",  # YOTC
    19: "mc",  # Copernicus Atmosphere Monitoring Service (CAMS, previously MACC)
    20: "pe",  # Permanent experiments
    21: "em",  # ERA-CLIM model integration for the 20th-century (ERA-20CM)
    22: "e2",  # ERA-CLIM reanalysis of the 20th-century using surface observations only (ERA-20C)
    23: "ea",  # ERA5
    24: "ep",  # ERA-CLIM2 coupled reanalysis of the 20th-century (CERA-20C)
    25: "rm",  # EURO4M
    26: "nr",  # NOAA/CIRES 20th Century Reanalysis version II
    27: "s2",  # Sub-seasonal to seasonal prediction project (S2S)
    28: "j5",  # Japanese 55 year Reanalysis (JRA55)
    29: "ur",  # UERRA
    30: "et",  # ERA-CLIM2 coupled reanalysis of the satellite era (CERA-SAT)
    31: "c3",  # Copernicus Climate Change Service (C3S)
    32: "yp",  # YOPP
    33: "lp",  # ERA5/LAND
    34: "lw",  # WMO Lead Centre Wave Forecast Verification
    35: "ce",  # Copernicus Emergency Management Service (CEMS)
    36: "cr",  # Copernicus Atmosphere Monitoring Service (CAMS) Research
    37: "rr",  # Copernicus Regional ReAnalysis (CARRA/CERRA)
    38: "ul",  # Project ULYSSES
    39: "wv",  # Global Wildfire Information System
    40: "e6",  # ERA6
    41: "l6",  # ERA6/LAND
    42: "ef",  # EFAS (European flood awareness system)
    43: "gf",  # GLOFAS (Global flood awareness system)
    44: "gg",  # Greenhouse Gases
    45: "ml",  # Machine learning
    46: "d1",  # Destination Earth
    47: "o6",  # Ocean ReAnalysis 6
    48: "eh",  # C3S European hydrology
    49: "gh",  # C3S Global hydrology
    50: "ci",  # CERISE project
    51: "ai",  # Operational AIFS
    99: "te",  # Test
    100: "at",  # Austria
    101: "be",  # Belgium
    102: "hr",  # Croatia
    103: "dk",  # Denmark
    104: "fi",  # Finland
    105: "fr",  # France
    106: "de",  # Germany
    107: "gr",  # Greece
    108: "hu",  # Hungary
    109: "is",  # Iceland
    110: "ie",  # Ireland
    111: "it",  # Italy
    112: "nl",  # Netherlands
    113: "no",  # Norway
    114: "pt",  # Portugal
    115: "si",  # Slovenia
    116: "es",  # Spain
    117: "se",  # Sweden
    118: "ch",  # Switzerland
    119: "tr",  # Turkey
    120: "uk",  # United Kingdom
    121: "ms",  # Member States projects
    199: "ma",  # Metaps
}

# http://apps.ecmwf.int/codes/grib/format/mars/stream/
MARSSTREAMS = {
    1022: "fsob",  # Forecast sensitivity to observations
    1023: "fsow",  # Forecast sensitivity to observations wave
    1024: "dahc",  # Daily archive hindcast
    1025: "oper",  # Atmospheric model
    1026: "scda",  # Atmospheric model (short cutoff)
    1027: "scwv",  # Wave model (short cutoff)
    1028: "dcda",  # Atmospheric model (delayed cutoff)
    1029: "dcwv",  # Wave model (delayed cutoff)
    1030: "enda",  # Ensemble data assimilation
    1032: "efho",  # Ensemble forecast hindcast overlap
    1033: "enfh",  # Ensemble forecast hindcasts
    1034: "efov",  # Ensemble forecast overlap
    1035: "enfo",  # Ensemble prediction system
    1036: "sens",  # Sensitivity forecast
    1037: "maed",  # Multianalysis ensemble data
    1038: "amap",  # Analysis for multianalysis project
    1039: "efhc",  # Ensemble forecast hindcasts (obsolete)
    1040: "efhs",  # Ensemble forecast hindcast statistics
    1041: "toga",  # TOGA
    1042: "cher",  # Chernobyl
    1043: "mnth",  # Monthly means
    1044: "supd",  # Deterministic supplementary data
    1045: "wave",  # Wave model
    1046: "ocea",  # Ocean
    1047: "fgge",  # FGGE
    1050: "egrr",  # Bracknell
    1051: "kwbc",  # Washington
    1052: "edzw",  # Offenbach
    1053: "lfpw",  # Toulouse
    1054: "rjtd",  # Tokyo
    1055: "cwao",  # Montreal
    1056: "ammc",  # Melbourne
    1057: "efas",  # European flood awareness system (EFAS)
    1058: "efse",  # European flood awareness system (EFAS) seasonal forecasts
    1059: "efcl",  # European flood awareness system (EFAS) climatology
    1060: "wfas",  # Global flood awareness system (GLOFAS)
    1061: "wfcl",  # Global flood awareness system (GLOFAS) climatology
    1062: "wfse",  # Global flood awareness system (GLOFAS) seasonal forecasts
    1063: "efrf",  # European flood awareness system (EFAS) reforecasts
    1064: "efsr",  # European flood awareness system (EFAS) seasonal reforecasts
    1065: "wfrf",  # Global flood awareness system (GLOFAS) reforecasts
    1066: "wfsr",  # Global flood awareness system (GLOFAS) seasonal reforecasts
    1070: "msdc",  # Monthly standard deviation and covariance
    1071: "moda",  # Monthly means of daily means
    1072: "monr",  # Monthly means using G. Boer's step function
    1073: "mnvr",  # Monthly variance and covariance data using G. Boer's step function
    1074: "msda",  # Monthly standard deviation and covariance of daily means
    1075: "mdfa",  # Monthly means of daily forecast accumulations
    1076: "dacl",  # Daily climatology
    1077: "wehs",  # Wave ensemble forecast hindcast statistics
    1078: "ewho",  # Ensemble forecast wave hindcast overlap
    1079: "enwh",  # Ensemble forecast wave hindcasts
    1080: "wamo",  # Wave monthly means
    1081: "waef",  # Wave ensemble forecast
    1082: "wasf",  # Wave seasonal forecast
    1083: "mawv",  # Multianalysis wave data
    1084: "ewhc",  # Wave ensemble forecast hindcast (obsolete)
    1085: "wvhc",  # Wave hindcast
    1086: "weov",  # Wave ensemble forecast overlap
    1087: "wavm",  # Wave model (standalone)
    1088: "ewda",  # Ensemble wave data assimilation
    1089: "dacw",  # Daily climatology wave
    1090: "seas",  # Seasonal forecast
    1091: "sfmm",  # Seasonal forecast atmospheric monthly means
    1092: "swmm",  # Seasonal forecast wave monthly means
    1093: "mofc",  # Monthly forecast
    1094: "mofm",  # Monthly forecast means
    1095: "wamf",  # Wave monthly forecast
    1096: "wmfm",  # Wave monthly forecast means
    1097: "smma",  # Seasonal monthly means anomalies
    1098: "clte",  # Climate run output
    1099: "clmn",  # Climate run monthly means output
    1100: "dame",  # Daily means
    1110: "seap",  # Sensitive area prediction
    1120: "eefh",  # Extended ensemble forecast hindcast
    1121: "eehs",  # Extended ensemble forecast hindcast statistics
    1122: "eefo",  # Extended ensemble prediction system
    1123: "weef",  # Wave extended ensemble forecast
    1124: "weeh",  # Wave extended ensemble forecast hindcast
    1125: "wees",  # Wave extended ensemble forecast hindcast statistics
    1200: "mnfc",  # Real-time
    1201: "mnfh",  # Hindcasts
    1202: "mnfa",  # Anomalies
    1203: "mnfw",  # Wave real-time
    1204: "mfhw",  # Monthly forecast hindcasts wave
    1205: "mfaw",  # Wave anomalies
    1206: "mnfm",  # Real-time means
    1207: "mfhm",  # Hindcast means
    1208: "mfam",  # Anomaly means
    1209: "mfwm",  # Wave real-time means
    1210: "mhwm",  # Wave hindcast means
    1211: "mawm",  # Wave anomaly means
    1220: "mmsf",  # Multi-model seasonal forecast
    1221: "msmm",  # Multi-model seasonal forecast atmospheric monthly means
    1222: "wams",  # Multi-model seasonal forecast wave
    1223: "mswm",  # Multi-model seasonal forecast wave monthly means
    1224: "mmsa",  # Multi-model seasonal forecast monthly anomalies
    1230: "mmaf",  # Multi-model multi-annual forecast
    1231: "mmam",  # Multi-model multi-annual forecast means
    1232: "mmaw",  # Multi-model multi-annual forecast wave
    1233: "mmwm",  # Multi-model multi-annual forecast wave means
    1240: "esmm",  # Combined multi-model monthly means
    1241: "ehmm",  # Combined multi-model hindcast monthly means
    1242: "edmm",  # Ensemble data assimilation monthly means
    1243: "edmo",  # Ensemble data assimilation monthly means of daily means
    1244: "ewmo",  # Ensemble wave data assimilation monthly means of daily means
    1245: "ewmm",  # Ensemble wave data assimilation monthly means
    1246: "espd",  # Ensemble supplementary data
    1247: "lwda",  # Long window daily archive
    1248: "lwwv",  # Long window wave
    1249: "elda",  # Ensemble Long window Data Assimilation
    1250: "ewla",  # Ensemble Wave Long window data Assimilation
    1251: "wamd",  # Wave monthly means of daily means
    1252: "gfas",  # Global fire assimilation system
    1253: "ocda",  # Ocean data assimilation
    1254: "olda",  # Ocean Long window data assimilation
    1255: "gfra",  # Global Fire assimilation system reanalysis
    1256: "rfsd",  # Retrospective forcing and simulation data
    2231: "cnrm",  # Meteo France climate centre
    2232: "mpic",  # Max Plank Institute
    2233: "ukmo",  # UKMO climate centre
}

# https://codes.ecmwf.int/grib/format/mars/type/
MARSTYPES = {
    1: "fg",  # First guess
    2: "an",  # Analysis
    3: "ia",  # Initialised analysis
    4: "oi",  # Oi analysis
    5: "3v",  # 3d variational analysis
    6: "4v",  # 4d variational analysis
    7: "3g",  # 3d variational gradients
    8: "4g",  # 4d variational gradients
    9: "fc",  # Forecast
    10: "cf",  # Control forecast
    11: "pf",  # Perturbed forecast
    12: "ef",  # Errors in first guess
    13: "ea",  # Errors in analysis
    14: "cm",  # Cluster means
    15: "cs",  # Cluster std deviations
    16: "fp",  # Forecast probability
    17: "em",  # Ensemble mean
    18: "es",  # Ensemble standard deviation
    19: "fa",  # Forecast accumulation
    20: "cl",  # Climatology
    21: "si",  # Climate simulation
    22: "s3",  # Climate 30 days simulation
    23: "ed",  # Empirical distribution
    24: "tu",  # Tubes
    25: "ff",  # Flux forcing realtime
    26: "of",  # Ocean forward
    27: "efi",  # Extreme forecast index
    28: "efic",  # Extreme forecast index control
    29: "pb",  # Probability boundaries
    30: "ep",  # Event probability
    31: "bf",  # Bias-corrected forecast
    32: "cd",  # Climate distribution
    33: "4i",  # 4D analysis increments
    34: "go",  # Gridded observations
    35: "me",  # Model errors
    36: "pd",  # Probability distribution
    37: "ci",  # Cluster information
    38: "sot",  # Shift of Tail
    39: "eme",  # Ensemble data assimilation model errors
    40: "im",  # Images
    42: "sim",  # Simulated images
    43: "wem",  # Weighted ensemble mean
    44: "wes",  # Weighted ensemble standard deviation
    45: "cr",  # Cluster representative
    46: "ses",  # Scaled ensemble standard deviation
    47: "taem",  # Time average ensemble mean
    48: "taes",  # Time average ensemble standard deviation
    50: "sg",  # Sensitivity gradient
    52: "sf",  # Sensitivity forecast
    60: "pa",  # Perturbed analysis
    61: "icp",  # Initial condition perturbation
    62: "sv",  # Singular vector
    63: "as",  # Adjoint singular vector
    64: "svar",  # Signal variance
    65: "cv",  # Calibration/Validation forecast
    70: "or",  # Ocean reanalysis
    71: "fx",  # Flux forcing
    72: "fu",  # Fill-up
    73: "fso",  # Simulations with forcing
    74: "tpa",  # Time processed analysis
    75: "if",  # Interim forecast
    80: "fcmean",  # Forecast mean
    81: "fcmax",  # Forecast maximum
    82: "fcmin",  # Forecast minimum
    83: "fcstdev",  # Forecast standard deviation
    86: "hcmean",  # Hindcast climate mean
    87: "ssd",  # Simulated satellite data
    88: "gsd",  # Gridded satellite data
    89: "ga",  # GFAS analysis
    90: "gai",  # Gridded analysis input
    256: "ob",  # Observations
    257: "fb",  # Feedback
    258: "ai",  # Analysis input
    259: "af",  # Analysis feedback
    260: "ab",  # Analysis bias
    261: "tf",  # Trajectory forecast
    262: "mfb",  # MonDB feedback
    263: "ofb",  # ODB feedback
    264: "oai",  # ODB analysis input
    265: "sfb",  # Summary feedback
    266: "fsoifb",  # Forecast sensitivity to observations impact feedback
    267: "fcdfb",  # Forecast departures feedback
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


def get_remote_url(filename, ecmwfmars, levtype_options, packing=None):
    """
        levtype_options should be a dict with for each 'levtype' field a dict containing:
        - string 'param'
            '/' separated list of parameters for the given level type
        - string 'levelist' (optional)
            '/' separated list of levels for the given level type
        See also:
          - 'levtype': https://confluence.ecmwf.int/pages/viewpage.action?pageId=149335319
          - 'param': https://confluence.ecmwf.int/pages/viewpage.action?pageId=149335858
          - 'levelist': https://confluence.ecmwf.int/pages/viewpage.action?pageId=149335403
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
    if packing is not None:
        request['packing'] = packing

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


def get_core_properties(product_type, ecmwfmars, levtype_options=None, packing=None):
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
        core.remote_url = get_remote_url(core.physical_name, ecmwfmars, levtype_options, packing)
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
        https://confluence.ecmwf.int/display/WEBAPI/Access+MARS
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
