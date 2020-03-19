from ..utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class FixedWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:windowfn:fixed_windows:v0.1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class GlobalWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:windowfn:global_windows:v0.1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class SessionsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:windowfn:session_windows:v0.1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class SlidingWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:windowfn:sliding_windows:v0.1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

