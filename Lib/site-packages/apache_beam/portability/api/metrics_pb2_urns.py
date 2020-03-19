from ..utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class MonitoringInfo(object):

  class MonitoringInfoLabels(object):
    TRANSFORM = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='PTRANSFORM'))
    PCOLLECTION = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='PCOLLECTION'))
    WINDOWING_STRATEGY = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='WINDOWING_STRATEGY'))
    CODER = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='CODER'))
    ENVIRONMENT = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='ENVIRONMENT'))
    NAMESPACE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='NAMESPACE'))
    NAME = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='NAME'))


class MonitoringInfoSpecs(object):

  class Enum(object):
    USER_COUNTER = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user', type_urn='beam:metrics:sum_int_64', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user numeric counters.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    ELEMENT_COUNT = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:element_count:v1', type_urn='beam:metrics:sum_int_64', required_labels=['PCOLLECTION'], annotations=[metrics_pb2.Annotation(key='description', value='The total elements output to a Pcollection by a PTransform.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    SAMPLED_BYTE_SIZE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:sampled_byte_size:v1', type_urn='beam:metrics:distribution_int_64', required_labels=['PCOLLECTION'], annotations=[metrics_pb2.Annotation(key='description', value='The total byte size and count of a sampled  set (or all) of elements in the pcollection. Sampling is used  because calculating the byte count involves serializing the  elements which is CPU intensive.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    START_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:start_bundle_msecs:v1', type_urn='beam:metrics:sum_int_64', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the start bundlefunction in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:process_bundle_msecs:v1', type_urn='beam:metrics:sum_int_64', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the process bundlefunction in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    FINISH_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:finish_bundle_msecs:v1', type_urn='beam:metrics:sum_int_64', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the finish bundle function in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    TOTAL_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:ptransform_execution_time:total_msecs:v1', type_urn='beam:metrics:sum_int_64', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the ptransform')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_DISTRIBUTION_COUNTER = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user_distribution', type_urn='beam:metrics:distribution_int_64', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user distribution counters.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)


class MonitoringInfoTypeUrns(object):

  class Enum(object):
    SUM_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:sum_int_64', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DISTRIBUTION_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:distribution_int_64', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    LATEST_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:latest_int_64', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

