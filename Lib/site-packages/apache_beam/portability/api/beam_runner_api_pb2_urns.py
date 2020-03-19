from ..utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class BeamConstants(object):

  class Constants(object):
    MIN_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '-9223372036854775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MAX_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '9223372036854775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '9223371950454775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardCoders(object):

  class Enum(object):
    BYTES = PropertiesFromEnumValue('beam:coder:bytes:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    STRING_UTF8 = PropertiesFromEnumValue('beam:coder:string_utf8:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    KV = PropertiesFromEnumValue('beam:coder:kv:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BOOL = PropertiesFromEnumValue('beam:coder:bool:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    VARINT = PropertiesFromEnumValue('beam:coder:varint:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DOUBLE = PropertiesFromEnumValue('beam:coder:double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ITERABLE = PropertiesFromEnumValue('beam:coder:iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TIMER = PropertiesFromEnumValue('beam:coder:timer:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    INTERVAL_WINDOW = PropertiesFromEnumValue('beam:coder:interval_window:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    LENGTH_PREFIX = PropertiesFromEnumValue('beam:coder:length_prefix:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GLOBAL_WINDOW = PropertiesFromEnumValue('beam:coder:global_window:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    WINDOWED_VALUE = PropertiesFromEnumValue('beam:coder:windowed_value:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PARAM_WINDOWED_VALUE = PropertiesFromEnumValue('beam:coder:param_windowed_value:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    STATE_BACKED_ITERABLE = PropertiesFromEnumValue('beam:coder:state_backed_iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ROW = PropertiesFromEnumValue('beam:coder:row:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardEnvironments(object):

  class Environments(object):
    DOCKER = PropertiesFromEnumValue('beam:env:docker:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS = PropertiesFromEnumValue('beam:env:process:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    EXTERNAL = PropertiesFromEnumValue('beam:env:external:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardPTransforms(object):

  class CombineComponents(object):
    COMBINE_PER_KEY_PRECOMBINE = PropertiesFromEnumValue('beam:transform:combine_per_key_precombine:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_PER_KEY_MERGE_ACCUMULATORS = PropertiesFromEnumValue('beam:transform:combine_per_key_merge_accumulators:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_PER_KEY_EXTRACT_OUTPUTS = PropertiesFromEnumValue('beam:transform:combine_per_key_extract_outputs:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_GROUPED_VALUES = PropertiesFromEnumValue('beam:transform:combine_grouped_values:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class Composites(object):
    COMBINE_PER_KEY = PropertiesFromEnumValue('beam:transform:combine_per_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_GLOBALLY = PropertiesFromEnumValue('beam:transform:combine_globally:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    RESHUFFLE = PropertiesFromEnumValue('beam:transform:reshuffle:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    WRITE_FILES = PropertiesFromEnumValue('beam:transform:write_files:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class DeprecatedPrimitives(object):
    READ = PropertiesFromEnumValue('beam:transform:read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CREATE_VIEW = PropertiesFromEnumValue('beam:transform:create_view:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class Primitives(object):
    PAR_DO = PropertiesFromEnumValue('beam:transform:pardo:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    FLATTEN = PropertiesFromEnumValue('beam:transform:flatten:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GROUP_BY_KEY = PropertiesFromEnumValue('beam:transform:group_by_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    IMPULSE = PropertiesFromEnumValue('beam:transform:impulse:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ASSIGN_WINDOWS = PropertiesFromEnumValue('beam:transform:window_into:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TEST_STREAM = PropertiesFromEnumValue('beam:transform:teststream:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MAP_WINDOWS = PropertiesFromEnumValue('beam:transform:map_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MERGE_WINDOWS = PropertiesFromEnumValue('beam:transform:merge_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class SplittableParDoComponents(object):
    PAIR_WITH_RESTRICTION = PropertiesFromEnumValue('beam:transform:sdf_pair_with_restriction:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SPLIT_RESTRICTION = PropertiesFromEnumValue('beam:transform:sdf_split_restriction:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_KEYED_ELEMENTS = PropertiesFromEnumValue('beam:transform:sdf_process_keyed_elements:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_ELEMENTS = PropertiesFromEnumValue('beam:transform:sdf_process_elements:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SPLIT_AND_SIZE_RESTRICTIONS = PropertiesFromEnumValue('beam:transform:sdf_split_and_size_restrictions:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS = PropertiesFromEnumValue('beam:transform:sdf_process_sized_element_and_restrictions:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardSideInputTypes(object):

  class Enum(object):
    ITERABLE = PropertiesFromEnumValue('beam:side_input:iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MULTIMAP = PropertiesFromEnumValue('beam:side_input:multimap:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

