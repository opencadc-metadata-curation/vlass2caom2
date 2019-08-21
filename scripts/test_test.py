import pytest
import caom2pipe
from vlass2caom2 import vlass_validator
from caom2pipe import manage_composable as mc

from mock import PropertyMock, Mock, patch

TDD='./'

# with patch('caom2pipe.manage_composable.Config.working_directory', new_callable=PropertyMock) as mockx:
#     mockx.side_effect = TDD
#     vlass_validator.generate_reconciliation_todo()

setter_mock = Mock(wraps=caom2pipe.manage_composable.Config.working_directory.fget,
                   side_effect=TDD)
mock_property = caom2pipe.manage_composable.Config.working_directory.getter(setter_mock)
with patch.object(caom2pipe.manage_composable.Config, 'working_directory', mock_property):
    vlass_validator.generate_reconciliation_todo()


# @pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
#                     reason='support single version')
# def test_generate_reconciliation_mode():
#     # execution
#     todo_file = os.path.join(TEST_DATA_DIR, 'todo.txt')
#     if os.access(todo_file, os.R_OK):
#         start_time = os.path.getmtime(todo_file)
#     else:
#         start_time = datetime.now()
#     getcwd_orig = os.getcwd
#     os.getcwd = Mock(return_value=TEST_DATA_DIR)
#     try:
#         from mock import PropertyMock
#
#         setter_mock = Mock(wraps=mc.Config.working_directory.fset,
#                            side_effect=TEST_DATA_DIR)
#         getter_mock = Mock(wraps=mc.Config.working_directory.fget,
#                            side_effect=TEST_DATA_DIR)
#         mock_property = mc.Config.working_directory.setter(setter_mock)
#         mock_property2 = mc.Config.working_directory.getter(getter_mock)
#         with patch.object(mc.Config, 'working_directory', mock_property2):
#         # with patch.object(mc.Config.working_directory, return_value=TEST_DATA_DIR):
#         # with patch(mc.Config.working_directory, return_value=TEST_DATA_DIR):
#         # with patch('caom2pipe.manage_composable.Config.working_directory', new_callable=PropertyMock) as wd_mock:
#         #     wd_mock.side_effect = TEST_DATA_DIR
#         #     wd_mock.return_value = TEST_DATA_DIR
#             vlass_validator.generate_reconciliation_todo()
#             end_time = os.path.getmtime(todo_file)
#             assert end_time > start_time, 'file not changed'
#             with open(todo_file) as f:
#                 result = f.readlines()
#             assert len(result) == 2, 'wrong amount of content'
#     finally:
#         os.getcwd = getcwd_orig

