"""Check acceptance command coverage without starting a server or S3 backend."""
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import s3_acceptance


class SemanticSections(unittest.TestCase):
    def test_commands_match_smoke_sections_and_gate_fault_only_cases(self):
        source = Path(__file__).with_name('record_service_smoke.py').read_text()
        choices = next(ast.literal_eval(keyword.value)
                       for call in ast.walk(ast.parse(source))
                       if isinstance(call, ast.Call) and call.args
                       and isinstance(call.args[0], ast.Constant)
                       and call.args[0].value == '--section'
                       for keyword in call.keywords if keyword.arg == 'choices')
        fault_only = {'indexer', 'authorization'}
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'server'
            binary.write_bytes(b'test binary')
            for faults in (False, True):
                argv = ['s3_acceptance.py', '--suite', 'semantic', '--server', str(binary)]
                if faults:
                    argv.append('--faults')
                with patch('sys.argv', argv), patch.object(s3_acceptance, 'configuration', return_value=({}, None)), \
                     patch.object(s3_acceptance.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as run:
                    s3_acceptance.main()
                commands = [call.args[0] for call in run.call_args_list]
                sections = [command[command.index('--section') + 1] for command in commands]
                self.assertEqual(len(sections), len(set(sections)))
                self.assertEqual(set(sections), set(choices) if faults else set(choices) - fault_only)
                for command in commands:
                    self.assertIn('--cold-restarts', command)
                    self.assertEqual('--faults' in command, faults)


if __name__ == '__main__':
    unittest.main()
