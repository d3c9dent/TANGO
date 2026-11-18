#!/usr/bin/env python3
"""Run the retained suite, optionally excluding six Linux process-identity tests."""
import argparse
import json
import platform
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LINUX_ONLY = {
    'test_audit_regressions.AuditRegressionTests.test_foreign_namespace_never_signals_matching_local_pid',
    'test_audit_regressions.AuditRegressionTests.test_same_namespace_wrong_fingerprint_never_signals_pid',
    'test_audit_regressions.AuditRegressionTests.test_stale_heartbeat_stop_targets_matching_process',
    'test_process.ProcessTests.test_foreign_namespace_status_and_stop_fail_closed',
    'test_process.ProcessTests.test_stale_heartbeat_stop_and_restart_use_live_identity',
    'test_process.ProcessTests.test_actual_scheduler_restart_preserves_queue',
}


def cases(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--portable', action='store_true',
                        help='Exclude the six Linux /proc tests; this is not a full-suite pass')
    parser.add_argument('--json', type=Path, help='Write a machine-readable summary')
    args = parser.parse_args()
    if sys.version_info < (3, 10):
        parser.error('Python 3.10 or later is required')
    if sys.platform != 'linux' and not args.portable:
        parser.error('The full suite requires Linux; use --portable for the limited subset')
    if not sys.platform.startswith(('linux', 'darwin')):
        parser.error('These tests require a POSIX host with fcntl; Windows is not supported')
    sys.dont_write_bytecode = True
    tests = list(cases(unittest.defaultTestLoader.discover(str(ROOT / 'tests'))))
    omitted = sorted(test.id() for test in tests if args.portable and test.id() in LINUX_ONLY)
    if args.portable and set(omitted) != LINUX_ONLY:
        parser.error('The Linux-only test inventory changed; review the exclusion list')
    selected = [test for test in tests if test.id() not in omitted]
    print(f'Platform: {platform.system()}; Python: {platform.python_version()}', flush=True)
    print(f'Discovered {len(tests)} tests; selected {len(selected)}; excluded {len(omitted)}.', flush=True)
    for name in omitted:
        print(f'Excluded (requires Linux /proc): {name}', flush=True)
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(selected))
    report = {
        'platform': platform.system(),
        'python': platform.python_version(),
        'mode': 'portable subset' if args.portable else 'full Linux suite',
        'discovered': len(tests),
        'executed': result.testsRun,
        'passed': result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped)
                  - len(result.expectedFailures) - len(result.unexpectedSuccesses),
        'failures': len(result.failures),
        'errors': len(result.errors),
        'skipped': len(result.skipped),
        'expected_failures': len(result.expectedFailures),
        'unexpected_successes': len(result.unexpectedSuccesses),
        'excluded_linux_tests': omitted,
        'seconds': round(time.monotonic() - started, 3),
        'successful': result.wasSuccessful(),
    }
    if args.json:
        args.json.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
