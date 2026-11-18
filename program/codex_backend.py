"""Real Codex CLI execution with durable attempt records and conservative recovery.

This module never manufactures collaboration-tool receipts. A session identifier
comes from the CLI's JSONL stream, and a successful turn requires both an actual
``turn.completed`` event and a zero process exit status. Attempt directories are
private runtime data, not release artifacts.
"""
from __future__ import annotations

import contextlib
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid


class BackendError(RuntimeError):
    def __init__(self, kind, message, *, record=None, record_dir=None):
        super().__init__(message)
        self.kind = kind
        self.record = record or {}
        self.record_dir = str(record_dir) if record_dir else None
        self.session = self.record.get('session')
        self.receipt = self.record.get('receipt')
        self.terminal_receipt = self.record.get('terminal_receipt')


def _private_write(path, content):
    path = Path(path)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _save(directory, record):
    record['updated'] = time.time()
    _private_write(Path(directory) / 'attempt.json', json.dumps(record, indent=2) + '\n')


def _json_file(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise BackendError('recovery', 'Expected a regular attempt journal', record_dir=path.parent)
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise BackendError('recovery', 'Unreadable attempt journal', record_dir=path.parent) from exc
    if not isinstance(data, dict):
        raise BackendError('recovery', 'Malformed attempt journal', record_dir=path.parent)
    return data


def process_identity(pid):
    """Return an OS birth marker, never a command-line or PID-only identity."""
    if type(pid) is not int or pid <= 0:
        return None
    try:
        if sys.platform.startswith('linux'):
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            if fields[0] == 'Z':
                return None
            boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            namespace = os.readlink(f'/proc/{pid}/ns/pid')
            return {'pid': pid, 'start': boot + ':' + fields[19], 'platform': 'linux',
                    'process_group': int(fields[2]), 'pid_namespace': namespace}
        if sys.platform == 'darwin':
            class BsdInfo(ctypes.Structure):
                _fields_ = [(name, ctypes.c_uint32) for name in (
                    'flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid',
                    'ruid', 'rgid', 'svuid', 'svgid', 'reserved')]
                _fields_ += [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
                _fields_ += [(name, ctypes.c_uint32) for name in (
                    'nfiles', 'pgid', 'jobc', 'tdev', 'tpgid')]
                _fields_ += [('nice', ctypes.c_int32), ('start_sec', ctypes.c_uint64),
                            ('start_usec', ctypes.c_uint64)]
            lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
            info = BsdInfo()
            size = lib.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
            if size != ctypes.sizeof(info) or info.pid != pid or info.status == 5:
                return None
            return {'pid': pid, 'start': f'{info.start_sec}:{info.start_usec}',
                    'platform': 'darwin', 'process_group': info.pgid}
    except (OSError, ValueError, IndexError):
        return None
    return None


def _group_members(group):
    """Conservatively probe only the owned group, without listing other processes.

    Signal zero does not terminate or change a process. A lingering zombie group
    is treated as unresolved until the operating system reaps it.
    """
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return []
    except OSError as exc:
        raise BackendError('recovery', 'Cannot establish process-group termination') from exc
    return [group]


def _same_process(expected, actual):
    return bool(expected and actual and all(expected.get(key) == actual.get(key)
        for key in ('pid', 'start', 'platform', 'process_group', 'pid_namespace')))


def _terminate(identity, process=None, grace=3):
    if not identity:
        raise BackendError('recovery', 'Process identity is missing; no PID was signalled')
    pid = identity['pid']
    for sig, wait_seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, grace)):
        if process is not None:
            process.poll()
        actual = process_identity(pid)
        members = _group_members(pid)
        if not members:
            return
        if actual is None:
            # The child may have exited between poll() and the birth-marker read.
            # Reap our child or wait for the OS; never signal a missing leader.
            deadline = time.monotonic() + grace
            while time.monotonic() < deadline:
                if process is not None:
                    process.poll()
                if not _group_members(pid):
                    return
                time.sleep(0.05)
        if not _same_process(identity, actual) or actual['process_group'] != pid:
            raise BackendError('recovery', 'Process identity changed or descendants are ambiguous; no PID was signalled')
        if identity.get('platform') == 'linux' and identity.get('pid_namespace') != os.readlink('/proc/self/ns/pid'):
            raise BackendError('recovery', 'Foreign PID namespace; no PID was signalled')
        # Recheck immediately before signalling; never use a journal PID alone.
        if not _same_process(identity, process_identity(pid)):
            raise BackendError('recovery', 'Process identity changed; no PID was signalled')
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            if process is not None:
                process.poll()
            if not _group_members(pid):
                return
            time.sleep(0.05)
    raise BackendError('recovery', 'The owned process group did not terminate')


def _classify(message):
    text = message.casefold()
    if any(word in text for word in ('quota', 'usage limit', 'rate limit', 'insufficient_quota', '429', 'credit balance')):
        return 'quota'
    if any(word in text for word in ('unauthorized', 'not logged in', 'authentication', '401', 'permission denied', 'operation not permitted', 'os error 1', 'approval required', 'sandbox denied', 'sign in', 'login required')):
        return 'permission'
    if any(word in text for word in ('network', 'connection', 'dns', 'timed out', 'unreachable', '503', '502')):
        return 'network'
    return 'program'


def _safe_error(text):
    text = re.sub(r'\b(?:sk-[A-Za-z0-9_-]+|Bearer\s+\S+)\b', '[REDACTED]', text, flags=re.I)
    return text[-2000:].strip() or 'Codex CLI execution failed'


def _failure_message(record, stderr):
    """Prefer the actual turn failure over unrelated startup diagnostics."""
    terminal = record.get('terminal_event') or {}
    primary = terminal.get('error') if terminal.get('type') == 'turn.failed' else None
    if primary:
        return _safe_error(primary if isinstance(primary, str) else json.dumps(primary))
    if record.get('cli_error'):
        return _safe_error(record['cli_error'])
    # This optional PATH-alias setup warning can also accompany successful turns.
    # It is not evidence that the model execution was denied. Keep all other
    # stderr lines, including actual app-server initialization permission errors.
    fallback = '\n'.join(line for line in stderr.splitlines() if not line.lstrip().startswith(
        'WARNING: proceeding, even though we could not create PATH aliases:'))
    return _safe_error(fallback)


def _tail(path, size=4096):
    try:
        with Path(path).open('rb') as stream:
            stream.seek(max(0, stream.seek(0, os.SEEK_END) - size))
            return stream.read(size).decode('utf-8', 'replace')
    except OSError:
        return ''


def _receipt(record, directory, *, terminal=False):
    result = {
        'thread_id': record['session'], 'attempt_id': record['attempt_id'],
        'journal_path': str(Path(directory) / 'attempt.json'),
        'process_identity': record['process_identity'],
        'thread_observed': record.get('thread_observed', False),
        'log_path': str(Path(directory) / 'events.jsonl'),
    }
    if not terminal:
        result['event'] = 'thread.started'
    if terminal or record.get('process_exited'):
        result.update(exit_code=record.get('exit_code'),
                      process_exited=record.get('process_exited', False),
                      turn_completed=record.get('turn_completed', False))
    if record.get('terminal_event'):
        result['terminal_event'] = record['terminal_event']
    receipt = {'kind': 'codex_cli', 'tool': 'codex.process' if terminal else record['tool'],
               'agent_id': record['session'], 'result': result}
    if terminal:
        receipt['status'] = 'completed' if record['state'] == 'completed' else (
            'interrupted' if record['state'] == 'interrupted' else 'failed')
    return receipt


@contextlib.contextmanager
def _attempt_lock(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink():
        raise BackendError('recovery', 'Symlinked attempt directory is refused')
    fd = os.open(directory / 'attempt.lock', os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BackendError('recovery', 'Another worker owns this attempt', record_dir=directory) from exc
        yield directory
    finally:
        os.close(fd)


class CodexBackend:
    def __init__(self, executable='codex', poll_interval=0.1, max_log_bytes=32 * 1024 * 1024):
        self.executable = str(executable)
        self.poll_interval = max(0.02, float(poll_interval))
        self.max_log_bytes = max_log_bytes

    def run(self, prompt, workdir, record_dir, schema, session=None, on_started=None,
            on_poll=None, timeout=1800, read_only=False):
        if not isinstance(prompt, str) or not prompt.strip() or not isinstance(schema, dict):
            raise ValueError('A nonempty prompt and JSON schema object are required')
        if timeout <= 0:
            raise ValueError('timeout must be positive')
        workdir = Path(workdir).resolve(strict=True)
        record_dir = Path(record_dir).absolute()
        config = {'prompt_sha256': hashlib.sha256(prompt.encode()).hexdigest(), 'schema': schema,
                  'workdir': str(workdir), 'session': session, 'read_only': bool(read_only),
                  'executable': self.executable}
        fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
        with _attempt_lock(record_dir):
            journal = record_dir / 'attempt.json'
            if journal.exists():
                previous = _json_file(journal)
                if previous.get('request_fingerprint') != fingerprint:
                    raise BackendError('recovery', 'Attempt directory belongs to different inputs', record=previous, record_dir=record_dir)
                if previous.get('state') == 'completed' and previous.get('process_exited'):
                    return {**previous, 'record_dir': str(record_dir), 'cached': True}
                raise BackendError('recovery', 'Attempt already exists; reconcile it before creating a new attempt', record=previous, record_dir=record_dir)
            record = {'schema_version': 1, 'attempt_id': uuid.uuid4().hex,
                      'request_fingerprint': fingerprint, 'state': 'spawning',
                      'process_started': False, 'process_exited': False, 'process_identity': None,
                      'session': session, 'requested_session': session, 'thread_observed': False,
                      'created': time.time(),
                      'tool': 'codex.exec.resume' if session else 'codex.exec',
                      'turn_completed': False, 'workdir': str(workdir)}
            _private_write(record_dir / 'schema.json', json.dumps(schema) + '\n')
            _private_write(record_dir / 'prompt.txt', prompt)
            sandbox = 'read-only' if read_only else 'workspace-write'
            command = [self.executable, '-c', 'approval_policy="never"', '-c', f'sandbox_mode="{sandbox}"', 'exec']
            if session:
                command += ['resume', session]
            else:
                command += ['--sandbox', sandbox, '--cd', str(workdir)]
            command += ['--json', '--skip-git-repo-check', '--output-schema', str(record_dir / 'schema.json'),
                        '--output-last-message', str(record_dir / 'response.json'), '-']
            record['command'] = command
            _save(record_dir, record)
            for name in ('events.jsonl', 'stderr.log', 'response.json'):
                _private_write(record_dir / name, '')
            with (record_dir / 'prompt.txt').open('rb') as stdin, \
                 (record_dir / 'events.jsonl').open('ab', buffering=0) as stdout, \
                 (record_dir / 'stderr.log').open('ab', buffering=0) as stderr:
                try:
                    process = subprocess.Popen(command, cwd=workdir, stdin=stdin, stdout=stdout,
                                               stderr=stderr, start_new_session=True)
                except OSError as exc:
                    record.update(state='spawn_failed', process_exited=True, error=_safe_error(str(exc)),
                                  error_kind='permission' if isinstance(exc, PermissionError) else 'dependency', exit_code=None)
                    _save(record_dir, record)
                    raise BackendError(record['error_kind'], record['error'], record=record, record_dir=record_dir) from exc
                record.update(process_started=True, process_identity=process_identity(process.pid), state='running')
                _save(record_dir, record)
                started = time.monotonic()
                error = None
                pending = b''
                with (record_dir / 'events.jsonl').open('rb') as events:
                    try:
                        while True:
                            data = events.read(1024 * 1024)
                            if data:
                                pending += data
                                if len(pending) > 4 * 1024 * 1024:
                                    raise BackendError('format', 'Codex JSONL event exceeds the size limit')
                                while b'\n' in pending:
                                    line, pending = pending.split(b'\n', 1)
                                    if line.strip():
                                        self._event(line, record, record_dir, session, on_started)
                            code = process.poll()
                            if code is not None and not data:
                                if pending.strip():
                                    self._event(pending, record, record_dir, session, on_started)
                                break
                            if on_poll and on_poll():
                                raise BackendError('cancelled', 'Execution cancelled by its owner')
                            if time.monotonic() - started >= timeout:
                                raise BackendError('timeout', 'Codex turn exceeded its execution timeout')
                            if stdout.tell() + stderr.tell() > self.max_log_bytes:
                                raise BackendError('program', 'Codex logs exceeded the configured size limit')
                            if not data:
                                time.sleep(self.poll_interval)
                    except BaseException as exc:
                        error = exc
                        try:
                            _terminate(record['process_identity'], process)
                        except BackendError as recovery:
                            record.update(error=str(recovery), error_kind='recovery')
                            _save(record_dir, record)
                            raise BackendError('recovery', str(recovery), record=record, record_dir=record_dir) from exc
                        process.wait(timeout=5)
                if _group_members(process.pid):
                    raise BackendError('recovery', 'CLI exited with surviving process-group members; recovery is required', record=record, record_dir=record_dir)
                record.update(process_exited=True, exit_code=process.returncode)
                if error is None:
                    try:
                        if process.returncode != 0 or not record['turn_completed']:
                            message = _failure_message(record, _tail(record_dir / 'stderr.log'))
                            raise BackendError(_classify(message), message)
                        if not record['session'] or not record['thread_observed'] or not record['process_identity']:
                            raise BackendError('format', 'No actual Codex session and process identity were observed')
                        response_file = record_dir / 'response.json'
                        if response_file.is_symlink() or not response_file.is_file():
                            raise BackendError('format', 'Codex response is not a regular file')
                        if response_file.stat().st_size > self.max_log_bytes:
                            raise BackendError('format', 'Codex response exceeds the size limit')
                        response = json.loads(response_file.read_text(encoding='utf-8'))
                        if not isinstance(response, dict):
                            raise ValueError('Final response must be a JSON object')
                        record.update(state='completed', response=response)
                    except (OSError, ValueError) as exc:
                        error = BackendError('format', 'Codex did not return a valid JSON object')
                    except BackendError as exc:
                        error = exc
                if error is not None:
                    kind = error.kind if isinstance(error, BackendError) else 'cancelled'
                    record.update(state='interrupted' if kind in ('cancelled', 'timeout') else 'failed',
                                  error_kind=kind, error=_safe_error(str(error)))
                if record['session']:
                    if record['thread_observed']:
                        record['receipt'] = _receipt(record, record_dir)
                    record['terminal_receipt'] = _receipt(record, record_dir, terminal=True)
                _save(record_dir, record)
                if error is not None:
                    if not isinstance(error, Exception):
                        raise error
                    raise BackendError(record['error_kind'], record['error'], record=record, record_dir=record_dir) from error
                return {**record, 'record_dir': str(record_dir), 'cached': False}

    def _event(self, line, record, directory, requested_session, callback):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError) as exc:
            raise BackendError('format', 'Codex emitted malformed JSONL') from exc
        if not isinstance(event, dict):
            raise BackendError('format', 'Codex emitted a non-object JSONL event')
        kind = event.get('type')
        if kind == 'thread.started':
            session = event.get('thread_id')
            if not isinstance(session, str) or not session or (requested_session and session != requested_session):
                raise BackendError('format', 'Codex returned an unexpected session identifier')
            if record['session'] and record['session'] != session:
                raise BackendError('format', 'Multiple Codex sessions appeared in one attempt')
            if not record['thread_observed']:
                record['session'] = session
                record['thread_observed'] = True
                record['receipt'] = _receipt(record, directory)
                _save(directory, record)
                if callback:
                    callback(session, record['receipt'])
        elif kind in ('turn.completed', 'turn.failed'):
            record['turn_completed'] = kind == 'turn.completed'
            record['terminal_event'] = event
            if kind == 'turn.failed':
                record['cli_error'] = _safe_error(json.dumps(event.get('error', event)))
            _save(directory, record)
        elif kind == 'error':
            record['cli_error'] = _safe_error(json.dumps(event.get('message', event)))

    def recover(self, record_dir, cancel=False):
        """Recover evidence, never restart an ambiguous attempt or invent completion."""
        record_dir = Path(record_dir).absolute()
        with _attempt_lock(record_dir):
            record = _json_file(record_dir / 'attempt.json')
            if record.get('state') in {'completed', 'failed', 'interrupted', 'spawn_failed'} and record.get('process_exited'):
                return {**record, 'record_dir': str(record_dir)}
            identity = record.get('process_identity')
            if not record.get('process_started') or not identity:
                raise BackendError('recovery', 'Ambiguous spawn gap; explicit process reconciliation is required', record=record, record_dir=record_dir)
            actual = process_identity(identity['pid'])
            members = _group_members(identity['pid'])
            if members:
                if not cancel:
                    raise BackendError('recovery', 'The previous attempt is still active', record=record, record_dir=record_dir)
                if not _same_process(identity, actual):
                    raise BackendError('recovery', 'Stale process identity or ambiguous descendants; no PID was signalled', record=record, record_dir=record_dir)
                _terminate(identity)
            # A crashed owner cannot recover the wait() exit status. A completion
            # event alone is insufficient to claim a successful model execution.
            record.update(state='interrupted', process_exited=True, exit_code=None,
                          turn_completed=False, error_kind='recovery',
                          error='Prior owner ended; process termination reconciled, result not committed')
            if record.get('session'):
                if record.get('thread_observed'):
                    record['receipt'] = _receipt(record, record_dir)
                record['terminal_receipt'] = _receipt(record, record_dir, terminal=True)
            _save(record_dir, record)
            return {**record, 'record_dir': str(record_dir)}
