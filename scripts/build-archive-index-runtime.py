"""Build the optional Windows index runtime with 64-bit Python file offsets.

Uses the already installed indexed_gzip sources and a local zlib source tree.
Produces an isolated candidate; does not replace or enable the production reader.
"""
import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, default=Path('work/archive-index-runtime'))
    parser.add_argument('--build', type=Path, default=Path('work/archive-index-build'))
    parser.add_argument('--build-runtime', type=Path, default=Path('work/archive-index-build-runtime'))
    parser.add_argument('--zlib-source', type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != 'win32':
        parser.error('this correction targets Windows C long offsets')
    runtime, build = args.runtime.resolve(), args.build.resolve()
    original, source = runtime/'indexed_gzip', build/'indexed_gzip'
    source.mkdir(parents=True, exist_ok=True)
    for name in ('indexed_gzip.c', 'zran.c', 'zran_file_util.c', 'zran.h', 'zran_file_util.h'):
        shutil.copyfile(original/name, source/name)
    target = source/'zran_file_util.c'
    before = target.read_bytes()
    start = before.index(b'int64_t _ftell_python(')
    end = before.index(b'/*', start)
    # Change only ftell, leaving the independent boolean seekable conversion.
    function = before[start:end]
    if function.count(b'PyLong_AsLong(data)') != 1:
        raise ValueError('unsupported indexed_gzip ftell implementation')
    after = before[:start]+function.replace(b'PyLong_AsLong(data)', b'PyLong_AsLongLong(data)')+before[end:]
    # C fread/fwrite return zero for zero-sized requests. Empty index exports
    # can request size=0; dividing the Python byte count by size crashes Windows.
    for name in (b'_fread_python', b'_fwrite_python'):
        offset = after.index(b'size_t '+name+b'(')
        brace = after.index(b'{', offset)+1
        after = after[:brace]+b'\n    if (size == 0 || nmemb == 0) return 0;\n'+after[brace:]
    target.write_bytes(after)
    # Observe the decoded bytes while building the index, rather than running
    # another decompression pass solely to locate the committed tar prefix.
    observer = b'''
int gapi_index_output(PyObject *f, const void *data, size_t size, uint64_t offset) {
    if (f == NULL || size == 0) return 0;
    _ZRAN_FILE_UTIL_ACQUIRE_GIL
    int status = 0;
    int present = PyObject_HasAttrString(f, "index_output");
    if (present < 0) status = -1;
    else if (present) {
        PyObject *payload = PyBytes_FromStringAndSize((const char *)data, (Py_ssize_t)size);
        PyObject *result = payload == NULL ? NULL : PyObject_CallMethod(f, "index_output", "OK",
            payload, (unsigned long long)offset);
        Py_XDECREF(payload);
        if (result == NULL) { PyErr_Clear(); status = -1; }
        else Py_DecRef(result);
    }
    _ZRAN_FILE_UTIL_RELEASE_GIL
    return status;
}
int gapi_sequential_enabled(PyObject *f) {
    if (f == NULL) return 0;
    _ZRAN_FILE_UTIL_ACQUIRE_GIL
    PyObject *value = PyObject_GetAttrString(f, "gapi_sequential");
    int result = 0;
    if (value == NULL) PyErr_Clear();
    else { result = PyObject_IsTrue(value); Py_DecRef(value); }
    _ZRAN_FILE_UTIL_RELEASE_GIL
    return result == 1;
}
'''
    target.write_bytes(after+observer)
    header=source/'zran_file_util.h'
    header.write_bytes(header.read_bytes()+b'\nint gapi_index_output(PyObject *, const void *, size_t, uint64_t);\nint gapi_sequential_enabled(PyObject *);\n')
    zran_path = source/'zran.c'
    zran_before = zran_path.read_bytes()
    zran_after = zran_before
    for declaration in (b'uint32_t data_size   = index->spacing * 4;',
                        b'uint64_t discard_size    = index->spacing * 4;'):
        if zran_after.count(declaration) != 1:
            raise ValueError('unsupported indexed_gzip spacing buffer implementation')
        # These are bounded streaming/ring buffers, not the point spacing.
        # Keep the existing 32 KiB dictionary and point-creation conditions.
        zran_after = zran_after.replace(declaration,
            declaration.split(b'=')[0]+b'= 8 * 1024 * 1024;')
    newline=b'\r\n' if b'\r\n' in zran_after else b'\n'
    anchor=newline.join((b'        cmp_offset   += bytes_consumed;',
        b'        uncmp_offset += bytes_output;',b'        data_offset'))
    if zran_after.count(anchor)!=1:
        raise ValueError('unsupported index expansion observer position')
    zran_after=zran_after.replace(anchor,
        b'        if (gapi_index_output(index->f, data + data_offset, bytes_output, uncmp_offset) != 0) goto fail;\n'+anchor)
    # Opt-in sequential readers retain one inflater and its bounded input
    # buffer. Ordinary indexed_gzip users keep the original random-read path.
    text=zran_after.decode().replace('\r\n','\n')
    header=source/'zran.h'
    htext=header.read_text()
    needle='    uint8_t  validating;'
    if htext.count(needle)!=1:raise ValueError('unsupported native index structure')
    header.write_text(htext.replace(needle,needle+'\n    void *gapi_stream;\n    uint64_t gapi_next, gapi_cmp, gapi_uncmp, gapi_filepos;'))
    helper='''
static void gapi_drop_stream(zran_index_t *index) {
    if (index->gapi_stream == NULL) return;
    z_stream *stream = (z_stream *)index->gapi_stream;
    if (stream->state != NULL) inflateEnd(stream);
    free(stream); index->gapi_stream = NULL;
    free(index->readbuf); index->readbuf = NULL;
    index->readbuf_offset = index->readbuf_end = 0;
}
'''
    text=text.replace('\nint zran_init(',helper+'\nint zran_init(',1)
    text=text.replace('    index->readbuf              = NULL;',
        '    index->gapi_stream = NULL;\n    index->gapi_next = UINT64_MAX;\n    index->readbuf              = NULL;',1)
    text=text.replace('    zran_log("zran_free\\n");',
        '    gapi_drop_stream(index);\n    zran_log("zran_free\\n");',1)
    start=text.index('int64_t zran_read(');end=text.index('\nint zran_export_index(',start)
    body=text[start:end]
    body=body.replace('    z_stream      strm;',
        '    z_stream stack_strm;\n    z_stream *strm = &stack_strm;\n    int gapi_enabled = 0;')
    body=body.replace('&strm,','strm,')
    gate='''
    gapi_enabled = gapi_sequential_enabled(index->f);
    if (gapi_enabled) {
        if (index->gapi_stream != NULL && index->gapi_next == index->uncmp_seek_offset &&
            index->gapi_filepos == (uint64_t)ftell_(index->fd, index->f)) {
            strm = (z_stream *)index->gapi_stream;
            cmp_offset = index->gapi_cmp; uncmp_offset = index->gapi_uncmp;
            first_inflate = 0;
            goto gapi_ready;
        }
        gapi_drop_stream(index);
        index->gapi_stream = calloc(1, sizeof(z_stream));
        if (index->gapi_stream == NULL) goto fail;
        strm = (z_stream *)index->gapi_stream;
    }
'''
    body=body.replace('    zran_log("zran_read(',gate+'\n    zran_log("zran_read(',1)
    body=body.replace('    total_read = 0;','gapi_ready:\n    total_read = 0;',1)
    cleanup=body.index('    ret = _zran_inflate(index,',body.index('A final call to _zran_inflate'))
    body=body[:cleanup]+'''    if (gapi_enabled) {
        if (ret == ZRAN_INFLATE_EOF) gapi_drop_stream(index);
        else {
            index->gapi_next = index->uncmp_seek_offset + total_read;
            index->gapi_cmp = cmp_offset; index->gapi_uncmp = uncmp_offset;
            index->gapi_filepos = (uint64_t)ftell_(index->fd, index->f);
        }
        goto gapi_read_success;
    }
'''+body[cleanup:]
    body=body.replace('    index->uncmp_seek_offset += total_read;',
        'gapi_read_success:\n    index->uncmp_seek_offset += total_read;',1)
    body=body.replace('fail:\n','fail:\n    if (gapi_enabled) gapi_drop_stream(index);\n',1)
    if 'z_stream      strm;' in body or body.count('gapi_ready:')!=1:
        raise ValueError('unsupported native sequential read implementation')
    text=text[:start]+body+text[end:]
    zran_after=text.encode()
    zran_path.write_bytes(zran_after)
    sys.path.insert(0, str(args.build_runtime.resolve()))
    from setuptools import Extension, setup
    zlib = args.zlib_source.resolve()
    sys.argv = ['setup.py', 'build_ext', '--build-lib', str(build/'lib'),
                '--build-temp', str(build/'obj')]
    extension = Extension('indexed_gzip.indexed_gzip',
        sources=[str(source/name) for name in ('indexed_gzip.c', 'zran.c', 'zran_file_util.c')]
                +[str(path) for path in sorted(zlib.glob('*.c'))],
        include_dirs=[str(source), str(zlib)],
        define_macros=[('CYTHON_LIMITED_API', '1'), ('Py_LIMITED_API', '0x030b0000')],
        py_limited_api=True)
    setup(name='gapi-indexed-gzip-windows-offset-fix', version='1.10.3',
          ext_modules=[extension])
    shutil.copyfile(original/'__init__.py', build/'lib/indexed_gzip/__init__.py')
    binary = build/'lib/indexed_gzip/indexed_gzip.pyd'
    receipt = {'base_version': 'indexed_gzip 1.10.3',
        'patch': '64-bit ftell; zero-sized I/O guard; 8 MiB buffers independent of spacing',
        'index_output_observer': 'decoded-bytes-and-offset/v1',
        'sequential_inflater': 'opt-in-persistent-inflater/v1',
        'original_zran_sha256': hashlib.sha256(zran_before).hexdigest(),
        'patched_zran_sha256': hashlib.sha256(zran_after).hexdigest(),
        'original_source_sha256': hashlib.sha256(before).hexdigest(),
        'patched_source_sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
        'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
        'zlib_sources_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in sorted(zlib.glob('*.[ch]'))},
        'production_enabled': False}
    (build/'build-receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
