from __future__ import absolute_import

import csv
import logging
import six
import tempfile
from contextlib import contextmanager
from django.core.files.base import ContentFile
from django.db import transaction, IntegrityError

from sentry.models import DEFAULT_BLOB_SIZE, File, FileBlob, FileBlobIndex
from sentry.tasks.base import instrumented_task
from sentry.utils import metrics
from sentry.utils.sdk import capture_exception

from .base import ExportError, ExportQueryType, SNUBA_MAX_RESULTS
from .models import ExportedData, ExportedDataBlob
from .utils import convert_to_utf8, snuba_error_handler
from .processors.discover import DiscoverProcessor
from .processors.issues_by_tag import IssuesByTagProcessor


logger = logging.getLogger(__name__)


@instrumented_task(name="sentry.data_export.tasks.assemble_download", queue="data_export")
def assemble_download(
    data_export_id,
    export_limit=1000000,
    batch_size=SNUBA_MAX_RESULTS,
    offset=0,
    environment_id=None,
    **kwargs
):
    try:
        data_export = ExportedData.objects.get(id=data_export_id)
        if offset == 0:
            logger.info("dataexport.start", extra={"data_export_id": data_export_id})
            metrics.incr("dataexport.start", tags={"success": True}, sample_rate=1.0)
        logger.info("dataexport.run", extra={"data_export_id": data_export_id, "offset": offset})
    except ExportedData.DoesNotExist as error:
        if offset == 0:
            metrics.incr("dataexport.start", tags={"success": False}, sample_rate=1.0)
        capture_exception(error)
        return

    # if there is an export limit, the last batch should only return up to the export limit
    if export_limit is not None:
        batch_size = min(batch_size, max(export_limit - offset, 0))

    try:
        # NOTE: the processors don't have an unified interface at the moment
        # so this function handles it for us
        headers, rows = get_processed(data_export, environment_id, batch_size, offset)

        # starting position for the next batch
        next_offset = offset + len(rows)

        with tempfile.TemporaryFile() as tf:
            writer = csv.DictWriter(tf, headers, extrasaction="ignore")
            if offset == 0:
                writer.writeheader()
            writer.writerows(rows)
            tf.seek(0)

            store_export_chunk_as_blob(data_export, offset, tf)
    except ExportError as error:
        return data_export.email_failure(message=six.text_type(error))
    except BaseException as error:
        metrics.incr("dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0)
        logger.info(
            "dataexport.error: {}".format(six.text_type(error)),
            extra={"query": data_export.payload, "org": data_export.organization_id},
        )
        capture_exception(error)
        return data_export.mail_failure(message="Internal processing failure")

    if (
        rows
        and len(rows) >= batch_size  # only == is needed, >= to be safe
        and (export_limit is None or next_offset < export_limit)
    ):
        assemble_download.delay(
            data_export_id,
            export_limit=export_limit,
            batch_size=batch_size,
            offset=next_offset,
            environment_id=environment_id,
        )
    else:
        merge_export_blobs.delay(data_export_id)


@transaction.atomic()
def store_export_chunk_as_blob(data_export, row_offset, fileobj, blob_size=DEFAULT_BLOB_SIZE):
    # adapted from `putfile` in  `src/sentry/models/file.py`
    byte_offset = 0
    while True:
        contents = fileobj.read(blob_size)
        if not contents:
            break

        blob_fileobj = ContentFile(contents)
        blob = FileBlob.from_file(blob_fileobj, logger=logger)
        ExportedDataBlob.objects.create(
            data_export=data_export, blob=blob, row_offset=row_offset, byte_offset=byte_offset
        )

        byte_offset += blob.size


@instrumented_task(name="sentry.data_export.tasks.merge_blobs", queue="data_export")
def merge_export_blobs(data_export_id, **kwargs):
    try:
        data_export = ExportedData.objects.get(id=data_export_id)
    except ExportedData.DoesNotExist as error:
        capture_exception(error)
        return

    # adapted from `putfile` in  `src/sentry/models/file.py`
    try:
        with transaction.atomic():
            # NOTE: A checksum is too expensive to compute here for large files since
            # we would have to read the entire file.
            file = File.objects.create(
                name=data_export.file_name, type="export.csv", headers={"Content-Type": "text/csv"},
            )
            size = 0
            for blob in ExportedDataBlob.objects.filter(data_export=data_export).order_by(
                "row_offset", "byte_offset"
            ):
                FileBlobIndex.objects.create(file=file, blob=blob.blob, offset=size)
                size += blob.blob.size
            file.size = size
            data_export.finalize_upload(file=file)
            logger.info("dataexport.end", extra={"data_export_id": data_export_id})
            metrics.incr("dataexport.end", sample_rate=1.0)
    except IntegrityError as error:
        metrics.incr("dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0)
        logger.info(
            "dataexport.error: {}".format(six.text_type(error)),
            extra={"query": data_export.payload, "org": data_export.organization_id},
        )
        capture_exception(error)
        return data_export.email_failure(message="Failed to save assembled file")


def get_processed(data_export, environment_id, batch_size, offset):
    if data_export.query_type == ExportQueryType.ISSUES_BY_TAG:
        processor = issues_by_tag_processor(data_export, environment_id)
        processed = process_issues_by_tag(processor, batch_size, offset)
    elif data_export.query_type == ExportQueryType.DISCOVER:
        processor = discover_processor(data_export, environment_id)
        processed = process_discover(processor, batch_size, offset)
    else:
        logger.warn(
            "dataexport.warn",
            extra={"warning": "unknown_query_type", "query_type": data_export.query_type},
        )
        raise ExportError("Invalid query. Unexpected query type.")
    return processor.header_fields, processed


def issues_by_tag_processor(data_export, environment_id):
    payload = data_export.query_info
    return IssuesByTagProcessor(
        project_id=payload["project"][0],
        group_id=payload["group"],
        key=payload["key"],
        environment_id=environment_id,
    )


def process_issues_by_tag(processor, limit, offset):
    with snuba_error_handler(logger=logger):
        gtv_list_unicode = processor.get_serialized_data(limit=limit, offset=offset)
        # TODO(python3): Remove next line once the 'csv' module has been updated to Python 3
        # See associated comment in './utils.py'
        return convert_to_utf8(gtv_list_unicode)


def discover_processor(data_export, environment_id):
    return DiscoverProcessor(
        discover_query=data_export.query_info, organization_id=data_export.organization_id,
    )


def process_discover(processor, limit, offset):
    with snuba_error_handler(logger=logger):
        raw_data_unicode = processor.data_fn(limit=limit, offset=offset)["data"]
        # TODO(python3): Remove next line once the 'csv' module has been updated to Python 3
        # See associated comment in './utils.py'
        raw_data = convert_to_utf8(raw_data_unicode)
        return processor.handle_fields(raw_data)


@contextmanager
def write_csv(headers, rows, write_headers):
    with tempfile.TemporaryFile() as tf:
        writer = csv.DictWriter(tf, headers, extrasaction="ignore")
        if write_headers:
            writer.writeheader()
        writer.writerows(rows)
        tf.seek(0)
        yield tf


'''
    # Create a temporary file
    try:
        with tempfile.TemporaryFile() as tf:
            # Process the query based on its type
            if data_export.query_type == ExportQueryType.ISSUES_BY_TAG:
                process_issues_by_tag(
                    data_export=data_export,
                    file=tf,
                    export_limit=export_limit,
                    batch_size=batch_size,
                    environment_id=environment_id,
                )
            elif data_export.query_type == ExportQueryType.DISCOVER:
                process_discover(
                    data_export=data_export,
                    file=tf,
                    export_limit=export_limit,
                    batch_size=batch_size,
                    environment_id=environment_id,
                )
            # Create a new File object and attach it to the ExportedData
            tf.seek(0)
            try:
                with transaction.atomic():
                    file = File.objects.create(
                        name=data_export.file_name,
                        type="export.csv",
                        headers={"Content-Type": "text/csv"},
                    )
                    file.putfile(tf, logger=logger)
                    data_export.finalize_upload(file=file)
                    logger.info("dataexport.end", extra={"data_export_id": data_export_id})
                    metrics.incr("dataexport.end", sample_rate=1.0)
            except IntegrityError as error:
                metrics.incr(
                    "dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0
                )
                logger.info(
                    "dataexport.error: {}".format(six.text_type(error)),
                    extra={"query": data_export.payload, "org": data_export.organization_id},
                )
                capture_exception(error)
                raise ExportError("Failed to save the assembled file")
    except ExportError as error:
        return data_export.email_failure(message=six.text_type(error))
    except BaseException as error:
        metrics.incr("dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0)
        logger.info(
            "dataexport.error: {}".format(six.text_type(error)),
            extra={"query": data_export.payload, "org": data_export.organization_id},
        )
        capture_exception(error)
        return data_export.email_failure(message="Internal processing failure")


def process_issues_by_tag(data_export, file, export_limit, batch_size, environment_id):
    """
    Convert the tag query to a CSV, writing it to the provided file.
    """
    payload = data_export.query_info
    try:
        processor = IssuesByTagProcessor(
            project_id=payload["project"][0],
            group_id=payload["group"],
            key=payload["key"],
            environment_id=environment_id,
        )
    except ExportError as error:
        metrics.incr("dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0)
        logger.info("dataexport.error: {}".format(six.text_type(error)))
        capture_exception(error)
        raise error

    writer = create_writer(file, processor.header_fields)
    iteration = 0
    with snuba_error_handler(logger=logger):
        is_completed = False
        while not is_completed:
            offset = batch_size * iteration
            next_offset = batch_size * (iteration + 1)
            is_exceeding_limit = export_limit and export_limit < next_offset
            gtv_list_unicode = processor.get_serialized_data(limit=batch_size, offset=offset)
            # TODO(python3): Remove next line once the 'csv' module has been updated to Python 3
            # See associated comment in './utils.py'
            gtv_list = convert_to_utf8(gtv_list_unicode)
            if is_exceeding_limit:
                # Since the next offset will pass the export_limit, just write the remainder
                writer.writerows(gtv_list[: export_limit % batch_size])
            else:
                writer.writerows(gtv_list)
                iteration += 1
            # If there are no returned results, or we've passed the export_limit, stop iterating
            is_completed = len(gtv_list) == 0 or is_exceeding_limit


def process_discover(data_export, file, export_limit, batch_size, environment_id):
    """
    Convert the discovery query to a CSV, writing it to the provided file.
    """
    try:
        processor = DiscoverProcessor(
            discover_query=data_export.query_info, organization_id=data_export.organization_id
        )
    except ExportError as error:
        metrics.incr("dataexport.error", tags={"error": six.text_type(error)}, sample_rate=1.0)
        logger.info("dataexport.error: {}".format(six.text_type(error)))
        capture_exception(error)
        raise error

    writer = create_writer(file, processor.header_fields)
    iteration = 0
    with snuba_error_handler(logger=logger):
        is_completed = False
        while not is_completed:
            offset = batch_size * iteration
            next_offset = batch_size * (iteration + 1)
            is_exceeding_limit = export_limit and export_limit < next_offset
            raw_data_unicode = processor.data_fn(offset=offset, limit=batch_size)["data"]
            # TODO(python3): Remove next line once the 'csv' module has been updated to Python 3
            # See associated comment in './utils.py'
            raw_data = convert_to_utf8(raw_data_unicode)
            raw_data = processor.handle_fields(raw_data)
            if is_exceeding_limit:
                # Since the next offset will pass the export_limit, just write the remainder
                writer.writerows(raw_data[: export_limit % batch_size])
            else:
                writer.writerows(raw_data)
                iteration += 1
            # If there are no returned results, or we've passed the export_limit, stop iterating
            is_completed = len(raw_data) == 0 or is_exceeding_limit


def create_writer(file, fields):
    writer = csv.DictWriter(file, fields, extrasaction="ignore")
    writer.writeheader()
    return writer
'''
