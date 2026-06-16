import time
import random
import logging

logger = logging.getLogger(__name__)


class ConflictError(Exception):
    pass

class TransientError(Exception):
    pass


def write_with_occ(es, entity_id, document, source_ts, stream_type, max_retries=3):
    for attempt in range(max_retries):
        current = es.get(entity_id)

        if current:
            ts_field = 'actor_source_ts' if stream_type == 'actor' else 'malware_source_ts'
            if current.get(ts_field, '') >= source_ts:
                logger.info(f"skipping stale event for {entity_id}")
                return 'skipped'

        try:
            if current is None:
                es.create(entity_id, document)
            else:
                es.update(
                    entity_id,
                    document,
                    seq_no=current['_seq_no'],
                    primary_term=current['_primary_term']
                )
            return 'success'

        except ConflictError:
            logger.info(f"conflict on {entity_id} attempt={attempt}, retrying")
            if attempt < max_retries - 1:
                time.sleep(0.1 * (2 ** attempt) + random.uniform(0, 0.5))
            continue

        except TransientError as e:
            logger.warning(f"transient error on {entity_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(0.1 * (2 ** attempt) + random.uniform(0, 0.5))
            continue

    return 'failed'