import time
import random
import logging

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class OCCConflict(Exception):
    pass

class TransientError(Exception):
    pass


def write_with_occ(
    es,
    entity_id,
    document,
    source_ts,
    ts_field,
    max_retries=MAX_RETRIES
):
    """
    Generic OCC based read-modify-write.

    Parameters
    ----------
    es : Elasticsearch client wrapper

    entity_id : str

    document : dict
        Incoming CDC document

    source_ts : int
        Source system timestamp

    ts_field : str
        e.g. actor_source_ts,
             malware_source_ts

    """
    for attempt in range(max_retries):
        current = es.get(entity_id)
        # ---------- Business Ordering ----------

        if current:
            current_ts = current.get(ts_field)
            if current_ts is not None and current_ts >= source_ts:
                logger.info(
                    f"Skipping stale event "
                    f"entity={entity_id}, "
                    f"incoming_ts={source_ts}, "
                    f"current_ts={current_ts}"
                )
                return "STALE"
        try:
            # ---------- Insert ----------
            # For creation race — make atomic
            if current is None:
                es.create(
                    entity_id,
                    document,
                    op_type='create'  # raises ConflictError if doc already exists
                )
                return "SUCCESS"            
            # ---------- Update with OCC ----------
            es.update(
                entity_id,
                document,
                seq_no=current["_seq_no"],
                primary_term=current["_primary_term"]
            )
            return "SUCCESS"
        except OCCConflict:
            logger.warning(
                f"OCC conflict "
                f"entity={entity_id}, "
                f"attempt={attempt+1}"
            )
            if attempt < max_retries - 1:
                sleep_time = (
                    0.1 * (2 ** attempt)
                    +
                    random.uniform(0, 0.5)
                )
                time.sleep(sleep_time)
                continue
            raise OCCConflict(
                f"Max OCC retries exceeded "
                f"for {entity_id}"
            )

        except TransientError as e:
            logger.warning(
                f"Transient error "
                f"entity={entity_id}, "
                f"attempt={attempt+1}, "
                f"error={e}"
            )
            if attempt < max_retries - 1:
                sleep_time = (
                    0.1 * (2 ** attempt)
                    +
                    random.uniform(0, 0.5)
                )
                time.sleep(sleep_time)
                continue
            raise
        except Exception:
            raise