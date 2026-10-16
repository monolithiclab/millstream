-- Migration 10: the reads of the HTTP API (spec section 12.1). A delivery's attempts are its run's events, and an
-- item's deliveries are found by item.
CREATE INDEX events_run ON events (pipeline, run_id, seq);
CREATE INDEX deliveries_item ON deliveries (pipeline, item_id);
