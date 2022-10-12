CREATE TABLE searches (
	timestamp_micros BIGINT,
	user__id BIGINT,
	query_id STRING,
	raw_query STRING,
	results__document_id BIGINT[],
	results__position BIGINT[],
	results__score DOUBLE[]
);

CREATE TABLE clicks (
	timestamp_micros BIGINT,
	query_id STRING,
	document_id BIGINT
);

