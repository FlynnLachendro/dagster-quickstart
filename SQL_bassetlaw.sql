-- BigQuery SQL Script to Enrich Planning Applications with Geographical Data

-- Using a LEFT JOIN ensures that all entries from the planning applications are included
-- in the result, even if there's no matching address in the delivery points.
-- This is crucial as it maintains the integrity of the planning applications data.
SELECT
  p.*,  -- Select all columns from the planning applications
  d.geom  -- Include the geographic data from the delivery point table
FROM
  `dummy_datasets.bassetlaw_planning_applications` p
LEFT JOIN
  `dummy_datasets.bassetlaw_deliverypoint` d
ON
  p.address = d.address  -- Join condition based on matching addresses
