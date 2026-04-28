-- Create materialized views for O(1) owner NFT count lookups.
-- Unique indexes allow REFRESH CONCURRENTLY (non-blocking reads during refresh).

-- NFT owner counts
CREATE MATERIALIZED VIEW nft_owner_nft_counts AS
  SELECT LOWER(owner_address) AS lower_owner_address,
         COUNT(*)             AS total_count
  FROM nfts
  GROUP BY LOWER(owner_address);

CREATE UNIQUE INDEX ON nft_owner_nft_counts(lower_owner_address);

-- NFTClass owner counts
CREATE MATERIALIZED VIEW nftclass_owner_nftclass_counts AS
  SELECT LOWER(owner_address) AS lower_owner_address,
         COUNT(*)             AS total_count
  FROM nft_classes
  WHERE owner_address IS NOT NULL
  GROUP BY LOWER(owner_address);

CREATE UNIQUE INDEX ON nftclass_owner_nftclass_counts(lower_owner_address);
