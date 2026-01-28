postgres=# CREATE EXTENSION IF NOT EXISTS pageinspect;

postgres=# DROP TABLE IF EXISTS public.lpdead_demo;

CREATE TABLE public.lpdead_demo (
  id  bigserial PRIMARY KEY,
  k   int NOT NULL,
  pad text NOT NULL
) WITH (autovacuum_enabled = off);

CREATE INDEX lpdead_demo_k_idx ON public.lpdead_demo(k);

INSERT INTO public.lpdead_demo(k, pad)
SELECT g, repeat('x', 200)
FROM generate_series(1, 300000) g;

ANALYZE public.lpdead_demo;
DROP TABLE
CREATE TABLE
CREATE INDEX


INSERT 0 300000
ANALYZE
postgres=#
postgres=#
postgres=#
postgres=# DELETE FROM public.lpdead_demo
WHERE k % 10 = 0;   -- ~%10
DELETE 30000
postgres=#
postgres=# SELECT sum(length(pad)) FROM public.lpdead_demo WHERE k BETWEEN 1 AND 300000;
SELECT sum(length(pad)) FROM public.lpdead_demo WHERE k BETWEEN 1 AND 300000;
   sum
----------
 54000000
(1 row)

   sum
----------
 54000000
(1 row)

postgres=# WITH idx AS (
  SELECT 'public.lpdead_demo_k_idx'::regclass::text AS i_txt
),
sz AS (
  SELECT i_txt, (pg_relation_size(i_txt::regclass)/8192 - 1)::int AS maxblk
  FROM idx
)
SELECT blkno,
       (demo_batch.bt_page_stats(i_txt, blkno)).live_items AS live,
       (demo_batch.bt_page_stats(i_txt, blkno)).dead_items AS dead
FROM sz, LATERAL generate_series(1, maxblk) AS blkno
WHERE (demo_batch.bt_page_stats(i_txt, blkno)).type = 'l'
  AND (demo_batch.bt_page_stats(i_txt, blkno)).dead_items > 0
ORDER BY dead DESC
LIMIT 20;
 blkno | live | dead
-------+------+------
    28 |  330 |   37
    33 |  330 |   37
    25 |  330 |   37
    26 |  330 |   37
    31 |  330 |   37
     8 |  330 |   37
     2 |  330 |   37
    23 |  330 |   37
    11 |  330 |   37
     6 |  330 |   37
    13 |  330 |   37
    30 |  330 |   37
    15 |  330 |   37
    16 |  330 |   37
     5 |  330 |   37
    18 |  330 |   37
    10 |  330 |   37
    20 |  330 |   37
    21 |  330 |   37
    35 |  330 |   37
(20 rows)

                                                               ^
postgres=#
postgres=# SELECT itemoffset, dead, htid, tids, itemlen, data
FROM demo_batch.bt_page_items('public.lpdead_demo_k_idx'::text, 28)
WHERE dead IS TRUE
LIMIT 100;
 itemoffset | dead |   htid   | tids | itemlen |          data
------------+------+----------+------+---------+-------------------------
          5 | t    | (288,16) |      |      16 | 30 25 00 00 00 00 00 00
         15 | t    | (288,26) |      |      16 | 3a 25 00 00 00 00 00 00
         25 | t    | (289,3)  |      |      16 | 44 25 00 00 00 00 00 00
         35 | t    | (289,13) |      |      16 | 4e 25 00 00 00 00 00 00
         45 | t    | (289,23) |      |      16 | 58 25 00 00 00 00 00 00
         55 | t    | (289,33) |      |      16 | 62 25 00 00 00 00 00 00
         65 | t    | (290,10) |      |      16 | 6c 25 00 00 00 00 00 00
         75 | t    | (290,20) |      |      16 | 76 25 00 00 00 00 00 00
         85 | t    | (290,30) |      |      16 | 80 25 00 00 00 00 00 00
         95 | t    | (291,7)  |      |      16 | 8a 25 00 00 00 00 00 00
        105 | t    | (291,17) |      |      16 | 94 25 00 00 00 00 00 00
        115 | t    | (291,27) |      |      16 | 9e 25 00 00 00 00 00 00
        125 | t    | (292,4)  |      |      16 | a8 25 00 00 00 00 00 00
        135 | t    | (292,14) |      |      16 | b2 25 00 00 00 00 00 00
        145 | t    | (292,24) |      |      16 | bc 25 00 00 00 00 00 00
        155 | t    | (293,1)  |      |      16 | c6 25 00 00 00 00 00 00
        165 | t    | (293,11) |      |      16 | d0 25 00 00 00 00 00 00
        175 | t    | (293,21) |      |      16 | da 25 00 00 00 00 00 00
        185 | t    | (293,31) |      |      16 | e4 25 00 00 00 00 00 00
        195 | t    | (294,8)  |      |      16 | ee 25 00 00 00 00 00 00
        205 | t    | (294,18) |      |      16 | f8 25 00 00 00 00 00 00
        215 | t    | (294,28) |      |      16 | 02 26 00 00 00 00 00 00
        225 | t    | (295,5)  |      |      16 | 0c 26 00 00 00 00 00 00
        235 | t    | (295,15) |      |      16 | 16 26 00 00 00 00 00 00
        245 | t    | (295,25) |      |      16 | 20 26 00 00 00 00 00 00
        255 | t    | (296,2)  |      |      16 | 2a 26 00 00 00 00 00 00
        265 | t    | (296,12) |      |      16 | 34 26 00 00 00 00 00 00
        275 | t    | (296,22) |      |      16 | 3e 26 00 00 00 00 00 00
        285 | t    | (296,32) |      |      16 | 48 26 00 00 00 00 00 00
        295 | t    | (297,9)  |      |      16 | 52 26 00 00 00 00 00 00
        305 | t    | (297,19) |      |      16 | 5c 26 00 00 00 00 00 00
        315 | t    | (297,29) |      |      16 | 66 26 00 00 00 00 00 00
        325 | t    | (298,6)  |      |      16 | 70 26 00 00 00 00 00 00
        335 | t    | (298,16) |      |      16 | 7a 26 00 00 00 00 00 00
        345 | t    | (298,26) |      |      16 | 84 26 00 00 00 00 00 00
        355 | t    | (299,3)  |      |      16 | 8e 26 00 00 00 00 00 00
        365 | t    | (299,13) |      |      16 | 98 26 00 00 00 00 00 00
(37 rows)
