# with changes to the model, we need to add new columns to the postgres table that records scored data
import sys, os
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(THIS_DIR, '../../'))
sys.path.append(os.path.join(THIS_DIR, '../../model/'))
import configUtils as cu
import sqlUtils as su


cfg_file = cu.findConfig()
cfg = cu.parseConfig(cfg_file)
engine = su.makeEngine(cfg)

newColumnsStr = """
ALTER TABLE IF EXISTS public."scoredData"
    ADD COLUMN IF NOT EXISTS "timeElapsedMin" integer;
"""

columnsStr = """
SELECT *
FROM public."scoredData"
WHERE false
"""

with engine.connect() as conn:
  conn.execute(newColumnsStr)
  res = conn.execute(columnsStr)
  print("columns:")
  [print(f"\t{i}") for i in res.keys()]
  