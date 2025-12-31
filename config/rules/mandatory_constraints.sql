"SBU_Name" != '0'
AND "SBU_Name" NOT IN (
  'Common',
  'Mumbai Ref',
  'Renewable Energy',
  'Visakh Ref'
)
AND "category" NOT LIKE '%O%'