SELECT

count(1) as TRANS_CNT

FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT 
INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT
ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT

WHERE TEST_CAL_DT.WEEK_BEG_DT > date'2001-09-09'