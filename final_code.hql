USE draftd01;
set hive.cli.print.header=true;

--profiling (not saved to a file)
select min(earliest_case_dt) as min, max(earliest_case_dt) as max from covid;
select sex, count(sex) as c from covid_nonulls group by sex order by c;
select race_ethnic, count(race_ethnic) as c from covid_nonulls group by race_ethnic order by c desc;
select current_status, count(current_status) as c from covid_nonulls group by current_status order by c desc;

--data to graph
--FIRST QUERY_________________________________________________________
insert overwrite directory 'project/output1' select age_group, race_ethnic, count(race_ethnic) as c from covid_nonulls group by race_ethnic, age_group order by age_group, c;

--SECOND QUERY (hospital deaths)__________________________________________________________
WITH t
AS (
	SELECT CASE WHEN hosp_yn = "Yes" AND death_yn = "Yes" THEN 1 ELSE 0 END hosp_death,
		CASE WHEN hosp_yn = "Yes" AND death_yn = "No" THEN 1 ELSE 0 END hosp_nodeath,
		CASE WHEN hosp_yn = "No" AND death_yn = "Yes" THEN 1 ELSE 0 END death_nohosp,
		CASE WHEN hosp_yn = "Missing" OR death_yn = "Missing" OR hosp_yn = "Unknown" OR death_yn = "Unknown" THEN 1 ELSE 0 END missing_hospdeath
	FROM covid_nonulls
	)
INSERT OVERWRITE DIRECTORY 'project/output2' SELECT sum(hosp_death) hosp_death
	,sum(hosp_nodeath) hosp_nodeath
	,sum(death_nohosp) death_nohosp
	,sum(missing_hospdeath) missing_hospdeath
FROM t;

--THIRD QUERY (icu deaths)__________________________________________________________
WITH t
AS (
	SELECT CASE WHEN icu_yn = "Yes" AND death_yn = "Yes" THEN 1 ELSE 0 END icu_death,
		CASE WHEN icu_yn = "Yes" AND death_yn = "No" THEN 1 ELSE 0 END icu_nodeath,
		CASE WHEN icu_yn = "No" AND death_yn = "Yes" THEN 1 ELSE 0 END death_noicu,
		CASE WHEN icu_yn = "Missing" OR death_yn = "Missing" OR icu_yn = "Unknown" OR death_yn = "Unknown" THEN 1 ELSE 0 END missing_icudeath
	FROM covid_nonulls
	)
INSERT OVERWRITE DIRECTORY 'project/output3' SELECT sum(icu_death) icu_death
	,sum(icu_nodeath) icu_nodeath
	,sum(death_noicu) death_noicu
	,sum(missing_icudeath) missing_icudeath
FROM t;

--FOURTH QUERY_________________________________________
insert overwrite directory 'project/output4' select year(earliest_case_dt) as y, month(earliest_case_dt) as m, race_ethnic, count(*) as c from covid_nonulls group by year(earliest_case_dt), month(earliest_case_dt), race_ethnic order by y, m, c;

--FIFTH QUERY_________________________________________
insert overwrite directory 'project/output5' select year(earliest_case_dt) as y, month(earliest_case_dt) as m, age_group, count(*) as c from covid_nonulls group by year(earliest_case_dt), month(earliest_case_dt), age_group order by y, m, age_group;

--SIXTH QUERY (hospital and med cond)____________________________________
WITH t
AS (
	SELECT CASE WHEN hosp_yn = "Yes" AND medcond_yn LIKE "%Yes%" THEN 1 ELSE 0 END hosp_medcond,
		CASE WHEN hosp_yn = "Yes" AND medcond_yn LIKE "%No%" THEN 1 ELSE 0 END hosp_nomedcond,
		CASE WHEN hosp_yn = "No" AND medcond_yn LIKE "%Yes%" THEN 1 ELSE 0 END medcond_nohosp,
		CASE WHEN hosp_yn = "Missing" OR medcond_yn LIKE "%Missing%" OR hosp_yn = "Unknown" OR medcond_yn LIKE "Unknown" THEN 1 ELSE 0 END missing_hospmedcond
	FROM covid_nonulls
	)
INSERT OVERWRITE DIRECTORY 'project/output6' SELECT sum(hosp_medcond) hosp_medcond
	,sum(hosp_nomedcond) hosp_nomedcond
	,sum(medcond_nohosp) medcond_nohosp
	,sum(missing_hospmedcond) missing_hospmedcond
FROM t;

--SEVENTH QUERY (icu and medcond) _____________________________________
WITH t
AS (
	SELECT CASE WHEN icu_yn = "Yes" AND medcond_yn LIKE "%Yes%" THEN 1 ELSE 0 END icu_medcond,
		CASE WHEN icu_yn = "Yes" AND medcond_yn LIKE "%No%" THEN 1 ELSE 0 END icu_nomedcond,
		CASE WHEN icu_yn = "No" AND medcond_yn LIKE "%Yes%" THEN 1 ELSE 0 END medcond_noicu,
		CASE WHEN icu_yn = "Missing" OR medcond_yn LIKE "%Missing%" OR icu_yn = "Unknown" OR medcond_yn LIKE "%Unknown%" THEN 1 ELSE 0 END missing_icumedcond
	FROM covid_nonulls
	)
INSERT OVERWRITE DIRECTORY 'project/output7' SELECT sum(icu_medcond) icu_medcond
	,sum(icu_nomedcond) icu_nomedcond
	,sum(medcond_noicu) medcond_noicu
	,sum(missing_icumedcond) missing_icumedcond
FROM t;

--EIGHTH QUERY (medcond and death)______________________________________
WITH t
AS (
	SELECT CASE WHEN medcond_yn LIKE "%Yes%" AND death_yn = "Yes" THEN 1 ELSE 0 END medcond_death,
		CASE WHEN medcond_yn LIKE "%Yes%" AND death_yn = "No" THEN 1 ELSE 0 END medcond_nodeath,
		CASE WHEN medcond_yn LIKE "%No%" AND death_yn = "Yes" THEN 1 ELSE 0 END death_nomedcond,
		CASE WHEN medcond_yn LIKE "%Missing%" OR death_yn = "Missing" OR medcond_yn LIKE "%Unknown%" OR death_yn = "Unknown" THEN 1 ELSE 0 END missing_medconddeath
	FROM covid_nonulls
	)
INSERT OVERWRITE DIRECTORY 'project/output8' SELECT sum(medcond_death) medcond_death
	,sum(medcond_nodeath) medcond_nodeath
	,sum(death_nomedcond) death_nomedcond
	,sum(missing_medconddeath) missing_medconddeath
FROM t;

--NINTH QUERY (hosp by month by race)_________________________________________
INSERT overwrite directory 'project/output9'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	race_ethnic,
	hosp_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	race_ethnic,
	hosp_yn
ORDER BY y, m, c;

--TENTH QUERY (icu by month by race)_________________________________________
INSERT overwrite directory 'project/output10'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	race_ethnic,
	icu_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	race_ethnic,
	icu_yn
ORDER BY y, m, c;

--ELEVENTH QUERY (death by month by race)_________________________________________
INSERT overwrite directory 'project/output11'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	race_ethnic,
	death_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	race_ethnic,
	death_yn
ORDER BY y, m, c;

--TWELTH QUERY (hosp by month by age)_________________________________________
INSERT overwrite directory 'project/output12'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	age_group,
	hosp_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	age_group,
	hosp_yn
ORDER BY y, m, age_group, hosp_yn;

--THIRTEENTH QUERY (icu by month by age)_________________________________________
INSERT overwrite directory 'project/output13'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	age_group,
	icu_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	age_group,
	icu_yn
ORDER BY y, m, age_group, icu_yn;

--FOURTEENTH QUERY (death by month by age)_________________________________________
INSERT overwrite directory 'project/output14'
SELECT year(earliest_case_dt) AS y,
	month(earliest_case_dt) AS m,
	age_group,
	death_yn,
	count(*) AS c
FROM covid_nonulls
GROUP BY year(earliest_case_dt),
	month(earliest_case_dt),
	age_group,
	death_yn
ORDER BY y, m, age_group, death_yn;