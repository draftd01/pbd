select count(*) from covid_nonulls;
select min(earliest_case_dt) as min, max(earliest_case_dt) as max from covid;
select sex, count(sex) as c from covid_nonulls group by sex order by c;
select race_ethnic, count(race_ethnic) as c from covid_nonulls group by race_ethnic order by c desc;
select current_status, count(current_status) as c from covid_nonulls group by current_status order by c desc;